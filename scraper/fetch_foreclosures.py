#!/usr/bin/env python3
"""
Harris County Foreclosure Posting Scraper
==========================================
Scrapes the FRCL (Foreclosure) page on cclerk.hctx.net for the next 3 months,
downloads and parses PDF documents, and enriches with HCAD address data.

Runs as a separate workflow from the main motivated-seller scraper.
"""

import json
import logging
import os
import re
import signal
import sys
import time
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# PDF text extraction
try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber is required. Install with: pip install pdfplumber")
    sys.exit(1)

# OCR for scanned image PDFs
try:
    import pytesseract
    from PIL import Image, ImageFilter, ImageEnhance
    from pdf2image import convert_from_bytes
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    print("WARNING: pytesseract/Pillow/pdf2image not installed. OCR will be unavailable.")

# Gemini Vision API for PDF extraction
from gemini_extract import parse_pdf_with_gemini, HAS_GEMINI

CT = ZoneInfo("America/Chicago")

def preprocess_for_ocr(img):
    """Pre-process a PIL Image for better OCR accuracy on scanned legal docs."""
    # Convert to grayscale
    img = img.convert("L")
    # Increase contrast Ã¢ÂÂ makes faded text darker, background lighter
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(1.8)
    # Sharpen to improve edge definition on text
    img = img.filter(ImageFilter.SHARPEN)
    # Adaptive threshold: convert to pure black & white
    # Use a threshold that works well for legal docs (slightly above midpoint)
    img = img.point(lambda x: 255 if x > 140 else 0, mode='1')
    # Convert back to grayscale for Tesseract (some modes need 8-bit)
    img = img.convert("L")
    return img

# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  Logging
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  Constants
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
FRCL_SEARCH_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
FRCL_DOC_URL = "https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx"
CCLERK_LOGIN_URL = "https://www.cclerk.hctx.net/Applications/WebSearch/Registration/Login.aspx"

HCAD_BULK_URL = "https://download.hcad.org/data/CAMA/2026/Real_acct_owner.zip"

# Batch limit: process at most this many PDFs per run to stay within
# GitHub Actions timeout.  Remaining PDFs are picked up on the next run
# because we track seen_ids incrementally.
MAX_PDFS_PER_RUN = 200

# OCR resolution Ã¢ÂÂ 200 DPI is a good balance of speed and accuracy
OCR_DPI = 300

# Per-PDF timeout in seconds Ã¢ÂÂ kill stuck OCR processing
PDF_TIMEOUT = 120

DASHBOARD_DIR = Path("dashboard")
DATA_DIR = Path("data")
SEEN_FILE = DATA_DIR / "foreclosure_ids.json"
OUTPUT_FILE = DASHBOARD_DIR / "foreclosures.json"

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

MONTH_ABBREVS = {
    "January": "January", "February": "February", "March": "March",
    "April": "April", "May": "May", "June": "June",
    "July": "July", "August": "August", "September": "September",
    "October": "October", "November": "November", "December": "December",
}


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  Word-to-Number Converter (for legal descriptions)
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
WORD_NUMS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
    "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}


def words_to_number(text: str) -> Optional[int]:
    """Convert written-out numbers to integers.

    Examples:
        "TWENTY-FOUR" Ã¢ÂÂ 24
        "EIGHT" Ã¢ÂÂ 8
        "ONE HUNDRED TWENTY-THREE" Ã¢ÂÂ 123
    """
    text = text.strip().lower().replace("-", " ")
    parts = text.split()
    if not parts:
        return None

    total = 0
    current = 0
    for word in parts:
        if word == "hundred":
            current = (current if current else 1) * 100
        elif word in WORD_NUMS:
            current += WORD_NUMS[word]
        else:
            return None  # unrecognized word

    total += current
    return total if total > 0 else None


def convert_legal_numbers(text: str) -> str:
    """Convert spelled-out lot/block numbers in legal descriptions.

    "LOT TWENTY-FOUR (24), IN BLOCK TWO (2)" stays as-is because it
    already has the digits in parens.

    "LOT TWENTY-FOUR, BLOCK TWO" becomes "LT 24 BLK 2".
    """
    # If parenthesized numbers exist, extract them
    # Pattern: LOT TWENTY-FOUR (24)
    lot_paren = re.search(r'LOT\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)
    blk_paren = re.search(r'BLOCK\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)
    sec_paren = re.search(r'SEC(?:TION)?\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)

    lot_num = lot_paren.group(1) if lot_paren else None
    blk_num = blk_paren.group(1) if blk_paren else None
    sec_num = sec_paren.group(1) if sec_paren else None

    # If no parens, try to convert the word numbers
    if not lot_num:
        m = re.search(r'LOT\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+IN\b|\s+OF\b|\s+BL)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                lot_num = str(n)

    if not blk_num:
        m = re.search(r'BLOCK\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+OF\b|\s+SEC|\s+A\s)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                blk_num = str(n)

    if not sec_num:
        m = re.search(r'SEC(?:TION)?\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+A\s|\s+IN\b|$)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                sec_num = str(n)

    # Extract subdivision name
    subdiv = None
    # Pattern 1: "OF <SUBDIVISION NAME>, SEC..."  or  "OF <NAME>, A SUBDIVISION"
    m = re.search(r'(?:IN\s+BLOCK\s+\S+\s+OF|,\s+OF)\s+([A-Z][A-Z\s&\']+?)(?:\s*,|\s+SEC|\s+A\s+SUB|\s+IN\s+HARRIS)',
                  text, re.IGNORECASE)
    if m:
        subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 2: "BLOCK X, <NAME> SEC Y"
        m = re.search(r'BLOCK\s+\S+\s*,?\s+([A-Z][A-Z\s&\']+?)(?:\s+SEC|\s*,\s*A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()
            subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE)

    if not subdiv:
        # Pattern 3: "BLOCK WORD (NUM) <SUBDIVISION> SECTION" Ã¢ÂÂ common in foreclosure OCR
        m = re.search(r'BLOCK\s+[A-Z\-\s]+\(\d+\)\s+([A-Z][A-Z\s&\']+?)(?:\s+SEC(?:TION)?)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 4: "BLOCK NUM <SUBDIVISION> SECTION" (no parenthesized number)
        m = re.search(r'BLOCK\s+\d+\s*,?\s*(?:OF\s+)?([A-Z][A-Z\s&\']+?)(?:\s+SEC(?:TION)?|\s*,\s*A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 5: "IN BLOCK ... OF <SUBDIVISION>" without SEC following
        m = re.search(r'IN\s+BLOCK\s+\S+\s*,?\s+(?:OF\s+)?([A-Z][A-Z\s&\']+?)(?:\s*,|\s+A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()
            subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE)

    # Clean subdivision name
    if subdiv:
        # Strip leading "OF" that sometimes gets captured
        subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE).strip()
        # Fix common OCR garbles in subdivision names
        subdiv = re.sub(r'FORTST\b', 'FOREST', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'FORST\b', 'FOREST', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT\b', 'ESTATE', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT5\b', 'ESTATES', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT[E3]S?\b', 'ESTATES', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'VILLAG\b', 'VILLAGE', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'HEIGH?TS?\b', 'HEIGHTS', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'GARDNS?\b', 'GARDENS', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'MANOH\b', 'MANOR', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'PLAC\b', 'PLACE', subdiv, flags=re.IGNORECASE)

    return {
        "lot": lot_num,
        "block": blk_num,
        "section": sec_num,
        "subdivision": subdiv,
        "raw": text,
    }


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  HCAD Bulk Data Loader (shared with main scraper)
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
class HCADMatcher:
    """Loads HCAD bulk data and matches by legal description or owner name."""

    def __init__(self):
        self.lookup = {}       # normalized owner name Ã¢ÂÂ address dict
        self.legal_index = {}  # "SUBDIVISION|BLKn" Ã¢ÂÂ address dict

    def load(self, session: requests.Session):
        """Download and parse HCAD bulk data."""
        log.info("Loading HCAD parcel data for address enrichment...")
        log.info(f"  Downloading {HCAD_BULK_URL} (attempt 1)")

        for attempt in range(1, 4):
            try:
                resp = session.get(HCAD_BULK_URL, timeout=300)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt < 3:
                    log.warning(f"  Download failed (attempt {attempt}): {e}")
                    time.sleep(5)
                else:
                    raise

        log.info(f"  Download OK - {len(resp.content):,} bytes")

        zf = zipfile.ZipFile(BytesIO(resp.content))
        log.info(f"  Zip contains: {zf.namelist()}")

        # Parse real_acct.txt
        with zf.open("real_acct.txt") as f:
            lines = f.read().decode("utf-8", errors="replace").splitlines()

        log.info(f"  Parsing primary address data from: real_acct.txt")

        # Column mapping (tab-delimited, 0-indexed)
        # acct=0, mailto=2, site_addr_1=17, site_addr_3=19,
        # mail_city=5, lgl_1=66, lgl_2=67, lgl_3=68
        count = 0
        for line in lines[1:]:  # skip header
            cols = line.split("\t")
            if len(cols) < 70:
                continue

            acct = cols[0].strip()
            owner = cols[2].strip().upper()  # mailto field = owner name
            site_addr = cols[17].strip()
            site_city = cols[19].strip() if len(cols) > 19 else ""
            site_zip = cols[20].strip() if len(cols) > 20 else ""

            if not owner or not site_addr:
                continue

            info = {
                "acct": acct,
                "prop_address": site_addr,
                "prop_city": site_city or "Houston",
                "prop_state": "TX",
                "prop_zip": site_zip,
            }

            # Owner name lookup
            norm_owner = re.sub(r'\s+', ' ', owner).strip()
            if norm_owner and norm_owner not in self.lookup:
                self.lookup[norm_owner] = info

            # Legal description index
            lgl_1 = cols[66].strip().upper() if len(cols) > 66 else ""
            lgl_2 = cols[67].strip().upper() if len(cols) > 67 else ""

            if lgl_1 and lgl_2:
                # Parse subdivision from lgl_1, block from lgl_2
                subdiv = re.sub(r'\s+', ' ', lgl_1).strip()
                blk_match = re.search(r'BLK\s*(\S+)', lgl_2)
                if subdiv and blk_match:
                    key = f"{subdiv}|BLK{blk_match.group(1)}"
                    if key not in self.legal_index:
                        self.legal_index[key] = info

            count += 1

        log.info(f"  Loaded {count:,} parcels, {len(self.legal_index):,} legal keys")

        # Also parse owners.txt for supplemental names
        if "owners.txt" in zf.namelist():
            with zf.open("owners.txt") as f:
                owner_lines = f.read().decode("utf-8", errors="replace").splitlines()
            # Build a quick acct Ã¢ÂÂ info index so we don't scan the entire dict per row
            acct_index = {}
            for existing_info in self.lookup.values():
                a = existing_info.get("acct")
                if a and a not in acct_index:
                    acct_index[a] = existing_info
            added = 0
            for line in owner_lines[1:]:
                cols = line.split("\t")
                if len(cols) < 3:
                    continue
                acct = cols[0].strip()
                name = cols[2].strip().upper()
                norm = re.sub(r'\s+', ' ', name).strip()
                if norm and norm not in self.lookup and acct in acct_index:
                    self.lookup[norm] = acct_index[acct]
                    added += 1
            log.info(f"  owners.txt: added {added:,} supplemental name variants")

    def match_legal(self, parsed_legal: dict) -> Optional[dict]:
        """Try to match by legal description (subdivision + block)."""
        subdiv = parsed_legal.get("subdivision", "")
        blk = parsed_legal.get("block", "")
        lot = parsed_legal.get("lot", "")

        if not subdiv or not blk:
            return None

        subdiv_norm = re.sub(r'\s+', ' ', subdiv.upper()).strip()
        key = f"{subdiv_norm}|BLK{blk}"

        if key in self.legal_index:
            return self.legal_index[key]

        # Try with SEC appended
        sec = parsed_legal.get("section", "")
        if sec:
            key_sec = f"{subdiv_norm} SEC {sec}|BLK{blk}"
            if key_sec in self.legal_index:
                return self.legal_index[key_sec]

        # Fuzzy match: find HCAD keys that contain a similar subdivision name
        # This handles OCR garbles like "RDE FOREST" Ã¢ÂÂ "JADE FOREST" or "LAKEWOOD FORTST" Ã¢ÂÂ "LAKEWOOD FOREST"
        if len(subdiv_norm) >= 5:
            # Extract all words from the subdivision name
            subdiv_words = subdiv_norm.split()
            best_match = None
            best_score = 0
            suffix = f"|BLK{blk}"
            for hcad_key in self.legal_index:
                if not hcad_key.endswith(suffix):
                    continue
                hcad_subdiv = hcad_key.replace(suffix, "")
                # Check if SEC matches when present
                if sec and f"SEC {sec}" not in hcad_subdiv and f"SEC {sec}" in key_sec:
                    continue
                # Score: count matching words (at least half must match)
                hcad_words = hcad_subdiv.split()
                matching = sum(1 for w in subdiv_words if w in hcad_words)
                # Also check if the first significant word matches (most reliable)
                first_word_match = len(subdiv_words) > 0 and len(hcad_words) > 0 and subdiv_words[0] == hcad_words[0]
                if matching >= len(subdiv_words) * 0.5 and first_word_match and matching > best_score:
                    best_score = matching
                    best_match = hcad_key
            if best_match:
                log.info(f"  Fuzzy legal match: '{subdiv_norm}' Ã¢ÂÂ '{best_match.split('|')[0]}'")
                return self.legal_index[best_match]

        return None

    def export_lookup(self, filepath):
        """Export the legal_index as a JSON file for the dashboard lookup tool."""
        # Convert to a simpler format: key Ã¢ÂÂ { address, city, zip, acct }
        lookup = {}
        for key, info in self.legal_index.items():
            lookup[key] = {
                "a": info.get("prop_address", ""),
                "c": info.get("prop_city", ""),
                "z": info.get("prop_zip", ""),
                "t": info.get("acct", ""),
            }
        with open(filepath, "w") as f:
            json.dump(lookup, f, separators=(",", ":"))
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        log.info(f"Exported legal lookup: {len(lookup):,} entries, {size_mb:.1f} MB Ã¢ÂÂ {filepath}")

    def match_owner(self, name: str) -> Optional[dict]:
        """Try to match by owner name (exact)."""
        norm = re.sub(r'\s+', ' ', name.upper()).strip()
        if norm in self.lookup:
            return self.lookup[norm]

        # Try LAST FIRST reversal
        parts = norm.split()
        if len(parts) >= 2:
            reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
            if reversed_name in self.lookup:
                return self.lookup[reversed_name]

        return None


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  HCAD ArcGIS API (fallback for when bulk data doesn't match)
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
HCAD_ARCGIS_URL = (
    "https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0/query"
)
HCAD_DETAIL_BASE = "https://public.hcad.org/records/details.asp?cession=A&acct="


def hcad_api_search(session: requests.Session, name: str) -> Optional[dict]:
    """Search HCAD ArcGIS REST API by owner name."""
    safe = name.strip().upper().replace("'", "''")
    if not safe or safe in ("SEE INSTRUMENT", "SEE DOCUMENT", "UNKNOWN"):
        return None

    where = (
        f"upper(owner_name_1) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_2) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_3) LIKE upper('%{safe}%')"
    )
    params = {
        "where":             where,
        "outFields":         ("HCAD_NUM,owner_name_1,owner_name_2,owner_name_3,"
                              "site_str_num,site_str_name,site_str_sfx,"
                              "site_str_pfx,site_str_sfx_dir,"
                              "site_city,site_zip,state_class"),
        "returnGeometry":    "false",
        "f":                 "json",
        "resultRecordCount": "10",
    }

    try:
        resp = session.get(HCAD_ARCGIS_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"HCAD API error for '{name}': {e}")
        return None

    features = data.get("features", [])
    if not features:
        log.info(f"HCAD API: no results for '{name}'")
        return None

    # Prefer residential properties (state_class starting with A or B)
    best = None
    for feat in features:
        attr = feat.get("attributes", {})
        sc = (attr.get("state_class") or "").upper()
        if sc.startswith("A") or sc.startswith("B"):
            best = attr
            break

    if not best:
        best = features[0].get("attributes", {})

    # Build street from split fields
    parts = []
    pfx = str(best.get("site_str_pfx") or "").strip()
    num = str(best.get("site_str_num") or "").strip()
    sname = str(best.get("site_str_name") or "").strip()
    sfx = str(best.get("site_str_sfx") or "").strip()
    sdir = str(best.get("site_str_sfx_dir") or "").strip()
    if pfx:
        parts.append(pfx)
    if num:
        parts.append(num)
    if sname:
        parts.append(sname)
    if sfx:
        parts.append(sfx)
    if sdir:
        parts.append(sdir)
    street = " ".join(parts)

    if not street:
        return None

    city = str(best.get("site_city") or "").strip()
    zipcode = str(best.get("site_zip") or "").strip()
    acct = str(best.get("HCAD_NUM") or "").strip()
    owner = str(best.get("owner_name_1") or "").strip()

    log.info(f"HCAD API: found residential '{owner}' -> {street}, {city} {zipcode}")
    return {
        "acct": acct,
        "owner_name": owner,
        "street": street,
        "city": city or "Houston",
        "state": "TX",
        "zip": zipcode,
    }


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  PDF Parser
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def parse_foreclosure_pdf(pdf_bytes: bytes, doc_id: str) -> dict:
    """Extract key fields from a foreclosure posting PDF.

    Handles two main formats:
    1. Format A: Labeled fields (Grantor(s):, Legal Description:, etc.)
    2. Format B: "NOTICE OF FORECLOSURE SALE AND APPOINTMENT OF SUBSTITUTE TRUSTEE"
       with address at top, "Property:" section, "Obligation Secured:" section
    """
    result = {
        "grantor": "",
        "co_borrower": "",
        "property_address": "",
        "property_city": "",
        "property_state": "TX",
        "property_zip": "",
        "legal_description": "",
        "amount": "",
        "sale_date": "",
        "mortgagee": "",
    }

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            full_text = ""
            has_scanned_pages = False
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text.strip():
                    full_text += text + "\n"
                else:
                    has_scanned_pages = True
            # Trigger OCR if the PDF is scanned images â pdfplumber sometimes
            # extracts 1-3 garbage chars from scanned pages, so check length < 20
            if has_scanned_pages and len(full_text.strip()) < 20:
                # PDF is entirely scanned images Ã¢ÂÂ run OCR to extract text
                if HAS_OCR:
                    log.info(f"  {doc_id} is a scanned PDF Ã¢ÂÂ running OCR at {OCR_DPI} DPI...")
                    try:
                        images = convert_from_bytes(pdf_bytes, dpi=OCR_DPI)
                        # Tesseract config: PSM 6 = assume uniform block of text (best for legal docs)
                        tess_config = "--psm 6"
                        for page_num, img in enumerate(images, 1):
                            # Pre-process image for better OCR accuracy
                            processed = preprocess_for_ocr(img)
                            page_text = pytesseract.image_to_string(processed, config=tess_config)
                            # If preprocessing produced poor results, retry with original
                            if len(page_text.strip()) < 50:
                                page_text_raw = pytesseract.image_to_string(img, config=tess_config)
                                if len(page_text_raw.strip()) > len(page_text.strip()):
                                    page_text = page_text_raw
                                    log.info(f"    Page {page_num}: fallback to raw image ({len(page_text)} chars)")
                            if page_text.strip():
                                full_text += page_text + "\n"
                                log.info(f"    Page {page_num}: {len(page_text)} chars extracted via OCR")
                            else:
                                log.info(f"    Page {page_num}: no text found via OCR")
                        log.info(f"  OCR complete: {len(full_text)} total chars")
                    except Exception as e:
                        log.warning(f"  OCR failed for {doc_id}: {e}")
                        result["needs_ocr"] = True
                        return result
                else:
                    log.info(f"  {doc_id} is a scanned PDF Ã¢ÂÂ OCR not available, flagging for VA lookup")
                    result["needs_ocr"] = True
                    return result
            elif has_scanned_pages:
                log.info(f"  {doc_id} has some scanned pages but got text from others")
    except Exception as e:
        log.warning(f"Failed to extract text from {doc_id}: {e}")
        return result

    if not full_text.strip():
        log.warning(f"Empty PDF text for {doc_id}")
        return result

    # Debug: log first 300 chars of extracted text to help diagnose parsing issues
    preview = full_text[:300].replace('\n', ' | ')
    log.info(f"  Text preview: {preview}")

    # Ã¢ÂÂÃ¢ÂÂ OCR Text Cleanup Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
    # Normalize common OCR artifacts before regex parsing
    # Strip common OCR noise characters that aren't meaningful in legal docs
    full_text = re.sub(r'[<>~\*\\\{\}]', ' ', full_text)
    # Collapse multiple spaces/tabs into single space
    full_text = re.sub(r'[ \t]{2,}', ' ', full_text)
    # Remove isolated single junk chars between words (OCR noise like ". : o ,")
    full_text = re.sub(r'(?<=[A-Z]) [^A-Za-z0-9\s]{1,2} (?=[A-Z])', ' ', full_text)
    # Remove isolated single-char noise at start of lines (common OCR artifact)
    full_text = re.sub(r'(?m)^[^A-Za-z0-9\s]{1,3}\s+', '', full_text)
    # Fix common OCR letter substitutions in key words
    full_text = re.sub(r'\bGrant0r\b', 'Grantor', full_text)
    full_text = re.sub(r'\b[Cc]0unty\b', 'County', full_text)
    full_text = re.sub(r'\bpr0perty\b', 'property', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bSP[Q0O](?:USE|OSE)\b', 'SPOUSE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bSUBSTITUTE\s+TR[U0]STEE\b', 'SUBSTITUTE TRUSTEE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bN[O0]TICE\b', 'NOTICE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bM[O0]RTGAG[O0]R\b', 'MORTGAGOR', full_text, flags=re.IGNORECASE)
    # Fix "| " Ã¢ÂÂ "l " (pipe misread as lowercase L) in common contexts
    full_text = re.sub(r'\|(?=\s+of\s)', 'l', full_text)
    # Normalize curly/smart quotes to straight
    full_text = full_text.replace('\u2018', "'").replace('\u2019', "'")
    full_text = full_text.replace('\u201c', '"').replace('\u201d', '"')
    # Strip null bytes and control characters (except newlines)
    full_text = re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f]', '', full_text)
    # Collapse runs of dots/periods (OCR artifact from scanned table lines)
    full_text = re.sub(r'\.{2,}', ' ', full_text)
    # Remove stray single dots surrounded by spaces (not decimal points)
    full_text = re.sub(r'(?<!\d)\.\s+(?!\d)', ' ', full_text)
    # Final whitespace normalization after all cleanup
    full_text = re.sub(r'[ \t]{2,}', ' ', full_text)

    lines = full_text.split("\n")

    # Ã¢ÂÂÃ¢ÂÂ Try Format A: labeled fields Ã¢ÂÂÃ¢ÂÂ
    # NOTE: Do NOT return early Ã¢ÂÂ fall through to Format B for address extraction
    # and more robust grantor patterns, since Format A doesn't extract addresses.
    if "Grantor(s):" in full_text or "Grantor (s):" in full_text or re.search(r'Grantor\s*\(?s?\)?\s*/\s*Mortgagor', full_text, re.IGNORECASE):
        for i, line in enumerate(lines):
            line_s = line.strip()

            # Grantor Ã¢ÂÂ check same line first, then next line
            m = re.match(r'Grantor\s*\(s?\)\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                # Reject if it's just another label (e.g., "Grantor(s): Current Mortgagee:")
                if val and not re.match(r'^(Current|Legal|Amount|Date|Property|Instrument)\b', val, re.IGNORECASE):
                    result["grantor"] = val
            elif re.match(r'Grantor\s*\(s?\)\s*:?\s*$', line_s, re.IGNORECASE):
                # Label on its own line Ã¢ÂÂ grab the NEXT non-empty line as the grantor name
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 2:
                        # Make sure it's a name, not another label
                        if not re.match(r'^(Current|Legal|Amount|Date|Property|Instrument|Account)\b', next_line, re.IGNORECASE):
                            result["grantor"] = next_line
                        break

            # Grantor(s)/Mortgagor(s): NAME Ã¢ÂÂ table header format
            if not result["grantor"]:
                m = re.match(r'Grantor\s*\(?s?\)?\s*/\s*Mortgagor\s*\(?s?\)?\s*:?\s*(.+)', line_s, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val and len(val) > 2:
                        result["grantor"] = val

            # Legal Description Ã¢ÂÂ same line or next line
            m = re.match(r'Legal\s+Description\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["legal_description"] = m.group(1).strip()
            elif re.match(r'Legal\s+Description\s*:?\s*$', line_s, re.IGNORECASE):
                # Label alone Ã¢ÂÂ grab following lines as legal description
                legal_parts = []
                for j in range(i + 1, min(i + 5, len(lines))):
                    next_line = lines[j].strip()
                    if not next_line:
                        break
                    if re.match(r'^(Amount|Date|Current|Instrument|Account|Grantor)\b', next_line, re.IGNORECASE):
                        break
                    legal_parts.append(next_line)
                if legal_parts:
                    result["legal_description"] = " ".join(legal_parts)

            # Property Address Ã¢ÂÂ Format A sometimes has this
            if not result["property_address"]:
                m = re.match(r'Property\s+Address\s*:\s*(.+)', line_s, re.IGNORECASE)
                if m:
                    result["property_address"] = m.group(1).strip()

            # Amount
            m = re.match(r'Amount\s*:\s*\$?([\d,\.]+)', line_s, re.IGNORECASE)
            if m:
                result["amount"] = "$" + m.group(1)

            # Date of Sale Ã¢ÂÂ strip time/hours portion
            m = re.match(r'Date\s+of\s+Sale\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                raw_date = m.group(1).strip()
                # Clean: "May 5, 2026 between the hours..." Ã¢ÂÂ "May 5, 2026"
                cleaned = re.sub(r'\s+between\s+.*', '', raw_date, flags=re.IGNORECASE)
                result["sale_date"] = cleaned

            # Current Mortgagee
            m = re.match(r'Current\s+Mortgagee\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["mortgagee"] = m.group(1).strip()

        log.info(f"  Format A extracted: grantor={result['grantor'][:30] if result['grantor'] else '(empty)'}, legal={'yes' if result['legal_description'] else 'no'}")
        # Fall through to Format B patterns for address extraction and grantor enhancement

    # Ã¢ÂÂÃ¢ÂÂ Format B: Structured paragraphs Ã¢ÂÂÃ¢ÂÂ
    # Street suffixes (including full words for OCR accuracy)
    _SFX = (r"(?:ST|RD|DR|LN|CT|AVE|BLVD|WAY|PL|CIR|TRL|PKWY|CV|"
            r"STREET|ROAD|DRIVE|LANE|COURT|AVENUE|BOULEVARD|PLACE|"
            r"CIRCLE|TRAIL|PARKWAY|COVE)")

    # Known foreclosure auction venues and courthouse addresses Ã¢ÂÂ NOT property addresses
    # These appear in every document and must be excluded
    # --- Harris County & surrounding area cities/communities ---
    # Used to validate extracted addresses are actual properties, not attorney/servicer offices
    _HARRIS_COUNTY_CITIES = {
        # Incorporated cities (fully or partially in Harris County)
        "HOUSTON", "BAYTOWN", "BELLAIRE", "BUNKER HILL VILLAGE", "DEER PARK",
        "EL LAGO", "FRIENDSWOOD", "GALENA PARK", "HEDWIG VILLAGE", "HILSHIRE VILLAGE",
        "HUMBLE", "HUNTERS CREEK VILLAGE", "JACINTO CITY", "JERSEY VILLAGE",
        "KATY", "LA PORTE", "LEAGUE CITY", "MISSOURI CITY", "MORGANS POINT",
        "NASSAU BAY", "PASADENA", "PEARLAND", "PINEY POINT VILLAGE",
        "SEABROOK", "SOUTH HOUSTON", "SOUTHSIDE PLACE", "SPRING VALLEY",
        "SPRING VALLEY VILLAGE", "STAFFORD", "TAYLOR LAKE VILLAGE", "TOMBALL",
        "WALLER", "WEBSTER", "WEST UNIVERSITY PLACE",
        # Unincorporated communities / CDPs (USPS mailing address cities)
        "ALDINE", "ATASCOCITA", "BARRETT", "CHANNELVIEW", "CLOVERLEAF",
        "CROSBY", "CYPRESS", "HIGHLANDS", "HUFSMITH", "KLEIN",
        "MANVEL", "MISSION BEND", "SHELDON", "SPRING", "THE WOODLANDS",
        "FRESNO", "ROSHARON", "ALIEF", "KINGWOOD",
        # Nearby cities that can have Harris County properties
        "PORTER", "HUFFMAN", "DAYTON", "MONT BELVIEU", "RICHMOND",
        "ROSENBERG", "SUGAR LAND", "CONROE", "MAGNOLIA", "HOCKLEY",
        "PRAIRIE VIEW", "PINEHURST", "LA MARQUE", "TEXAS CITY",
        "DICKINSON", "SANTA FE", "KEMAH", "SHOREACRES",
        # Common abbreviations / variations
        "W UNIVERSITY PL", "S HOUSTON",
    }

    _VENUE_PATTERNS = [
        r'9401\s+KNIGHT',        # Bayou City Event Center (auction venue)
        r'401\s+KNI[GC]HT',     # OCR-garbled version of 9401 Knight
        r'201\s+CAROLINE',       # Harris County Civil Courthouse
        r'1001\s+PRESTON',       # Harris County Admin Building
        r'SENTARA\s+WAY',        # LoanCare LLC (common servicer address)
        r'CONGRESS\s+(?:AVE|ST)', # Government buildings
        r'1255.*15TH.*ST',          # McCarthy & Holthus LLP (attorney office, Plano TX)
        r'1\s*HOME\s*CAMPUS',      # Mortgage servicer address (West Des Moines IA)
    ]

    def _is_venue_address(addr_str):
        """Check if an address matches a known auction venue or courthouse."""
        for pat in _VENUE_PATTERNS:
            if re.search(pat, addr_str, re.IGNORECASE):
                return True
        return False

    # Check for address at top of document (first 10 lines)
    if not result["property_address"]:
        for line in lines[:10]:
            line_s = line.strip()
            if not line_s or not line_s[0].isdigit():
                continue
            # Pattern: "15647 COUNTESSWELLS DRIVE, HUMBLE, TX 77346"
            m = re.match(r'^(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*[,]?\s*'
                         r'([A-Z]+)\s*[,]?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
                         line_s, re.IGNORECASE)
            if m:
                result["property_address"] = m.group(1).strip()
                result["property_city"] = m.group(2).strip()
                result["property_state"] = "TX"
                result["property_zip"] = m.group(3).strip()
                break

    # Check for "Commonly known as:" line (support multi-word cities like LA PORTE)
    if not result["property_address"]:
        m = re.search(r'[Cc]ommonly\s+known\s+as\s*:?\s*(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*[,]?\s*'
                      r'([A-Z][A-Z\s]*?)\s*[,]?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)', full_text, re.IGNORECASE)
        if m:
            addr = m.group(1).strip()
            city = m.group(2).strip()
            if len(city) >= 3 and all(c.isalpha() or c == ' ' for c in city):
                result["property_address"] = addr
                result["property_city"] = city
                result["property_zip"] = m.group(3).strip()
                if not _is_venue_address(addr):
                    log.info(f"  Address (commonly known as): {addr}, {city}")

    # Check for address after clerk filing stamp (common in OCR'd foreclosure docs)
    # Pattern: "FRCL-2026-XXXX FILED date\n ADDRESS LINE \n acct# \n CITY, TX ZIP"
    if not result["property_address"]:
        m = re.search(
            r'FRCL-\d{4}-\d+\s+FILED\s+[\d/]+\s+[\d:]+\s*[AP]?M?\s*'
            r'(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*'
            r'(?:\d{10,16}\s*)?'  # optional account number
            r'([A-Z][A-Z\s]*?)\s*,?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
            full_text, re.IGNORECASE
        )
        if m:
            result["property_address"] = m.group(1).strip()
            result["property_city"] = m.group(2).strip()
            result["property_state"] = "TX"
            result["property_zip"] = m.group(3).strip()
            log.info(f"  Address from filing stamp: {result['property_address']}, {result['property_city']}")

    # Broader fallback: find "NUMBER STREET ... CITY, TX ZIP" anywhere in text
    # Tolerant of OCR junk characters between street and city
    if not result["property_address"]:
        m = re.search(
            r'(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r')'
            r'[\s\.\:,o_]*'  # allow OCR junk chars between street and acct#
            r'(?:\d{10,16}[\s\.\:,]*)?'  # optional account number with junk
            r'([A-Z][A-Z\s]+?)\s*,?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
            full_text, re.IGNORECASE
        )
        if m:
            addr = m.group(1).strip()
            city = m.group(2).strip()
            zipcode = m.group(3).strip()
            # Sanity check: city should be real words, not OCR junk
            # Allow spaces for cities like LA PORTE, LA MARQUE, etc.
            if len(city) >= 3 and all(c.isalpha() or c == ' ' for c in city):
                result["property_address"] = addr
                result["property_city"] = city
                result["property_state"] = "TX"
                result["property_zip"] = zipcode
                log.info(f"  Address (broad match): {addr}, {city}, TX {zipcode}")

    # Ã¢ÂÂÃ¢ÂÂ Validate extracted address Ã¢ÂÂ reject auction venues Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
    if result["property_address"] and _is_venue_address(result["property_address"]):
        log.info(f"  Rejected venue address: {result['property_address']} Ã¢ÂÂ not a property")
        result["property_address"] = ""
        result["property_city"] = ""
        result["property_state"] = "TX"
        result["property_zip"] = ""

    # --- Reject addresses outside Harris County area ---
    # Attorney/servicer addresses (Plano, West Des Moines, etc.) should not be used
    if result["property_address"] and result["property_city"]:
        city_upper = result["property_city"].strip().upper()
        if city_upper and city_upper not in _HARRIS_COUNTY_CITIES:
            log.info(f"  Rejected non-Harris-County address: {result['property_address']}, {result['property_city']} (not in allowed city list)")
            result["property_address"] = ""
            result["property_city"] = ""
            result["property_state"] = "TX"
            result["property_zip"] = ""

    # Extract Property / Legal Description section
    # Try multiple patterns since OCR can garble the text
    legal_found = False

    # First, check if the document references "Exhibit A" for the legal description
    # Many foreclosure postings say "described in the attached Exhibit A"
    exhibit_ref = re.search(
        r'(?:described\s+in\s+(?:the\s+)?(?:attached\s+)?Exhibit\s+["\']?A["\']?|'
        r'see\s+(?:attached\s+)?Exhibit\s+["\']?A["\']?|'
        r'set\s+forth\s+(?:in|on)\s+(?:attached\s+)?Exhibit\s+["\']?A["\']?)',
        full_text, re.IGNORECASE
    )
    if exhibit_ref:
        # Find the "Exhibit A" header later in the document and grab text after it
        m = re.search(
            r'(?:^|\n)\s*(?:EXHIBIT\s+["\']?A["\']?|Exhibit\s+["\']?A["\']?)\s*[\n\r]+'
            r'(.*?)(?:\n\s*(?:EXHIBIT\s+["\']?B|Exhibit\s+["\']?B)|$)',
            full_text[exhibit_ref.end():], re.DOTALL | re.IGNORECASE
        )
        if m:
            legal_block = m.group(1).strip()
            legal_block = re.sub(r'\s+', ' ', legal_block).strip()
            # Remove boilerplate that sometimes appears at the start of Exhibit A
            legal_block = re.sub(r'^(?:Legal\s+Description\s*:?\s*|Property\s+Description\s*:?\s*)', '', legal_block, flags=re.IGNORECASE).strip()
            if len(legal_block) > 10:
                result["legal_description"] = legal_block
                legal_found = True
                log.info(f"  Legal description from Exhibit A: {legal_block[:80]}...")

    if not legal_found:
        for pat in [
            r'(?:described\s+as\s+follows|Property\s+to\s+be\s+sold\s+is\s+described)\s*:?\s*\n?(.*?)(?:Security\s+Instrument|Deed\s+of\s+Trust\s*:|The\s+(?:Security|Deed))',
            r'Property\s*:\s*.*?(?:described\s+as\s+follows\s*:?)?\s*\n(.*?)(?:Security\s+Instrument|Deed\s+of\s+Trust)',
            r'(LOT\s+[A-Z\-\s]+\(\d+\).*?(?:HARRIS\s+COUNTY|COUNTY\s*,\s*TEXAS))',
        ]:
            m = re.search(pat, full_text, re.DOTALL | re.IGNORECASE)
            if m:
                legal_block = m.group(1).strip()
                legal_block = re.sub(r'\s+', ' ', legal_block).strip()
                if len(legal_block) > 10:
                    result["legal_description"] = legal_block
                    legal_found = True
                    break

    # Ã¢ÂÂÃ¢ÂÂ Clean up legal description Ã¢ÂÂ truncate at boundary phrases Ã¢ÂÂÃ¢ÂÂ
    # OCR often captures text beyond the legal description into the next section
    if result["legal_description"]:
        ld = result["legal_description"]
        # Cut at common boundary phrases that signal end of legal description
        for boundary in [
            r'(?:[,:\s]+\d*\s*Instr[ua]n?[mt]en?t\s+to[\s-]+be)',  # "Instruntent to be", "Instrament to-be"
            r'(?:The\s+[Ii]nstr[ua]n?[mt]en?t\s+to[\s-]+be)',      # "The instrument to be"
            r'(?:The\s+mstr[au]ment\s+to[\s-]+be)',                 # "The mstrament to be"
            r'(?:\d+\s+[Ii]nstr[ua]n?[mt]en?t\s+to)',              # "2 Instrament to"
            r'(?:to[\s-]+be\s+[Ff]oreclosed)',                      # "to be Foreclosed" / "to-be Foreclosed"
            r'(?:be\s+[Ff]oreclosed\s+The)',                        # "be Foreclosed The"
            r'(?:Security\s+Instrument)',
            r'(?:Deed\s+of\s+Trust)',
        ]:
            m = re.search(boundary, ld, re.IGNORECASE)
            if m:
                ld = ld[:m.start()].strip()
                break
        # Final cleanup Ã¢ÂÂ strip trailing junk punctuation and stray numbers
        ld = re.sub(r'[,:;\.\s\d]+$', '', ld)
        # Truncate at "HARRIS COUNTY TEXAS" if still too long
        if len(ld) > 200:
            # Try to cut at last "HARRIS COUNTY" or "COUNTY TEXAS"
            m = re.search(r'(?:HARRIS\s+COUNTY|COUNTY\s*,?\s*TEXAS)', ld, re.IGNORECASE)
            if m:
                ld = ld[:m.end()].strip()
        result["legal_description"] = ld

    # Ã¢ÂÂÃ¢ÂÂ Reject legal description if it's clearly boilerplate, not a real legal desc Ã¢ÂÂÃ¢ÂÂ
    if result["legal_description"]:
        ld_lower = result["legal_description"].lower()
        boilerplate_phrases = [
            "property code", "mortgagee", "servicing agreement", "mortgage servicer",
            "foreclosure of the lien", "virtue of a", "authorized to collect",
            "pursuant to the", "loancare", "nationstar", "ocwen",
        ]
        boilerplate_count = sum(1 for phrase in boilerplate_phrases if phrase in ld_lower)
        if boilerplate_count >= 2:
            log.info(f"  Rejected boilerplate legal desc ({boilerplate_count} boilerplate phrases found)")
            result["legal_description"] = ""

    # Extract grantor Ã¢ÂÂ try multiple patterns (skip if Format A already found one)
    # Pattern 1: "executed by GRANTOR NAME"
    if not result["grantor"]:
        m = re.search(r'(?:executed\s+by|delivered\s+by)\s+([A-Z][A-Za-z\s,\.&]+?)(?:\s*,?\s*as\s+[Gg]rantors?|\s+(?:and\s+wife|delivered|executed|in\s+favor|securing|to\s+))',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 2: "WHEREAS, on DATE, GRANTOR NAME, ... as Grantor(s)"
    if not result["grantor"]:
        m = re.search(r'WHEREAS\s*,\s*on\s+\d+/\d+/\d+\s*,\s*([A-Za-z\s,\.]+?)\s*,\s*(?:and\s+wife|as\s+[Gg]rantors?)',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 3: "with NAME, as grantor(s)" Ã¢ÂÂ common in foreclosure postings
    if not result["grantor"]:
        m = re.search(r'with\s+([A-Z][A-Za-z\s,\.&]+?)\s*,?\s*as\s+[Gg]rantors?',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 4: "Grantor: NAME" or "Grantor(s)/Mortgagor(s): NAME" Ã¢ÂÂ labeled field at top of document
    if not result["grantor"]:
        m = re.search(r'[Gg]rantors?(?:\(s\))?\s*(?:/\s*[Mm]ortgagors?(?:\(s\))?\s*)?\s*:\s*\n?\s*([A-Z][A-Za-z\s,\.&]+?)(?:\s*\n|\s+[Cc]urrent|\s+[Gg]rantee|\s+[Oo]riginal|\s+Instrument|\s+Property|\s+Deed|\s+Date|\s+Account)',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 5: "NAME secures the repay" (OCR-garbled version)
    if not result["grantor"]:
        m = re.search(r'([A-Z][A-Za-z\s]+?)\s+secures\s+the\s+repa', full_text, re.IGNORECASE)
        if m:
            # Take just the last 1-3 words before "secures" as the name
            name = m.group(1).strip()
            # Remove leading noise from OCR
            parts = name.split()
            if len(parts) > 4:
                name = " ".join(parts[-3:])
            result["grantor"] = name.rstrip(",. ")

    # Pattern 6: Table-format OCR â "Grantor(s)/Mortgagor(s)" header row
    if not result["grantor"]:
        m = re.search(
            r'[Gg]rantor\s*\(?s?\)?\s*(?:/\s*[Mm]ortgagor\s*\(?s?\)?\s*)?'
            r'[\s:,.\-|]*'
            r'\d{1,2}/\d{1,2}/\d{2,4}'
            r'[\s:,.\-|]*'
            r'([A-Z][A-Z\s,\.&\'\-]+?)'
            r'(?:\s+(?:SINGLE|MARRIED|AN?\s+(?:UN)?MARRIED|SPOUSE|A\s+SINGLE)\b|\s+[Cc]urrent|\s+[Ii]nstrument|\s+[Ll]egal|\s+[Aa]ccount|\s+[Dd]ate|\s*$)',
            full_text
        )
        if m:
            name = m.group(1).strip().rstrip(",. ;:")
            if len(name.split()) >= 2 and sum(c.isalpha() for c in name) > len(name) * 0.6:
                result["grantor"] = name
                log.info(f"  Pattern 6 (table-format OCR): {name[:40]}")

    # Pattern 7: Standalone date + ALL_CAPS_NAME (OCR'd scanned table)
    if not result["grantor"]:
        for m in re.finditer(
            r'(\d{1,2}/\d{1,2}/\d{2,4})\s+'
            r'([A-Z][A-Z\s\'\-]{3,50}?)'
            r'(?:\s+(?:SINGLE|MARRIED|AN?\s+(?:UN)?MARRIED|AND\s+SPOUSE|A\s+SINGLE|A\s+MARRIED)\b)',
            full_text
        ):
            name = m.group(2).strip().rstrip(",. ;:")
            if len(name.split()) >= 2 and not re.match(r'^(?:NOTICE|DEED|HARRIS|STATE|COUNTY)\b', name):
                result["grantor"] = name
                log.info(f"  Pattern 7 (date+name): {name[:40]}")
                break

    # Pattern 8: "executed by NAME" with OCR garbled "executed"
    if not result["grantor"]:
        m = re.search(
            r'(?:ex[ec]{1,2}uted|deliver[eo]d)\s+by\s+'
            r'([A-Z][A-Z\s,\.&\'\-]+?)'
            r'(?:\s*,?\s*(?:as\s+[Gg]rantors?|and\s+wife|delivered|executed|in\s+favor|securing|to\s+|Deed|a\s+[Dd]eed))',
            full_text
        )
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")
            log.info(f"  Pattern 8 (executed by, OCR): {result['grantor'][:40]}")

    # Clean up grantor Ã¢ÂÂ remove OCR junk that gets appended after the name
    if result["grantor"]:
        g = result["grantor"]
        # Remove leading "by " prefix (artifact from "executed by" capture)
        g = re.sub(r'^by\s+', '', g, flags=re.IGNORECASE)
        # Remove leading OCR noise chars
        g = re.sub(r'^[^A-Za-z]+', '', g)
        # Remove newlines and everything after them (OCR artifact)
        if "\n" in g:
            parts = g.split("\n")
            clean_parts = [parts[0]]
            for p in parts[1:]:
                p_stripped = p.strip()
                # Keep ONLY if it looks like a continuation name (e.g., "NAHTALY AULD" after "AND YUDITH")
                # or a suffix like JR, SR, III
                if re.match(r'^(?:[A-Z][A-Z\s\-\']+(?:,\s*(?:HUSBAND|WIFE|AN?\s+(?:UN)?MARRIED\s+(?:WO)?MAN))?)\s*$', p_stripped, re.IGNORECASE) and len(p_stripped) < 60:
                    clean_parts.append(p_stripped)
                elif re.match(r'^(?:JR|SR|III?|IV)\b', p_stripped, re.IGNORECASE):
                    clean_parts.append(p_stripped)
                else:
                    break  # Stop at first non-name line
            g = " ".join(clean_parts)
        # Remove common OCR junk phrases that get captured (tolerant of garbled first letter)
        g = re.sub(r'\s+(?:p?ayment|indebtedness|securing|of\s+the|in\s+favor|the\s+repay|but\s+not\s+limited|pursuant|default|note\s+dated|deed\s+of).*', '', g, flags=re.IGNORECASE)
        # Strip trailing "AND" when no co-borrower name follows
        g = re.sub(r'\s+AND\s*$', '', g, flags=re.IGNORECASE)
        # Strip marital status descriptors and spouse prefix
        g = re.sub(r',?\s*AND\s+SPOUSE\s+', ' AND ', g, flags=re.IGNORECASE)
        g = re.sub(r',?\s*(?:AN?\s+)?(?:UN)?MARRIED\s+(?:WO)?MAN\b', '', g, flags=re.IGNORECASE)
        g = re.sub(r',?\s*(?:HUSBAND(?:\s+AND\s+WIFE)?|WIFE(?:\s+AND\s+HUSBAND)?)\s*$', '', g, flags=re.IGNORECASE)
        # Remove trailing junk punctuation
        g = g.strip().rstrip(",. ;:")
        result["grantor"] = g

    # Split borrower / co-borrower on " AND "
    result["co_borrower"] = ""
    if result["grantor"] and " AND " in result["grantor"].upper():
        name_parts = re.split(r'\s+AND\s+', result["grantor"], maxsplit=1, flags=re.IGNORECASE)
        if len(name_parts) == 2:
            borrower = name_parts[0].strip().rstrip(",. ;:")
            co = name_parts[1].strip().rstrip(",. ;:")
            # Only split if both parts look like names (2+ chars, mostly alpha)
            alpha_chars_b = sum(c.isalpha() or c == ' ' for c in borrower)
            alpha_chars_c = sum(c.isalpha() or c == ' ' for c in co)
            if alpha_chars_b > 3 and alpha_chars_c > 3:
                result["grantor"] = borrower
                # Clean marital status from co-borrower too
                co = re.sub(r',?\s*(?:AN?\s+)?(?:UN)?MARRIED\s+(?:WO)?MAN\b', '', co, flags=re.IGNORECASE)
                co = re.sub(r',?\s*(?:HUSBAND(?:\s+AND\s+WIFE)?|WIFE(?:\s+AND\s+HUSBAND)?)\s*$', '', co, flags=re.IGNORECASE)
                result["co_borrower"] = co.strip().rstrip(",. ;:")

    # Extract amount Ã¢ÂÂ multiple patterns
    m = re.search(r'(?:amount\s+of|sum\s+of)\s+\$?([\d,]+(?:\.\d{2})?)', full_text, re.IGNORECASE)
    if m:
        result["amount"] = "$" + m.group(1)
    if not result["amount"]:
        m = re.search(r'Amount\s*:\s*\$?([\d,]+(?:\.\d{2})?)', full_text, re.IGNORECASE)
        if m:
            result["amount"] = "$" + m.group(1)

    # Extract sale date from "NOTICE IS HEREBY GIVEN, that on DATE"
    m = re.search(r'(?:NOTICE\s+IS\s+HEREBY\s+GIVEN|Date\s+of\s+Sale)\s*[,:]?\s*(?:that\s+on\s+)?(\d{1,2}/\d{1,2}/\d{4})',
                  full_text, re.IGNORECASE)
    if m:
        result["sale_date"] = m.group(1)

    # Also check for spelled-out date: "May 5, 2026"
    if not result["sale_date"]:
        m = re.search(r'(?:Sale\s+Information|NOTICE\s+IS\s+HEREBY\s+GIVEN).*?'
                      r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
                      r'(\d{1,2})\s*,\s*(\d{4})',
                      full_text, re.IGNORECASE | re.DOTALL)
        if m:
            result["sale_date"] = f"{m.group(1)} {m.group(2)}, {m.group(3)}"

    # Extract mortgagee / servicer Ã¢ÂÂ multiple patterns
    for mort_pat in [
        r'[Cc]urrent\s+[Mm]ortgagee\s*:\s*([A-Z][A-Za-z\s,\.&]+?)(?:\s*\n|\s*$)',
        r'mortgage\s+servicer\s+(?:is|for)\s+([A-Z][A-Za-z\s,\.&]+?)(?:\s*,\s*whose|\s*\.)',
        r'(\$[\d,]+(?:\.\d{2})?)\.\s+([A-Z][A-Z\s,\.&]+?)(?:\s*,|\s+LLC|\s+is\s+)',
    ]:
        m = re.search(mort_pat, full_text, re.IGNORECASE | re.MULTILINE)
        if m:
            # For the last pattern, the mortgagee name is in group 2
            name = m.group(2) if m.lastindex >= 2 else m.group(1)
            name = name.strip().rstrip(",. ")
            # Skip garbage results
            if len(name) > 3 and not name.startswith("of "):
                result["mortgagee"] = name
                break

    log.info(f"  Parse result: grantor={result['grantor'][:40] if result['grantor'] else '(none)'} | "
             f"addr={result['property_address'][:40] if result['property_address'] else '(none)'} | "
             f"legal={'yes' if result['legal_description'] else 'no'}")
    return result


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  FRCL List Scraper
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def get_target_months() -> list:
    """Return the next 3 months as (year, month_number) tuples."""
    now = datetime.now()
    months = []
    for offset in range(1, 4):
        target = now.replace(day=1) + timedelta(days=32 * offset)
        target = target.replace(day=1)
        months.append((target.year, target.month))
    return months


def scrape_frcl_list(session: requests.Session, year: int, month: int) -> list:
    """Scrape the FRCL list page for a given month.

    Returns list of dicts: {"doc_id", "sale_date", "file_date", "pages"}
    """
    month_name = MONTH_NAMES[month]
    log.info(f"Scraping foreclosure list for {month_name} {year}...")

    # First, load the search page to get ViewState
    log.info(f"  Loading FRCL search page...")
    resp = session.get(FRCL_SEARCH_URL, timeout=(10, 30))
    resp.raise_for_status()
    log.info(f"  Search page loaded: {len(resp.text):,} bytes")
    soup = BeautifulSoup(resp.text, "html.parser")

    viewstate = soup.find("input", {"name": "__VIEWSTATE"})
    viewstate_val = viewstate["value"] if viewstate else ""
    event_val_tag = soup.find("input", {"name": "__EVENTVALIDATION"})
    event_val = event_val_tag["value"] if event_val_tag else ""
    viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
    viewstate_gen_val = viewstate_gen["value"] if viewstate_gen else ""

    # Submit the search form
    post_data = {
        "__VIEWSTATE": viewstate_val,
        "__VIEWSTATEGENERATOR": viewstate_gen_val,
        "__EVENTVALIDATION": event_val,
        "__VIEWSTATEENCRYPTED": "",
        "ctl00$ContentPlaceHolder1$rbtlDate": "SaleDate",
        "ctl00$ContentPlaceHolder1$ddlYear": str(year),
        "ctl00$ContentPlaceHolder1$ddlMonth": str(month),
        "ctl00$ContentPlaceHolder1$btnSearch": "SEARCH",
        "ctl00$ContentPlaceHolder1$txtFileNo": "",
    }

    log.info(f"  Submitting search form for {month_name} {year}...")
    resp = session.post(FRCL_SEARCH_URL, data=post_data, timeout=(15, 120))
    resp.raise_for_status()
    log.info(f"  Response received: {len(resp.text):,} bytes")
    soup = BeautifulSoup(resp.text, "html.parser")

    # Parse the results table
    records = []
    table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})
    if not table:
        # Check if results text shows count
        count_text = soup.find(string=re.compile(r'\d+\s+Row\(s\)\s+Found', re.IGNORECASE))
        if count_text:
            log.info(f"  Found results text but no table yet")
        else:
            log.info(f"  No results found for {month_name} {year}")
            return records

    # Parse all pages Ã¢ÂÂ with safety limits to prevent infinite loops
    MAX_PAGES = 100  # Hard cap: 100 pages ÃÂ 20 results = 2,000 max
    page_num = 1
    seen_doc_ids = set()

    while True:
        prev_count = len(records)
        rows = table.find_all("tr") if table else []
        for row in rows:
            # Skip pager rows (class pagination-ys) and header rows (class bg-color-transblue)
            row_class = row.get("class", [])
            if isinstance(row_class, list):
                row_class = " ".join(row_class)
            if "pagination" in row_class or "bg-color" in row_class:
                continue

            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            # Cell layout: [0]=empty/checkbox, [1]=Doc ID (link), [2]=Sale Date, [3]=File Date, [4]=Pgs
            link = cells[1].find("a")
            if not link:
                continue

            doc_id = link.get_text(strip=True)
            if not doc_id.startswith("FRCL"):
                continue

            # Skip duplicates (prevents infinite loop re-scraping same page)
            if doc_id in seen_doc_ids:
                continue
            seen_doc_ids.add(doc_id)

            sale_date = cells[2].get_text(strip=True)
            file_date = cells[3].get_text(strip=True)
            pages = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            # Capture the actual PDF viewer URL (uses encrypted token, not plain doc ID)
            pdf_href = link.get("href", "")
            if pdf_href and not pdf_href.startswith("http"):
                pdf_href = "https://www.cclerk.hctx.net/applications/websearch/" + pdf_href

            records.append({
                "doc_id": doc_id,
                "sale_date": sale_date,
                "file_date": file_date,
                "pages": pages,
                "pdf_url": pdf_href,
            })

        new_on_page = len(records) - prev_count
        log.info(f"  Page {page_num}: {new_on_page} new records (total: {len(records)})")

        # If this page added zero new records, we've seen them all Ã¢ÂÂ stop
        if new_on_page == 0 and page_num > 1:
            log.info(f"  No new records on page {page_num} Ã¢ÂÂ pagination complete")
            break

        # Hard cap to prevent runaway loops
        if page_num >= MAX_PAGES:
            log.warning(f"  Hit max page limit ({MAX_PAGES}) Ã¢ÂÂ stopping pagination")
            break

        # Check for next page Ã¢ÂÂ pager row has class "pagination-ys"
        pager = soup.find("tr", class_="pagination-ys")
        if not pager:
            break

        # Find the next numbered page link
        page_num += 1
        next_link = None
        for a in pager.find_all("a"):
            try:
                page_text = a.get_text(strip=True)
                if page_text == str(page_num):
                    next_link = a
                    break
            except:
                pass

        # If exact page number not found, look for "..." but ONLY use
        # the LAST "..." link (which goes forward, not backward)
        if not next_link:
            dots_links = [a for a in pager.find_all("a") if a.get_text(strip=True) == "..."]
            if dots_links:
                next_link = dots_links[-1]  # last "..." = forward
            if not next_link:
                log.info(f"  No more page links found Ã¢ÂÂ pagination complete")
                break

        # Get the postback for the next page
        href = next_link.get("href", "")
        m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
        if not m:
            break

        event_target = m.group(1)
        event_arg = m.group(2)

        # Re-extract viewstate from current page
        viewstate_tag = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate_val = viewstate_tag["value"] if viewstate_tag else ""
        event_val_tag = soup.find("input", {"name": "__EVENTVALIDATION"})
        event_val = event_val_tag["value"] if event_val_tag else ""

        post_data = {
            "__VIEWSTATE": viewstate_val,
            "__VIEWSTATEGENERATOR": viewstate_gen_val,
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": event_val,
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": event_arg,
        }

        time.sleep(0.5)
        log.info(f"  Loading page {page_num}...")
        resp = session.post(FRCL_SEARCH_URL, data=post_data, timeout=(15, 60))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})

        if not table:
            break

    log.info(f"  Found {len(records)} foreclosure postings for {month_name} {year}")
    return records


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  Login to County Clerk (required to download PDF documents)
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def login_to_cclerk(session: requests.Session) -> bool:
    """Log in to the Harris County Clerk website.

    The clerk site requires authentication to view/download PDF documents.
    Credentials are read from environment variables CCLERK_USERNAME and
    CCLERK_PASSWORD (set as GitHub Actions secrets).

    Returns True on success, False on failure.
    """
    username = os.environ.get("CCLERK_USERNAME", "")
    password = os.environ.get("CCLERK_PASSWORD", "")

    if not username or not password:
        log.warning("CCLERK_USERNAME / CCLERK_PASSWORD not set Ã¢ÂÂ PDF downloads will fail")
        return False

    try:
        # Step 1: GET the login page to grab ASP.NET ViewState tokens
        resp = session.get(CCLERK_LOGIN_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract hidden form fields
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate = viewstate["value"] if viewstate else ""
        generator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        generator = generator["value"] if generator else ""
        encrypted = soup.find("input", {"name": "__VIEWSTATEENCRYPTED"})
        encrypted = encrypted["value"] if encrypted else ""
        validation = soup.find("input", {"name": "__EVENTVALIDATION"})
        validation = validation["value"] if validation else ""
        prevpage = soup.find("input", {"name": "__PREVIOUSPAGE"})
        prevpage = prevpage["value"] if prevpage else ""

        # Step 2: POST the login form
        login_data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": generator,
            "__VIEWSTATEENCRYPTED": encrypted,
            "__PREVIOUSPAGE": prevpage,
            "__EVENTVALIDATION": validation,
            "ctl00$ContentPlaceHolder1$Login1$UserName": username,
            "ctl00$ContentPlaceHolder1$Login1$Password": password,
            "ctl00$ContentPlaceHolder1$Login1$LoginButton": "LOG IN",
        }

        resp = session.post(CCLERK_LOGIN_URL, data=login_data, timeout=30,
                           allow_redirects=True)
        resp.raise_for_status()

        # Check for successful login: page should contain LOGOUT or WELCOME
        if "LOGOUT" in resp.text.upper() or "WELCOME" in resp.text.upper():
            log.info("Successfully logged in to Harris County Clerk")
            return True
        else:
            log.warning("Login POST succeeded but could not confirm authentication")
            return False

    except Exception as e:
        log.warning(f"Login failed: {e}")
        return False


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  PDF Downloader
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def download_frcl_pdf(session: requests.Session, doc_id: str,
                      pdf_url: str = "") -> Optional[bytes]:
    """Download a foreclosure PDF using its encrypted viewer URL.

    The county clerk uses encrypted token URLs (ViewECdocs.aspx?ID=<token>)
    that are only available from the search results page.  Plain doc-ID URLs
    (ViewECdocs.aspx?id=FRCL-2026-612) return HTML, not a PDF.
    """
    url = pdf_url if pdf_url else f"{FRCL_DOC_URL}?id={doc_id}"
    log.info(f"  Downloading PDF: {url[:120]}...")

    try:
        resp = session.get(url, timeout=60, allow_redirects=True)
        log.info(f"  Response: status={resp.status_code}, type={resp.headers.get('Content-Type','')}, size={len(resp.content)} bytes, url={resp.url[:120]}")
        resp.raise_for_status()

        # Check if we got redirected to the login page
        if "Login.aspx" in resp.url:
            log.warning(f"  Redirected to login page Ã¢ÂÂ session expired!")
            return None

        # Check if we got a PDF
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" in content_type.lower() or resp.content[:4] == b"%PDF":
            log.info(f"  PDF downloaded OK: {len(resp.content):,} bytes")
            return resp.content
        else:
            log.warning(f"  Non-PDF response for {doc_id}: {content_type}")
            # Log first 200 chars of response to help debug
            log.warning(f"  Response preview: {resp.text[:200]}")
            return None
    except Exception as e:
        log.warning(f"  Failed to download {doc_id}: {e}")
        return None


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
#  Main
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def main():
    log.info("Harris County Foreclosure Scraper")
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"Date: {today}")

    # Create directories
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    # Load previously seen doc IDs
    seen_ids = set()
    existing_records = []
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f:
            seen_data = json.load(f)
            seen_ids = set(seen_data.get("seen_ids", []))
            log.info(f"Loaded {len(seen_ids)} previously seen foreclosure IDs")

    # Load existing foreclosure records
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing_data = json.load(f)
            existing_records = existing_data.get("records", [])
            log.info(f"Loaded {len(existing_records)} existing foreclosure records")

    # Generate batch ID for this run (B1, B2, B3, ...)
    existing_batch_nums = set()
    for rec in existing_records:
        bid = rec.get("batch_id", "")
        if bid.startswith("B"):
            try:
                existing_batch_nums.add(int(bid[1:]))
            except ValueError:
                pass
    next_batch_num = max(existing_batch_nums, default=0) + 1
    batch_id = f"B{next_batch_num}"
    now_ts = datetime.now(CT).strftime("%Y-%m-%d %I:%M %p %Z")
    run_number = os.environ.get("GITHUB_RUN_NUMBER", "")
    run_tag = f"Run #{run_number} / " if run_number else ""
    batch_label = f"{run_tag}Batch {next_batch_num} Ã¢ÂÂ {now_ts}"
    log.info(f"This run: {batch_id} ({batch_label})")

    # Set up HTTP session with realistic browser headers
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx",
    })

    # Log in to county clerk (required to download PDFs)
    logged_in = login_to_cclerk(session)
    if not logged_in:
        log.warning("Proceeding without login Ã¢ÂÂ PDF downloads may fail")

    # Get target months (next 3 months)
    target_months = get_target_months()
    log.info(f"Target months: {[f'{MONTH_NAMES[m]} {y}' for y, m in target_months]}")

    # Scrape FRCL list for each month
    all_listings = []
    for year, month in target_months:
        listings = scrape_frcl_list(session, year, month)
        all_listings.extend(listings)

    log.info(f"Total foreclosure listings found: {len(all_listings)}")

    # Find doc IDs that need OCR reprocessing (previously failed to extract text)
    ocr_retry_ids = set()
    reparse_ids = set()
    for rec in existing_records:
        if rec.get("needs_ocr") and not rec.get("grantor"):
            ocr_retry_ids.add(rec["doc_id"])
        # Re-process records that have no grantor AND no address Ã¢ÂÂ parser likely failed
        elif not rec.get("grantor") and (not rec.get("prop_address") or rec.get("prop_address") == "Not found"):
            reparse_ids.add(rec["doc_id"])
    if ocr_retry_ids:
        log.info(f"Found {len(ocr_retry_ids)} records needing OCR reprocessing")
    if reparse_ids:
        log.info(f"Found {len(reparse_ids)} records with missing grantor+address Ã¢ÂÂ will re-parse")

    # Filter to new doc IDs PLUS OCR retries PLUS records needing re-parsing
    retry_ids = ocr_retry_ids | reparse_ids
    new_listings = [l for l in all_listings if l["doc_id"] not in seen_ids or l["doc_id"] in retry_ids]
    log.info(f"New listings to process: {len(new_listings)} (skipping {len(all_listings) - len(new_listings)} already seen)")

    if not new_listings:
        log.info("No new foreclosure listings to process")
        # Still save output to update the dashboard
        _save_output(existing_records, seen_ids, today, target_months)
        return

    # Apply batch limit to stay within GitHub Actions timeout
    if len(new_listings) > MAX_PDFS_PER_RUN:
        log.info(f"Batch limit: processing first {MAX_PDFS_PER_RUN} of {len(new_listings)} new listings")
        new_listings = new_listings[:MAX_PDFS_PER_RUN]

    # Load HCAD bulk data for address enrichment
    # Use a SEPARATE session so we don't clobber the clerk login cookies
    hcad = HCADMatcher()
    hcad_session = requests.Session()
    hcad_session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    if len(new_listings) >= 100:
        try:
            hcad.load(hcad_session)
        except Exception as e:
            log.warning(f"HCAD bulk load failed (will use API fallback): {e}")
    else:
        log.info("Small batch Ã¢ÂÂ skipping HCAD bulk download, using ArcGIS API only")

    # Re-login before PDF downloads (session may have gone stale during HCAD load)
    log.info("Re-authenticating with county clerk before PDF downloads...")
    logged_in = login_to_cclerk(session)
    if not logged_in:
        log.warning("Re-login failed Ã¢ÂÂ PDF downloads may fail")

    # Process each new listing
    new_records = []
    api_session = requests.Session()
    api_session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })

    log.info(f"=== Starting PDF processing: {len(new_listings)} PDFs to download ===")

    # Diagnostic: dump full extracted text for first 5 PDFs to help debug parsing
    DIAG_LIMIT = 10
    diag_count = 0

    # Per-PDF timeout handler
    class PdfTimeout(Exception):
        pass

    def _timeout_handler(signum, frame):
        raise PdfTimeout("PDF processing exceeded time limit")

    for i, listing in enumerate(new_listings):
        doc_id = listing["doc_id"]
        t0 = time.time()
        log.info(f"Processing {doc_id} ({i+1}/{len(new_listings)})...")

        try:
            # Set per-PDF timeout
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(PDF_TIMEOUT)

            # Download PDF using encrypted token URL from search results
            pdf_bytes = download_frcl_pdf(session, doc_id, listing.get("pdf_url", ""))
            if not pdf_bytes:
                log.warning(f"  Skipping {doc_id} - could not download PDF")
                seen_ids.add(doc_id)  # Mark as seen so we don't retry
                signal.alarm(0)
                continue

            # Parse PDF - try Gemini Vision first, fallback to OCR
            parsed = parse_pdf_with_gemini(pdf_bytes, doc_id)
            if parsed is None:
                parsed = parse_foreclosure_pdf(pdf_bytes, doc_id)
            else:
                # Rate limit for Gemini free tier (15 RPM)
                time.sleep(4.5)
            signal.alarm(0)  # Cancel timeout after successful parse
            log.info(f"  PDF parsed in {time.time()-t0:.1f}s Ã¢ÂÂ grantor={parsed.get('grantor','')[:30]}")

            # Diagnostic: dump full extracted text for first N PDFs
            if diag_count < DIAG_LIMIT:
                diag_count += 1
                try:
                    with pdfplumber.open(BytesIO(pdf_bytes)) as dpdf:
                        diag_text = ""
                        for pg in dpdf.pages:
                            diag_text += (pg.extract_text() or "") + "\n"
                    log.info(f"  ===== DIAGNOSTIC TEXT DUMP ({doc_id}) =====")
                    # Print first 2000 chars so we can see the actual content
                    for chunk_start in range(0, min(len(diag_text), 2000), 200):
                        chunk = diag_text[chunk_start:chunk_start + 200].replace('\n', ' | ')
                        log.info(f"  DIAG[{chunk_start}]: {chunk}")
                    log.info(f"  ===== END DIAGNOSTIC ({doc_id}, total {len(diag_text)} chars) ====="  )
                except Exception as de:
                    log.info(f"  DIAG ERROR for {doc_id}: {de}")
        except PdfTimeout:
            signal.alarm(0)
            log.warning(f"  TIMEOUT: {doc_id} exceeded {PDF_TIMEOUT}s Ã¢ÂÂ skipping")
            seen_ids.add(doc_id)
            continue
        except Exception as e:
            signal.alarm(0)
            log.warning(f"  ERROR processing {doc_id}: {e} Ã¢ÂÂ skipping")
            seen_ids.add(doc_id)
            continue

        # Build record
        record = {
            "doc_id": doc_id,
            "doc_type": "FRCL",
            "cat": "FORECLOSURE",
            "cat_label": "Foreclosure Posting",
            "sale_date": listing["sale_date"] or parsed.get("sale_date", ""),
            "file_date": listing["file_date"],
            "date_scraped": today,
            "batch_id": batch_id,
            "batch_label": batch_label,
            "grantor": parsed.get("grantor", ""),
            "co_borrower": parsed.get("co_borrower", ""),
            "amount": parsed.get("amount", ""),
            "mortgagee": parsed.get("mortgagee", ""),
            "legal_description": parsed.get("legal_description", ""),
            "prop_address": parsed.get("property_address", ""),
            "prop_city": parsed.get("property_city", ""),
            "prop_state": parsed.get("property_state", "TX"),
            "prop_zip": parsed.get("property_zip", ""),
            "match_confidence": "none",
            "hcad_url": "",
            "clerk_url": listing.get("pdf_url", f"https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx?id={doc_id}"),
            "needs_ocr": parsed.get("needs_ocr", False),
        }

        # Ã¢ÂÂÃ¢ÂÂ Enrich with HCAD data Ã¢ÂÂÃ¢ÂÂ
        # Step 1: If we got address from PDF, we're done (high confidence)
        if record["prop_address"]:
            record["match_confidence"] = "high"
            log.info(f"  Address from PDF: {record['prop_address']}, {record['prop_city']}")
        else:
            # Step 2: Try legal description matching
            if parsed.get("legal_description"):
                legal_parsed = convert_legal_numbers(parsed["legal_description"])
                hcad_match = hcad.match_legal(legal_parsed)
                if hcad_match:
                    record["prop_address"] = hcad_match["prop_address"]
                    record["prop_city"] = hcad_match.get("prop_city", "Houston")
                    record["prop_state"] = "TX"
                    record["prop_zip"] = hcad_match.get("prop_zip", "")
                    record["match_confidence"] = "high"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{hcad_match['acct']}"
                    log.info(f"  Legal desc match: {record['prop_address']}")

            # Step 3: Try owner name matching against bulk data
            if not record["prop_address"] and parsed.get("grantor"):
                grantor_name = parsed["grantor"].upper()
                hcad_match = hcad.match_owner(grantor_name)
                if hcad_match:
                    record["prop_address"] = hcad_match["prop_address"]
                    record["prop_city"] = hcad_match.get("prop_city", "Houston")
                    record["prop_state"] = "TX"
                    record["prop_zip"] = hcad_match.get("prop_zip", "")
                    record["match_confidence"] = "medium"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{hcad_match['acct']}"
                    log.info(f"  Owner name match: {record['prop_address']}")

            # Step 4: Try HCAD ArcGIS API as last resort
            if not record["prop_address"] and parsed.get("grantor"):
                grantor_name = parsed["grantor"].upper()
                api_result = hcad_api_search(api_session, grantor_name)
                if api_result:
                    record["prop_address"] = api_result["street"]
                    record["prop_city"] = api_result["city"]
                    record["prop_state"] = "TX"
                    record["prop_zip"] = api_result["zip"]
                    record["match_confidence"] = "medium"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{api_result['acct']}"
                time.sleep(0.25)

        # If still no address, set "Not found"
        if not record["prop_address"]:
            record["prop_address"] = "Not found"
            log.info(f"  No address found for {doc_id}")

        new_records.append(record)
        seen_ids.add(doc_id)
        elapsed = time.time() - t0
        log.info(f"  Done {doc_id} in {elapsed:.1f}s Ã¢ÂÂ addr={record['prop_address'][:40]}")

        # Rate limit between PDF downloads
        time.sleep(0.3)

    log.info(f"Processed {len(new_records)} new foreclosure records")

    # Merge with existing records (update if doc_id already exists)
    existing_by_id = {r["doc_id"]: r for r in existing_records}
    for rec in new_records:
        existing_by_id[rec["doc_id"]] = rec

    all_records = list(existing_by_id.values())

    # Remove records with sale dates that have already passed
    now = datetime.now()
    active_records = []
    for rec in all_records:
        # Keep record if we can't parse the sale date (be safe)
        try:
            sd = rec.get("sale_date", "")
            if sd:
                # Try parsing various date formats
                for fmt in ("%m/%d/%Y", "%B %d, %Y", "%Y-%m-%d"):
                    try:
                        sale_dt = datetime.strptime(sd, fmt)
                        if sale_dt < now - timedelta(days=7):
                            # Sale date has passed by more than a week, skip
                            continue
                        break
                    except ValueError:
                        continue
            active_records.append(rec)
        except:
            active_records.append(rec)

    _save_output(active_records, seen_ids, today, target_months)


def _save_output(records: list, seen_ids: set, today: str, target_months: list):
    """Save foreclosure records and seen IDs."""
    # Sort by sale date
    records.sort(key=lambda r: r.get("sale_date", ""), reverse=False)

    with_address = sum(1 for r in records if r.get("prop_address", "").strip()
                       and r["prop_address"] != "Not found")

    output = {
        "fetched_at": datetime.now(CT).isoformat(),
        "source": "Harris County Clerk - Foreclosure Postings (cclerk.hctx.net)",
        "target_months": [f"{MONTH_NAMES[m]} {y}" for y, m in target_months],
        "total": len(records),
        "with_address": with_address,
        "records": records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    log.info(f"Saved {OUTPUT_FILE}: {len(records)} records, {with_address} with address")

    # Save seen IDs
    with open(SEEN_FILE, "w") as f:
        json.dump({"seen_ids": sorted(seen_ids), "last_updated": today}, f, indent=2)
    log.info(f"Saved {SEEN_FILE}: {len(seen_ids)} tracked IDs")

    log.info(f"Done. Total: {len(records)} | With address: {with_address}")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Harris County Foreclosure Posting Scraper
==========================================
Scrapes the FRCL (Foreclosure) page on cclerk.hctx.net for the next 3 months,
downloads and parses PDF documents, and enriches with HCAD address data.

Runs as a separate workflow from the main motivated-seller scraper.
"""

import json
import logging
import os
import re
import signal
import sys
import time
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# PDF text extraction
try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber is required. Install with: pip install pdfplumber")
    sys.exit(1)

# OCR for scanned image PDFs
try:
    import pytesseract
    from PIL import Image, ImageFilter, ImageEnhance
    from pdf2image import convert_from_bytes
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    print("WARNING: pytesseract/Pillow/pdf2image not installed. OCR will be unavailable.")

def preprocess_for_ocr(img):
    """Pre-process a PIL Image for better OCR accuracy on scanned legal docs."""
    # Convert to grayscale
    img = img.convert("L")
    # Increase contrast â makes faded text darker, background lighter
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(1.8)
    # Sharpen to improve edge definition on text
    img = img.filter(ImageFilter.SHARPEN)
    # Adaptive threshold: convert to pure black & white
    # Use a threshold that works well for legal docs (slightly above midpoint)
    img = img.point(lambda x: 255 if x > 140 else 0, mode='1')
    # Convert back to grayscale for Tesseract (some modes need 8-bit)
    img = img.convert("L")
    return img

# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  Logging
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  Constants
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
FRCL_SEARCH_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
FRCL_DOC_URL = "https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx"
CCLERK_LOGIN_URL = "https://www.cclerk.hctx.net/Applications/WebSearch/Registration/Login.aspx"

HCAD_BULK_URL = "https://download.hcad.org/data/CAMA/2026/Real_acct_owner.zip"

# Batch limit: process at most this many PDFs per run to stay within
# GitHub Actions timeout.  Remaining PDFs are picked up on the next run
# because we track seen_ids incrementally.
MAX_PDFS_PER_RUN = 200

# OCR resolution â 200 DPI is a good balance of speed and accuracy
OCR_DPI = 300

# Per-PDF timeout in seconds â kill stuck OCR processing
PDF_TIMEOUT = 120

DASHBOARD_DIR = Path("dashboard")
DATA_DIR = Path("data")
SEEN_FILE = DATA_DIR / "foreclosure_ids.json"
OUTPUT_FILE = DASHBOARD_DIR / "foreclosures.json"

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

MONTH_ABBREVS = {
    "January": "January", "February": "February", "March": "March",
    "April": "April", "May": "May", "June": "June",
    "July": "July", "August": "August", "September": "September",
    "October": "October", "November": "November", "December": "December",
}


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  Word-to-Number Converter (for legal descriptions)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
WORD_NUMS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
    "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}


def words_to_number(text: str) -> Optional[int]:
    """Convert written-out numbers to integers.

    Examples:
        "TWENTY-FOUR" â 24
        "EIGHT" â 8
        "ONE HUNDRED TWENTY-THREE" â 123
    """
    text = text.strip().lower().replace("-", " ")
    parts = text.split()
    if not parts:
        return None

    total = 0
    current = 0
    for word in parts:
        if word == "hundred":
            current = (current if current else 1) * 100
        elif word in WORD_NUMS:
            current += WORD_NUMS[word]
        else:
            return None  # unrecognized word

    total += current
    return total if total > 0 else None


def convert_legal_numbers(text: str) -> str:
    """Convert spelled-out lot/block numbers in legal descriptions.

    "LOT TWENTY-FOUR (24), IN BLOCK TWO (2)" stays as-is because it
    already has the digits in parens.

    "LOT TWENTY-FOUR, BLOCK TWO" becomes "LT 24 BLK 2".
    """
    # If parenthesized numbers exist, extract them
    # Pattern: LOT TWENTY-FOUR (24)
    lot_paren = re.search(r'LOT\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)
    blk_paren = re.search(r'BLOCK\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)
    sec_paren = re.search(r'SEC(?:TION)?\s+[A-Z\-\s]+\((\d+)\)', text, re.IGNORECASE)

    lot_num = lot_paren.group(1) if lot_paren else None
    blk_num = blk_paren.group(1) if blk_paren else None
    sec_num = sec_paren.group(1) if sec_paren else None

    # If no parens, try to convert the word numbers
    if not lot_num:
        m = re.search(r'LOT\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+IN\b|\s+OF\b|\s+BL)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                lot_num = str(n)

    if not blk_num:
        m = re.search(r'BLOCK\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+OF\b|\s+SEC|\s+A\s)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                blk_num = str(n)

    if not sec_num:
        m = re.search(r'SEC(?:TION)?\s+([A-Z][A-Z\-\s]+?)(?:\s*,|\s+A\s|\s+IN\b|$)', text, re.IGNORECASE)
        if m:
            n = words_to_number(m.group(1))
            if n:
                sec_num = str(n)

    # Extract subdivision name
    subdiv = None
    # Pattern 1: "OF <SUBDIVISION NAME>, SEC..."  or  "OF <NAME>, A SUBDIVISION"
    m = re.search(r'(?:IN\s+BLOCK\s+\S+\s+OF|,\s+OF)\s+([A-Z][A-Z\s&\']+?)(?:\s*,|\s+SEC|\s+A\s+SUB|\s+IN\s+HARRIS)',
                  text, re.IGNORECASE)
    if m:
        subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 2: "BLOCK X, <NAME> SEC Y"
        m = re.search(r'BLOCK\s+\S+\s*,?\s+([A-Z][A-Z\s&\']+?)(?:\s+SEC|\s*,\s*A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()
            subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE)

    if not subdiv:
        # Pattern 3: "BLOCK WORD (NUM) <SUBDIVISION> SECTION" â common in foreclosure OCR
        m = re.search(r'BLOCK\s+[A-Z\-\s]+\(\d+\)\s+([A-Z][A-Z\s&\']+?)(?:\s+SEC(?:TION)?)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 4: "BLOCK NUM <SUBDIVISION> SECTION" (no parenthesized number)
        m = re.search(r'BLOCK\s+\d+\s*,?\s*(?:OF\s+)?([A-Z][A-Z\s&\']+?)(?:\s+SEC(?:TION)?|\s*,\s*A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()

    if not subdiv:
        # Pattern 5: "IN BLOCK ... OF <SUBDIVISION>" without SEC following
        m = re.search(r'IN\s+BLOCK\s+\S+\s*,?\s+(?:OF\s+)?([A-Z][A-Z\s&\']+?)(?:\s*,|\s+A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()
            subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE)

    # Clean subdivision name
    if subdiv:
        # Strip leading "OF" that sometimes gets captured
        subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE).strip()
        # Fix common OCR garbles in subdivision names
        subdiv = re.sub(r'FORTST\b', 'FOREST', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'FORST\b', 'FOREST', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT\b', 'ESTATE', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT5\b', 'ESTATES', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'ESTAT[E3]S?\b', 'ESTATES', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'VILLAG\b', 'VILLAGE', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'HEIGH?TS?\b', 'HEIGHTS', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'GARDNS?\b', 'GARDENS', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'MANOH\b', 'MANOR', subdiv, flags=re.IGNORECASE)
        subdiv = re.sub(r'PLAC\b', 'PLACE', subdiv, flags=re.IGNORECASE)

    return {
        "lot": lot_num,
        "block": blk_num,
        "section": sec_num,
        "subdivision": subdiv,
        "raw": text,
    }


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  HCAD Bulk Data Loader (shared with main scraper)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
class HCADMatcher:
    """Loads HCAD bulk data and matches by legal description or owner name."""

    def __init__(self):
        self.lookup = {}       # normalized owner name â address dict
        self.legal_index = {}  # "SUBDIVISION|BLKn" â address dict

    def load(self, session: requests.Session):
        """Download and parse HCAD bulk data."""
        log.info("Loading HCAD parcel data for address enrichment...")
        log.info(f"  Downloading {HCAD_BULK_URL} (attempt 1)")

        for attempt in range(1, 4):
            try:
                resp = session.get(HCAD_BULK_URL, timeout=300)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt < 3:
                    log.warning(f"  Download failed (attempt {attempt}): {e}")
                    time.sleep(5)
                else:
                    raise

        log.info(f"  Download OK - {len(resp.content):,} bytes")

        zf = zipfile.ZipFile(BytesIO(resp.content))
        log.info(f"  Zip contains: {zf.namelist()}")

        # Parse real_acct.txt
        with zf.open("real_acct.txt") as f:
            lines = f.read().decode("utf-8", errors="replace").splitlines()

        log.info(f"  Parsing primary address data from: real_acct.txt")

        # Column mapping (tab-delimited, 0-indexed)
        # acct=0, mailto=2, site_addr_1=17, site_addr_3=19,
        # mail_city=5, lgl_1=66, lgl_2=67, lgl_3=68
        count = 0
        for line in lines[1:]:  # skip header
            cols = line.split("\t")
            if len(cols) < 70:
                continue

            acct = cols[0].strip()
            owner = cols[2].strip().upper()  # mailto field = owner name
            site_addr = cols[17].strip()
            site_city = cols[19].strip() if len(cols) > 19 else ""
            site_zip = cols[20].strip() if len(cols) > 20 else ""

            if not owner or not site_addr:
                continue

            info = {
                "acct": acct,
                "prop_address": site_addr,
                "prop_city": site_city or "Houston",
                "prop_state": "TX",
                "prop_zip": site_zip,
            }

            # Owner name lookup
            norm_owner = re.sub(r'\s+', ' ', owner).strip()
            if norm_owner and norm_owner not in self.lookup:
                self.lookup[norm_owner] = info

            # Legal description index
            lgl_1 = cols[66].strip().upper() if len(cols) > 66 else ""
            lgl_2 = cols[67].strip().upper() if len(cols) > 67 else ""

            if lgl_1 and lgl_2:
                # Parse subdivision from lgl_1, block from lgl_2
                subdiv = re.sub(r'\s+', ' ', lgl_1).strip()
                blk_match = re.search(r'BLK\s*(\S+)', lgl_2)
                if subdiv and blk_match:
                    key = f"{subdiv}|BLK{blk_match.group(1)}"
                    if key not in self.legal_index:
                        self.legal_index[key] = info

            count += 1

        log.info(f"  Loaded {count:,} parcels, {len(self.legal_index):,} legal keys")

        # Also parse owners.txt for supplemental names
        if "owners.txt" in zf.namelist():
            with zf.open("owners.txt") as f:
                owner_lines = f.read().decode("utf-8", errors="replace").splitlines()
            # Build a quick acct â info index so we don't scan the entire dict per row
            acct_index = {}
            for existing_info in self.lookup.values():
                a = existing_info.get("acct")
                if a and a not in acct_index:
                    acct_index[a] = existing_info
            added = 0
            for line in owner_lines[1:]:
                cols = line.split("\t")
                if len(cols) < 3:
                    continue
                acct = cols[0].strip()
                name = cols[2].strip().upper()
                norm = re.sub(r'\s+', ' ', name).strip()
                if norm and norm not in self.lookup and acct in acct_index:
                    self.lookup[norm] = acct_index[acct]
                    added += 1
            log.info(f"  owners.txt: added {added:,} supplemental name variants")

    def match_legal(self, parsed_legal: dict) -> Optional[dict]:
        """Try to match by legal description (subdivision + block)."""
        subdiv = parsed_legal.get("subdivision", "")
        blk = parsed_legal.get("block", "")
        lot = parsed_legal.get("lot", "")

        if not subdiv or not blk:
            return None

        subdiv_norm = re.sub(r'\s+', ' ', subdiv.upper()).strip()
        key = f"{subdiv_norm}|BLK{blk}"

        if key in self.legal_index:
            return self.legal_index[key]

        # Try with SEC appended
        sec = parsed_legal.get("section", "")
        if sec:
            key_sec = f"{subdiv_norm} SEC {sec}|BLK{blk}"
            if key_sec in self.legal_index:
                return self.legal_index[key_sec]

        # Fuzzy match: find HCAD keys that contain a similar subdivision name
        # This handles OCR garbles like "RDE FOREST" â "JADE FOREST" or "LAKEWOOD FORTST" â "LAKEWOOD FOREST"
        if len(subdiv_norm) >= 5:
            # Extract all words from the subdivision name
            subdiv_words = subdiv_norm.split()
            best_match = None
            best_score = 0
            suffix = f"|BLK{blk}"
            for hcad_key in self.legal_index:
                if not hcad_key.endswith(suffix):
                    continue
                hcad_subdiv = hcad_key.replace(suffix, "")
                # Check if SEC matches when present
                if sec and f"SEC {sec}" not in hcad_subdiv and f"SEC {sec}" in key_sec:
                    continue
                # Score: count matching words (at least half must match)
                hcad_words = hcad_subdiv.split()
                matching = sum(1 for w in subdiv_words if w in hcad_words)
                # Also check if the first significant word matches (most reliable)
                first_word_match = len(subdiv_words) > 0 and len(hcad_words) > 0 and subdiv_words[0] == hcad_words[0]
                if matching >= len(subdiv_words) * 0.5 and first_word_match and matching > best_score:
                    best_score = matching
                    best_match = hcad_key
            if best_match:
                log.info(f"  Fuzzy legal match: '{subdiv_norm}' â '{best_match.split('|')[0]}'")
                return self.legal_index[best_match]

        return None

    def export_lookup(self, filepath):
        """Export the legal_index as a JSON file for the dashboard lookup tool."""
        # Convert to a simpler format: key â { address, city, zip, acct }
        lookup = {}
        for key, info in self.legal_index.items():
            lookup[key] = {
                "a": info.get("prop_address", ""),
                "c": info.get("prop_city", ""),
                "z": info.get("prop_zip", ""),
                "t": info.get("acct", ""),
            }
        with open(filepath, "w") as f:
            json.dump(lookup, f, separators=(",", ":"))
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        log.info(f"Exported legal lookup: {len(lookup):,} entries, {size_mb:.1f} MB â {filepath}")

    def match_owner(self, name: str) -> Optional[dict]:
        """Try to match by owner name (exact)."""
        norm = re.sub(r'\s+', ' ', name.upper()).strip()
        if norm in self.lookup:
            return self.lookup[norm]

        # Try LAST FIRST reversal
        parts = norm.split()
        if len(parts) >= 2:
            reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
            if reversed_name in self.lookup:
                return self.lookup[reversed_name]

        return None


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  HCAD ArcGIS API (fallback for when bulk data doesn't match)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
HCAD_ARCGIS_URL = (
    "https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0/query"
)
HCAD_DETAIL_BASE = "https://public.hcad.org/records/details.asp?cession=A&acct="


def hcad_api_search(session: requests.Session, name: str) -> Optional[dict]:
    """Search HCAD ArcGIS REST API by owner name."""
    safe = name.strip().upper().replace("'", "''")
    if not safe or safe in ("SEE INSTRUMENT", "SEE DOCUMENT", "UNKNOWN"):
        return None

    where = (
        f"upper(owner_name_1) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_2) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_3) LIKE upper('%{safe}%')"
    )
    params = {
        "where":             where,
        "outFields":         ("HCAD_NUM,owner_name_1,owner_name_2,owner_name_3,"
                              "site_str_num,site_str_name,site_str_sfx,"
                              "site_str_pfx,site_str_sfx_dir,"
                              "site_city,site_zip,state_class"),
        "returnGeometry":    "false",
        "f":                 "json",
        "resultRecordCount": "10",
    }

    try:
        resp = session.get(HCAD_ARCGIS_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"HCAD API error for '{name}': {e}")
        return None

    features = data.get("features", [])
    if not features:
        log.info(f"HCAD API: no results for '{name}'")
        return None

    # Prefer residential properties (state_class starting with A or B)
    best = None
    for feat in features:
        attr = feat.get("attributes", {})
        sc = (attr.get("state_class") or "").upper()
        if sc.startswith("A") or sc.startswith("B"):
            best = attr
            break

    if not best:
        best = features[0].get("attributes", {})

    # Build street from split fields
    parts = []
    pfx = str(best.get("site_str_pfx") or "").strip()
    num = str(best.get("site_str_num") or "").strip()
    sname = str(best.get("site_str_name") or "").strip()
    sfx = str(best.get("site_str_sfx") or "").strip()
    sdir = str(best.get("site_str_sfx_dir") or "").strip()
    if pfx:
        parts.append(pfx)
    if num:
        parts.append(num)
    if sname:
        parts.append(sname)
    if sfx:
        parts.append(sfx)
    if sdir:
        parts.append(sdir)
    street = " ".join(parts)

    if not street:
        return None

    city = str(best.get("site_city") or "").strip()
    zipcode = str(best.get("site_zip") or "").strip()
    acct = str(best.get("HCAD_NUM") or "").strip()
    owner = str(best.get("owner_name_1") or "").strip()

    log.info(f"HCAD API: found residential '{owner}' -> {street}, {city} {zipcode}")
    return {
        "acct": acct,
        "owner_name": owner,
        "street": street,
        "city": city or "Houston",
        "state": "TX",
        "zip": zipcode,
    }


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  PDF Parser
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def parse_foreclosure_pdf(pdf_bytes: bytes, doc_id: str) -> dict:
    """Extract key fields from a foreclosure posting PDF.

    Handles two main formats:
    1. Format A: Labeled fields (Grantor(s):, Legal Description:, etc.)
    2. Format B: "NOTICE OF FORECLOSURE SALE AND APPOINTMENT OF SUBSTITUTE TRUSTEE"
       with address at top, "Property:" section, "Obligation Secured:" section
    """
    result = {
        "grantor": "",
        "co_borrower": "",
        "property_address": "",
        "property_city": "",
        "property_state": "TX",
        "property_zip": "",
        "legal_description": "",
        "amount": "",
        "sale_date": "",
        "mortgagee": "",
    }

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            full_text = ""
            has_scanned_pages = False
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text.strip():
                    full_text += text + "\n"
                else:
                    has_scanned_pages = True
            # Trigger OCR if the PDF is scanned images — pdfplumber sometimes
            # extracts 1-3 garbage chars from scanned pages, so check length < 20
            if has_scanned_pages and len(full_text.strip()) < 20:
                # PDF is entirely scanned images â run OCR to extract text
                if HAS_OCR:
                    log.info(f"  {doc_id} is a scanned PDF â running OCR at {OCR_DPI} DPI...")
                    try:
                        images = convert_from_bytes(pdf_bytes, dpi=OCR_DPI)
                        # Tesseract config: PSM 6 = assume uniform block of text (best for legal docs)
                        tess_config = "--psm 6"
                        for page_num, img in enumerate(images, 1):
                            # Pre-process image for better OCR accuracy
                            processed = preprocess_for_ocr(img)
                            page_text = pytesseract.image_to_string(processed, config=tess_config)
                            # If preprocessing produced poor results, retry with original
                            if len(page_text.strip()) < 50:
                                page_text_raw = pytesseract.image_to_string(img, config=tess_config)
                                if len(page_text_raw.strip()) > len(page_text.strip()):
                                    page_text = page_text_raw
                                    log.info(f"    Page {page_num}: fallback to raw image ({len(page_text)} chars)")
                            if page_text.strip():
                                full_text += page_text + "\n"
                                log.info(f"    Page {page_num}: {len(page_text)} chars extracted via OCR")
                            else:
                                log.info(f"    Page {page_num}: no text found via OCR")
                        log.info(f"  OCR complete: {len(full_text)} total chars")
                    except Exception as e:
                        log.warning(f"  OCR failed for {doc_id}: {e}")
                        result["needs_ocr"] = True
                        return result
                else:
                    log.info(f"  {doc_id} is a scanned PDF â OCR not available, flagging for VA lookup")
                    result["needs_ocr"] = True
                    return result
            elif has_scanned_pages:
                log.info(f"  {doc_id} has some scanned pages but got text from others")
    except Exception as e:
        log.warning(f"Failed to extract text from {doc_id}: {e}")
        return result

    if not full_text.strip():
        log.warning(f"Empty PDF text for {doc_id}")
        return result

    # Debug: log first 300 chars of extracted text to help diagnose parsing issues
    preview = full_text[:300].replace('\n', ' | ')
    log.info(f"  Text preview: {preview}")

    # ââ OCR Text Cleanup ââââââââââââââââââââââââââââââââââââââââââ
    # Normalize common OCR artifacts before regex parsing
    # Strip common OCR noise characters that aren't meaningful in legal docs
    full_text = re.sub(r'[<>~\*\\\{\}]', ' ', full_text)
    # Collapse multiple spaces/tabs into single space
    full_text = re.sub(r'[ \t]{2,}', ' ', full_text)
    # Remove isolated single junk chars between words (OCR noise like ". : o ,")
    full_text = re.sub(r'(?<=[A-Z]) [^A-Za-z0-9\s]{1,2} (?=[A-Z])', ' ', full_text)
    # Remove isolated single-char noise at start of lines (common OCR artifact)
    full_text = re.sub(r'(?m)^[^A-Za-z0-9\s]{1,3}\s+', '', full_text)
    # Fix common OCR letter substitutions in key words
    full_text = re.sub(r'\bGrant0r\b', 'Grantor', full_text)
    full_text = re.sub(r'\b[Cc]0unty\b', 'County', full_text)
    full_text = re.sub(r'\bpr0perty\b', 'property', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bSP[Q0O](?:USE|OSE)\b', 'SPOUSE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bSUBSTITUTE\s+TR[U0]STEE\b', 'SUBSTITUTE TRUSTEE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bN[O0]TICE\b', 'NOTICE', full_text, flags=re.IGNORECASE)
    full_text = re.sub(r'\bM[O0]RTGAG[O0]R\b', 'MORTGAGOR', full_text, flags=re.IGNORECASE)
    # Fix "| " â "l " (pipe misread as lowercase L) in common contexts
    full_text = re.sub(r'\|(?=\s+of\s)', 'l', full_text)
    # Normalize curly/smart quotes to straight
    full_text = full_text.replace('\u2018', "'").replace('\u2019', "'")
    full_text = full_text.replace('\u201c', '"').replace('\u201d', '"')
    # Strip null bytes and control characters (except newlines)
    full_text = re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f]', '', full_text)
    # Collapse runs of dots/periods (OCR artifact from scanned table lines)
    full_text = re.sub(r'\.{2,}', ' ', full_text)
    # Remove stray single dots surrounded by spaces (not decimal points)
    full_text = re.sub(r'(?<!\d)\.\s+(?!\d)', ' ', full_text)
    # Final whitespace normalization after all cleanup
    full_text = re.sub(r'[ \t]{2,}', ' ', full_text)

    lines = full_text.split("\n")

    # ââ Try Format A: labeled fields ââ
    # NOTE: Do NOT return early â fall through to Format B for address extraction
    # and more robust grantor patterns, since Format A doesn't extract addresses.
    if "Grantor(s):" in full_text or "Grantor (s):" in full_text or re.search(r'Grantor\s*\(?s?\)?\s*/\s*Mortgagor', full_text, re.IGNORECASE):
        for i, line in enumerate(lines):
            line_s = line.strip()

            # Grantor â check same line first, then next line
            m = re.match(r'Grantor\s*\(s?\)\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                # Reject if it's just another label (e.g., "Grantor(s): Current Mortgagee:")
                if val and not re.match(r'^(Current|Legal|Amount|Date|Property|Instrument)\b', val, re.IGNORECASE):
                    result["grantor"] = val
            elif re.match(r'Grantor\s*\(s?\)\s*:?\s*$', line_s, re.IGNORECASE):
                # Label on its own line â grab the NEXT non-empty line as the grantor name
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 2:
                        # Make sure it's a name, not another label
                        if not re.match(r'^(Current|Legal|Amount|Date|Property|Instrument|Account)\b', next_line, re.IGNORECASE):
                            result["grantor"] = next_line
                        break

            # Grantor(s)/Mortgagor(s): NAME â table header format
            if not result["grantor"]:
                m = re.match(r'Grantor\s*\(?s?\)?\s*/\s*Mortgagor\s*\(?s?\)?\s*:?\s*(.+)', line_s, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val and len(val) > 2:
                        result["grantor"] = val

            # Legal Description â same line or next line
            m = re.match(r'Legal\s+Description\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["legal_description"] = m.group(1).strip()
            elif re.match(r'Legal\s+Description\s*:?\s*$', line_s, re.IGNORECASE):
                # Label alone â grab following lines as legal description
                legal_parts = []
                for j in range(i + 1, min(i + 5, len(lines))):
                    next_line = lines[j].strip()
                    if not next_line:
                        break
                    if re.match(r'^(Amount|Date|Current|Instrument|Account|Grantor)\b', next_line, re.IGNORECASE):
                        break
                    legal_parts.append(next_line)
                if legal_parts:
                    result["legal_description"] = " ".join(legal_parts)

            # Property Address â Format A sometimes has this
            if not result["property_address"]:
                m = re.match(r'Property\s+Address\s*:\s*(.+)', line_s, re.IGNORECASE)
                if m:
                    result["property_address"] = m.group(1).strip()

            # Amount
            m = re.match(r'Amount\s*:\s*\$?([\d,\.]+)', line_s, re.IGNORECASE)
            if m:
                result["amount"] = "$" + m.group(1)

            # Date of Sale â strip time/hours portion
            m = re.match(r'Date\s+of\s+Sale\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                raw_date = m.group(1).strip()
                # Clean: "May 5, 2026 between the hours..." â "May 5, 2026"
                cleaned = re.sub(r'\s+between\s+.*', '', raw_date, flags=re.IGNORECASE)
                result["sale_date"] = cleaned

            # Current Mortgagee
            m = re.match(r'Current\s+Mortgagee\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["mortgagee"] = m.group(1).strip()

        log.info(f"  Format A extracted: grantor={result['grantor'][:30] if result['grantor'] else '(empty)'}, legal={'yes' if result['legal_description'] else 'no'}")
        # Fall through to Format B patterns for address extraction and grantor enhancement

    # ââ Format B: Structured paragraphs ââ
    # Street suffixes (including full words for OCR accuracy)
    _SFX = (r"(?:ST|RD|DR|LN|CT|AVE|BLVD|WAY|PL|CIR|TRL|PKWY|CV|"
            r"STREET|ROAD|DRIVE|LANE|COURT|AVENUE|BOULEVARD|PLACE|"
            r"CIRCLE|TRAIL|PARKWAY|COVE)")

    # Known foreclosure auction venues and courthouse addresses â NOT property addresses
    # These appear in every document and must be excluded
    _VENUE_PATTERNS = [
        r'9401\s+KNIGHT',        # Bayou City Event Center (auction venue)
        r'401\s+KNI[GC]HT',     # OCR-garbled version of 9401 Knight
        r'201\s+CAROLINE',       # Harris County Civil Courthouse
        r'1001\s+PRESTON',       # Harris County Admin Building
        r'SENTARA\s+WAY',        # LoanCare LLC (common servicer address)
        r'CONGRESS\s+(?:AVE|ST)', # Government buildings
    ]

    def _is_venue_address(addr_str):
        """Check if an address matches a known auction venue or courthouse."""
        for pat in _VENUE_PATTERNS:
            if re.search(pat, addr_str, re.IGNORECASE):
                return True
        return False

    # Check for address at top of document (first 10 lines)
    if not result["property_address"]:
        for line in lines[:10]:
            line_s = line.strip()
            if not line_s or not line_s[0].isdigit():
                continue
            # Pattern: "15647 COUNTESSWELLS DRIVE, HUMBLE, TX 77346"
            m = re.match(r'^(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*[,]?\s*'
                         r'([A-Z]+)\s*[,]?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
                         line_s, re.IGNORECASE)
            if m:
                result["property_address"] = m.group(1).strip()
                result["property_city"] = m.group(2).strip()
                result["property_state"] = "TX"
                result["property_zip"] = m.group(3).strip()
                break

    # Check for "Commonly known as:" line (support multi-word cities like LA PORTE)
    if not result["property_address"]:
        m = re.search(r'[Cc]ommonly\s+known\s+as\s*:?\s*(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*[,]?\s*'
                      r'([A-Z][A-Z\s]*?)\s*[,]?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)', full_text, re.IGNORECASE)
        if m:
            addr = m.group(1).strip()
            city = m.group(2).strip()
            if len(city) >= 3 and all(c.isalpha() or c == ' ' for c in city):
                result["property_address"] = addr
                result["property_city"] = city
                result["property_zip"] = m.group(3).strip()
                if not _is_venue_address(addr):
                    log.info(f"  Address (commonly known as): {addr}, {city}")

    # Check for address after clerk filing stamp (common in OCR'd foreclosure docs)
    # Pattern: "FRCL-2026-XXXX FILED date\n ADDRESS LINE \n acct# \n CITY, TX ZIP"
    if not result["property_address"]:
        m = re.search(
            r'FRCL-\d{4}-\d+\s+FILED\s+[\d/]+\s+[\d:]+\s*[AP]?M?\s*'
            r'(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*'
            r'(?:\d{10,16}\s*)?'  # optional account number
            r'([A-Z][A-Z\s]*?)\s*,?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
            full_text, re.IGNORECASE
        )
        if m:
            result["property_address"] = m.group(1).strip()
            result["property_city"] = m.group(2).strip()
            result["property_state"] = "TX"
            result["property_zip"] = m.group(3).strip()
            log.info(f"  Address from filing stamp: {result['property_address']}, {result['property_city']}")

    # Broader fallback: find "NUMBER STREET ... CITY, TX ZIP" anywhere in text
    # Tolerant of OCR junk characters between street and city
    if not result["property_address"]:
        m = re.search(
            r'(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r')'
            r'[\s\.\:,o_]*'  # allow OCR junk chars between street and acct#
            r'(?:\d{10,16}[\s\.\:,]*)?'  # optional account number with junk
            r'([A-Z][A-Z\s]+?)\s*,?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)',
            full_text, re.IGNORECASE
        )
        if m:
            addr = m.group(1).strip()
            city = m.group(2).strip()
            zipcode = m.group(3).strip()
            # Sanity check: city should be real words, not OCR junk
            # Allow spaces for cities like LA PORTE, LA MARQUE, etc.
            if len(city) >= 3 and all(c.isalpha() or c == ' ' for c in city):
                result["property_address"] = addr
                result["property_city"] = city
                result["property_state"] = "TX"
                result["property_zip"] = zipcode
                log.info(f"  Address (broad match): {addr}, {city}, TX {zipcode}")

    # ââ Validate extracted address â reject auction venues ââââââ
    if result["property_address"] and _is_venue_address(result["property_address"]):
        log.info(f"  Rejected venue address: {result['property_address']} â not a property")
        result["property_address"] = ""
        result["property_city"] = ""
        result["property_state"] = "TX"
        result["property_zip"] = ""

    # Extract Property / Legal Description section
    # Try multiple patterns since OCR can garble the text
    legal_found = False

    # First, check if the document references "Exhibit A" for the legal description
    # Many foreclosure postings say "described in the attached Exhibit A"
    exhibit_ref = re.search(
        r'(?:described\s+in\s+(?:the\s+)?(?:attached\s+)?Exhibit\s+["\']?A["\']?|'
        r'see\s+(?:attached\s+)?Exhibit\s+["\']?A["\']?|'
        r'set\s+forth\s+(?:in|on)\s+(?:attached\s+)?Exhibit\s+["\']?A["\']?)',
        full_text, re.IGNORECASE
    )
    if exhibit_ref:
        # Find the "Exhibit A" header later in the document and grab text after it
        m = re.search(
            r'(?:^|\n)\s*(?:EXHIBIT\s+["\']?A["\']?|Exhibit\s+["\']?A["\']?)\s*[\n\r]+'
            r'(.*?)(?:\n\s*(?:EXHIBIT\s+["\']?B|Exhibit\s+["\']?B)|$)',
            full_text[exhibit_ref.end():], re.DOTALL | re.IGNORECASE
        )
        if m:
            legal_block = m.group(1).strip()
            legal_block = re.sub(r'\s+', ' ', legal_block).strip()
            # Remove boilerplate that sometimes appears at the start of Exhibit A
            legal_block = re.sub(r'^(?:Legal\s+Description\s*:?\s*|Property\s+Description\s*:?\s*)', '', legal_block, flags=re.IGNORECASE).strip()
            if len(legal_block) > 10:
                result["legal_description"] = legal_block
                legal_found = True
                log.info(f"  Legal description from Exhibit A: {legal_block[:80]}...")

    if not legal_found:
        for pat in [
            r'(?:described\s+as\s+follows|Property\s+to\s+be\s+sold\s+is\s+described)\s*:?\s*\n?(.*?)(?:Security\s+Instrument|Deed\s+of\s+Trust\s*:|The\s+(?:Security|Deed))',
            r'Property\s*:\s*.*?(?:described\s+as\s+follows\s*:?)?\s*\n(.*?)(?:Security\s+Instrument|Deed\s+of\s+Trust)',
            r'(LOT\s+[A-Z\-\s]+\(\d+\).*?(?:HARRIS\s+COUNTY|COUNTY\s*,\s*TEXAS))',
        ]:
            m = re.search(pat, full_text, re.DOTALL | re.IGNORECASE)
            if m:
                legal_block = m.group(1).strip()
                legal_block = re.sub(r'\s+', ' ', legal_block).strip()
                if len(legal_block) > 10:
                    result["legal_description"] = legal_block
                    legal_found = True
                    break

    # ââ Clean up legal description â truncate at boundary phrases ââ
    # OCR often captures text beyond the legal description into the next section
    if result["legal_description"]:
        ld = result["legal_description"]
        # Cut at common boundary phrases that signal end of legal description
        for boundary in [
            r'(?:[,:\s]+\d*\s*Instr[ua]n?[mt]en?t\s+to[\s-]+be)',  # "Instruntent to be", "Instrament to-be"
            r'(?:The\s+[Ii]nstr[ua]n?[mt]en?t\s+to[\s-]+be)',      # "The instrument to be"
            r'(?:The\s+mstr[au]ment\s+to[\s-]+be)',                 # "The mstrament to be"
            r'(?:\d+\s+[Ii]nstr[ua]n?[mt]en?t\s+to)',              # "2 Instrament to"
            r'(?:to[\s-]+be\s+[Ff]oreclosed)',                      # "to be Foreclosed" / "to-be Foreclosed"
            r'(?:be\s+[Ff]oreclosed\s+The)',                        # "be Foreclosed The"
            r'(?:Security\s+Instrument)',
            r'(?:Deed\s+of\s+Trust)',
        ]:
            m = re.search(boundary, ld, re.IGNORECASE)
            if m:
                ld = ld[:m.start()].strip()
                break
        # Final cleanup â strip trailing junk punctuation and stray numbers
        ld = re.sub(r'[,:;\.\s\d]+$', '', ld)
        # Truncate at "HARRIS COUNTY TEXAS" if still too long
        if len(ld) > 200:
            # Try to cut at last "HARRIS COUNTY" or "COUNTY TEXAS"
            m = re.search(r'(?:HARRIS\s+COUNTY|COUNTY\s*,?\s*TEXAS)', ld, re.IGNORECASE)
            if m:
                ld = ld[:m.end()].strip()
        result["legal_description"] = ld

    # ââ Reject legal description if it's clearly boilerplate, not a real legal desc ââ
    if result["legal_description"]:
        ld_lower = result["legal_description"].lower()
        boilerplate_phrases = [
            "property code", "mortgagee", "servicing agreement", "mortgage servicer",
            "foreclosure of the lien", "virtue of a", "authorized to collect",
            "pursuant to the", "loancare", "nationstar", "ocwen",
        ]
        boilerplate_count = sum(1 for phrase in boilerplate_phrases if phrase in ld_lower)
        if boilerplate_count >= 2:
            log.info(f"  Rejected boilerplate legal desc ({boilerplate_count} boilerplate phrases found)")
            result["legal_description"] = ""

    # Extract grantor â try multiple patterns (skip if Format A already found one)
    # Pattern 1: "executed by GRANTOR NAME"
    if not result["grantor"]:
        m = re.search(r'(?:executed\s+by|delivered\s+by)\s+([A-Z][A-Za-z\s,\.&]+?)(?:\s*,?\s*as\s+[Gg]rantors?|\s+(?:and\s+wife|delivered|executed|in\s+favor|securing|to\s+))',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 2: "WHEREAS, on DATE, GRANTOR NAME, ... as Grantor(s)"
    if not result["grantor"]:
        m = re.search(r'WHEREAS\s*,\s*on\s+\d+/\d+/\d+\s*,\s*([A-Za-z\s,\.]+?)\s*,\s*(?:and\s+wife|as\s+[Gg]rantors?)',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 3: "with NAME, as grantor(s)" â common in foreclosure postings
    if not result["grantor"]:
        m = re.search(r'with\s+([A-Z][A-Za-z\s,\.&]+?)\s*,?\s*as\s+[Gg]rantors?',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 4: "Grantor: NAME" or "Grantor(s)/Mortgagor(s): NAME" â labeled field at top of document
    if not result["grantor"]:
        m = re.search(r'[Gg]rantors?(?:\(s\))?\s*(?:/\s*[Mm]ortgagors?(?:\(s\))?\s*)?\s*:\s*\n?\s*([A-Z][A-Za-z\s,\.&]+?)(?:\s*\n|\s+[Cc]urrent|\s+[Gg]rantee|\s+[Oo]riginal|\s+Instrument|\s+Property|\s+Deed|\s+Date|\s+Account)',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 5: "NAME secures the repay" (OCR-garbled version)
    if not result["grantor"]:
        m = re.search(r'([A-Z][A-Za-z\s]+?)\s+secures\s+the\s+repa', full_text, re.IGNORECASE)
        if m:
            # Take just the last 1-3 words before "secures" as the name
            name = m.group(1).strip()
            # Remove leading noise from OCR
            parts = name.split()
            if len(parts) > 4:
                name = " ".join(parts[-3:])
            result["grantor"] = name.rstrip(",. ")

    # Pattern 6: Table-format OCR — "Grantor(s)/Mortgagor(s)" header row
    if not result["grantor"]:
        m = re.search(
            r'[Gg]rantor\s*\(?s?\)?\s*(?:/\s*[Mm]ortgagor\s*\(?s?\)?\s*)?'
            r'[\s:,.\-|]*'
            r'\d{1,2}/\d{1,2}/\d{2,4}'
            r'[\s:,.\-|]*'
            r'([A-Z][A-Z\s,\.&\'\-]+?)'
            r'(?:\s+(?:SINGLE|MARRIED|AN?\s+(?:UN)?MARRIED|SPOUSE|A\s+SINGLE)\b|\s+[Cc]urrent|\s+[Ii]nstrument|\s+[Ll]egal|\s+[Aa]ccount|\s+[Dd]ate|\s*$)',
            full_text
        )
        if m:
            name = m.group(1).strip().rstrip(",. ;:")
            if len(name.split()) >= 2 and sum(c.isalpha() for c in name) > len(name) * 0.6:
                result["grantor"] = name
                log.info(f"  Pattern 6 (table-format OCR): {name[:40]}")

    # Pattern 7: Standalone date + ALL_CAPS_NAME (OCR'd scanned table)
    if not result["grantor"]:
        for m in re.finditer(
            r'(\d{1,2}/\d{1,2}/\d{2,4})\s+'
            r'([A-Z][A-Z\s\'\-]{3,50}?)'
            r'(?:\s+(?:SINGLE|MARRIED|AN?\s+(?:UN)?MARRIED|AND\s+SPOUSE|A\s+SINGLE|A\s+MARRIED)\b)',
            full_text
        ):
            name = m.group(2).strip().rstrip(",. ;:")
            if len(name.split()) >= 2 and not re.match(r'^(?:NOTICE|DEED|HARRIS|STATE|COUNTY)\b', name):
                result["grantor"] = name
                log.info(f"  Pattern 7 (date+name): {name[:40]}")
                break

    # Pattern 8: "executed by NAME" with OCR garbled "executed"
    if not result["grantor"]:
        m = re.search(
            r'(?:ex[ec]{1,2}uted|deliver[eo]d)\s+by\s+'
            r'([A-Z][A-Z\s,\.&\'\-]+?)'
            r'(?:\s*,?\s*(?:as\s+[Gg]rantors?|and\s+wife|delivered|executed|in\s+favor|securing|to\s+|Deed|a\s+[Dd]eed))',
            full_text
        )
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")
            log.info(f"  Pattern 8 (executed by, OCR): {result['grantor'][:40]}")

    # Clean up grantor â remove OCR junk that gets appended after the name
    if result["grantor"]:
        g = result["grantor"]
        # Remove leading "by " prefix (artifact from "executed by" capture)
        g = re.sub(r'^by\s+', '', g, flags=re.IGNORECASE)
        # Remove leading OCR noise chars
        g = re.sub(r'^[^A-Za-z]+', '', g)
        # Remove newlines and everything after them (OCR artifact)
        if "\n" in g:
            parts = g.split("\n")
            clean_parts = [parts[0]]
            for p in parts[1:]:
                p_stripped = p.strip()
                # Keep ONLY if it looks like a continuation name (e.g., "NAHTALY AULD" after "AND YUDITH")
                # or a suffix like JR, SR, III
                if re.match(r'^(?:[A-Z][A-Z\s\-\']+(?:,\s*(?:HUSBAND|WIFE|AN?\s+(?:UN)?MARRIED\s+(?:WO)?MAN))?)\s*$', p_stripped, re.IGNORECASE) and len(p_stripped) < 60:
                    clean_parts.append(p_stripped)
                elif re.match(r'^(?:JR|SR|III?|IV)\b', p_stripped, re.IGNORECASE):
                    clean_parts.append(p_stripped)
                else:
                    break  # Stop at first non-name line
            g = " ".join(clean_parts)
        # Remove common OCR junk phrases that get captured (tolerant of garbled first letter)
        g = re.sub(r'\s+(?:p?ayment|indebtedness|securing|of\s+the|in\s+favor|the\s+repay|but\s+not\s+limited|pursuant|default|note\s+dated|deed\s+of).*', '', g, flags=re.IGNORECASE)
        # Strip trailing "AND" when no co-borrower name follows
        g = re.sub(r'\s+AND\s*$', '', g, flags=re.IGNORECASE)
        # Strip marital status descriptors and spouse prefix
        g = re.sub(r',?\s*AND\s+SPOUSE\s+', ' AND ', g, flags=re.IGNORECASE)
        g = re.sub(r',?\s*(?:AN?\s+)?(?:UN)?MARRIED\s+(?:WO)?MAN\b', '', g, flags=re.IGNORECASE)
        g = re.sub(r',?\s*(?:HUSBAND(?:\s+AND\s+WIFE)?|WIFE(?:\s+AND\s+HUSBAND)?)\s*$', '', g, flags=re.IGNORECASE)
        # Remove trailing junk punctuation
        g = g.strip().rstrip(",. ;:")
        result["grantor"] = g

    # Split borrower / co-borrower on " AND "
    result["co_borrower"] = ""
    if result["grantor"] and " AND " in result["grantor"].upper():
        name_parts = re.split(r'\s+AND\s+', result["grantor"], maxsplit=1, flags=re.IGNORECASE)
        if len(name_parts) == 2:
            borrower = name_parts[0].strip().rstrip(",. ;:")
            co = name_parts[1].strip().rstrip(",. ;:")
            # Only split if both parts look like names (2+ chars, mostly alpha)
            alpha_chars_b = sum(c.isalpha() or c == ' ' for c in borrower)
            alpha_chars_c = sum(c.isalpha() or c == ' ' for c in co)
            if alpha_chars_b > 3 and alpha_chars_c > 3:
                result["grantor"] = borrower
                # Clean marital status from co-borrower too
                co = re.sub(r',?\s*(?:AN?\s+)?(?:UN)?MARRIED\s+(?:WO)?MAN\b', '', co, flags=re.IGNORECASE)
                co = re.sub(r',?\s*(?:HUSBAND(?:\s+AND\s+WIFE)?|WIFE(?:\s+AND\s+HUSBAND)?)\s*$', '', co, flags=re.IGNORECASE)
                result["co_borrower"] = co.strip().rstrip(",. ;:")

    # Extract amount â multiple patterns
    m = re.search(r'(?:amount\s+of|sum\s+of)\s+\$?([\d,]+(?:\.\d{2})?)', full_text, re.IGNORECASE)
    if m:
        result["amount"] = "$" + m.group(1)
    if not result["amount"]:
        m = re.search(r'Amount\s*:\s*\$?([\d,]+(?:\.\d{2})?)', full_text, re.IGNORECASE)
        if m:
            result["amount"] = "$" + m.group(1)

    # Extract sale date from "NOTICE IS HEREBY GIVEN, that on DATE"
    m = re.search(r'(?:NOTICE\s+IS\s+HEREBY\s+GIVEN|Date\s+of\s+Sale)\s*[,:]?\s*(?:that\s+on\s+)?(\d{1,2}/\d{1,2}/\d{4})',
                  full_text, re.IGNORECASE)
    if m:
        result["sale_date"] = m.group(1)

    # Also check for spelled-out date: "May 5, 2026"
    if not result["sale_date"]:
        m = re.search(r'(?:Sale\s+Information|NOTICE\s+IS\s+HEREBY\s+GIVEN).*?'
                      r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
                      r'(\d{1,2})\s*,\s*(\d{4})',
                      full_text, re.IGNORECASE | re.DOTALL)
        if m:
            result["sale_date"] = f"{m.group(1)} {m.group(2)}, {m.group(3)}"

    # Extract mortgagee / servicer â multiple patterns
    for mort_pat in [
        r'[Cc]urrent\s+[Mm]ortgagee\s*:\s*([A-Z][A-Za-z\s,\.&]+?)(?:\s*\n|\s*$)',
        r'mortgage\s+servicer\s+(?:is|for)\s+([A-Z][A-Za-z\s,\.&]+?)(?:\s*,\s*whose|\s*\.)',
        r'(\$[\d,]+(?:\.\d{2})?)\.\s+([A-Z][A-Z\s,\.&]+?)(?:\s*,|\s+LLC|\s+is\s+)',
    ]:
        m = re.search(mort_pat, full_text, re.IGNORECASE | re.MULTILINE)
        if m:
            # For the last pattern, the mortgagee name is in group 2
            name = m.group(2) if m.lastindex >= 2 else m.group(1)
            name = name.strip().rstrip(",. ")
            # Skip garbage results
            if len(name) > 3 and not name.startswith("of "):
                result["mortgagee"] = name
                break

    log.info(f"  Parse result: grantor={result['grantor'][:40] if result['grantor'] else '(none)'} | "
             f"addr={result['property_address'][:40] if result['property_address'] else '(none)'} | "
             f"legal={'yes' if result['legal_description'] else 'no'}")
    return result


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  FRCL List Scraper
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def get_target_months() -> list:
    """Return the next 3 months as (year, month_number) tuples."""
    now = datetime.now()
    months = []
    for offset in range(1, 4):
        target = now.replace(day=1) + timedelta(days=32 * offset)
        target = target.replace(day=1)
        months.append((target.year, target.month))
    return months


def scrape_frcl_list(session: requests.Session, year: int, month: int) -> list:
    """Scrape the FRCL list page for a given month.

    Returns list of dicts: {"doc_id", "sale_date", "file_date", "pages"}
    """
    month_name = MONTH_NAMES[month]
    log.info(f"Scraping foreclosure list for {month_name} {year}...")

    # First, load the search page to get ViewState
    log.info(f"  Loading FRCL search page...")
    resp = session.get(FRCL_SEARCH_URL, timeout=(10, 30))
    resp.raise_for_status()
    log.info(f"  Search page loaded: {len(resp.text):,} bytes")
    soup = BeautifulSoup(resp.text, "html.parser")

    viewstate = soup.find("input", {"name": "__VIEWSTATE"})
    viewstate_val = viewstate["value"] if viewstate else ""
    event_val_tag = soup.find("input", {"name": "__EVENTVALIDATION"})
    event_val = event_val_tag["value"] if event_val_tag else ""
    viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
    viewstate_gen_val = viewstate_gen["value"] if viewstate_gen else ""

    # Submit the search form
    post_data = {
        "__VIEWSTATE": viewstate_val,
        "__VIEWSTATEGENERATOR": viewstate_gen_val,
        "__EVENTVALIDATION": event_val,
        "__VIEWSTATEENCRYPTED": "",
        "ctl00$ContentPlaceHolder1$rbtlDate": "SaleDate",
        "ctl00$ContentPlaceHolder1$ddlYear": str(year),
        "ctl00$ContentPlaceHolder1$ddlMonth": str(month),
        "ctl00$ContentPlaceHolder1$btnSearch": "SEARCH",
        "ctl00$ContentPlaceHolder1$txtFileNo": "",
    }

    log.info(f"  Submitting search form for {month_name} {year}...")
    resp = session.post(FRCL_SEARCH_URL, data=post_data, timeout=(15, 120))
    resp.raise_for_status()
    log.info(f"  Response received: {len(resp.text):,} bytes")
    soup = BeautifulSoup(resp.text, "html.parser")

    # Parse the results table
    records = []
    table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})
    if not table:
        # Check if results text shows count
        count_text = soup.find(string=re.compile(r'\d+\s+Row\(s\)\s+Found', re.IGNORECASE))
        if count_text:
            log.info(f"  Found results text but no table yet")
        else:
            log.info(f"  No results found for {month_name} {year}")
            return records

    # Parse all pages â with safety limits to prevent infinite loops
    MAX_PAGES = 100  # Hard cap: 100 pages Ã 20 results = 2,000 max
    page_num = 1
    seen_doc_ids = set()

    while True:
        prev_count = len(records)
        rows = table.find_all("tr") if table else []
        for row in rows:
            # Skip pager rows (class pagination-ys) and header rows (class bg-color-transblue)
            row_class = row.get("class", [])
            if isinstance(row_class, list):
                row_class = " ".join(row_class)
            if "pagination" in row_class or "bg-color" in row_class:
                continue

            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            # Cell layout: [0]=empty/checkbox, [1]=Doc ID (link), [2]=Sale Date, [3]=File Date, [4]=Pgs
            link = cells[1].find("a")
            if not link:
                continue

            doc_id = link.get_text(strip=True)
            if not doc_id.startswith("FRCL"):
                continue

            # Skip duplicates (prevents infinite loop re-scraping same page)
            if doc_id in seen_doc_ids:
                continue
            seen_doc_ids.add(doc_id)

            sale_date = cells[2].get_text(strip=True)
            file_date = cells[3].get_text(strip=True)
            pages = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            # Capture the actual PDF viewer URL (uses encrypted token, not plain doc ID)
            pdf_href = link.get("href", "")
            if pdf_href and not pdf_href.startswith("http"):
                pdf_href = "https://www.cclerk.hctx.net/applications/websearch/" + pdf_href

            records.append({
                "doc_id": doc_id,
                "sale_date": sale_date,
                "file_date": file_date,
                "pages": pages,
                "pdf_url": pdf_href,
            })

        new_on_page = len(records) - prev_count
        log.info(f"  Page {page_num}: {new_on_page} new records (total: {len(records)})")

        # If this page added zero new records, we've seen them all â stop
        if new_on_page == 0 and page_num > 1:
            log.info(f"  No new records on page {page_num} â pagination complete")
            break

        # Hard cap to prevent runaway loops
        if page_num >= MAX_PAGES:
            log.warning(f"  Hit max page limit ({MAX_PAGES}) â stopping pagination")
            break

        # Check for next page â pager row has class "pagination-ys"
        pager = soup.find("tr", class_="pagination-ys")
        if not pager:
            break

        # Find the next numbered page link
        page_num += 1
        next_link = None
        for a in pager.find_all("a"):
            try:
                page_text = a.get_text(strip=True)
                if page_text == str(page_num):
                    next_link = a
                    break
            except:
                pass

        # If exact page number not found, look for "..." but ONLY use
        # the LAST "..." link (which goes forward, not backward)
        if not next_link:
            dots_links = [a for a in pager.find_all("a") if a.get_text(strip=True) == "..."]
            if dots_links:
                next_link = dots_links[-1]  # last "..." = forward
            if not next_link:
                log.info(f"  No more page links found â pagination complete")
                break

        # Get the postback for the next page
        href = next_link.get("href", "")
        m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
        if not m:
            break

        event_target = m.group(1)
        event_arg = m.group(2)

        # Re-extract viewstate from current page
        viewstate_tag = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate_val = viewstate_tag["value"] if viewstate_tag else ""
        event_val_tag = soup.find("input", {"name": "__EVENTVALIDATION"})
        event_val = event_val_tag["value"] if event_val_tag else ""

        post_data = {
            "__VIEWSTATE": viewstate_val,
            "__VIEWSTATEGENERATOR": viewstate_gen_val,
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": event_val,
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": event_arg,
        }

        time.sleep(0.5)
        log.info(f"  Loading page {page_num}...")
        resp = session.post(FRCL_SEARCH_URL, data=post_data, timeout=(15, 60))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})

        if not table:
            break

    log.info(f"  Found {len(records)} foreclosure postings for {month_name} {year}")
    return records


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  Login to County Clerk (required to download PDF documents)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def login_to_cclerk(session: requests.Session) -> bool:
    """Log in to the Harris County Clerk website.

    The clerk site requires authentication to view/download PDF documents.
    Credentials are read from environment variables CCLERK_USERNAME and
    CCLERK_PASSWORD (set as GitHub Actions secrets).

    Returns True on success, False on failure.
    """
    username = os.environ.get("CCLERK_USERNAME", "")
    password = os.environ.get("CCLERK_PASSWORD", "")

    if not username or not password:
        log.warning("CCLERK_USERNAME / CCLERK_PASSWORD not set â PDF downloads will fail")
        return False

    try:
        # Step 1: GET the login page to grab ASP.NET ViewState tokens
        resp = session.get(CCLERK_LOGIN_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract hidden form fields
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate = viewstate["value"] if viewstate else ""
        generator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        generator = generator["value"] if generator else ""
        encrypted = soup.find("input", {"name": "__VIEWSTATEENCRYPTED"})
        encrypted = encrypted["value"] if encrypted else ""
        validation = soup.find("input", {"name": "__EVENTVALIDATION"})
        validation = validation["value"] if validation else ""
        prevpage = soup.find("input", {"name": "__PREVIOUSPAGE"})
        prevpage = prevpage["value"] if prevpage else ""

        # Step 2: POST the login form
        login_data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": generator,
            "__VIEWSTATEENCRYPTED": encrypted,
            "__PREVIOUSPAGE": prevpage,
            "__EVENTVALIDATION": validation,
            "ctl00$ContentPlaceHolder1$Login1$UserName": username,
            "ctl00$ContentPlaceHolder1$Login1$Password": password,
            "ctl00$ContentPlaceHolder1$Login1$LoginButton": "LOG IN",
        }

        resp = session.post(CCLERK_LOGIN_URL, data=login_data, timeout=30,
                           allow_redirects=True)
        resp.raise_for_status()

        # Check for successful login: page should contain LOGOUT or WELCOME
        if "LOGOUT" in resp.text.upper() or "WELCOME" in resp.text.upper():
            log.info("Successfully logged in to Harris County Clerk")
            return True
        else:
            log.warning("Login POST succeeded but could not confirm authentication")
            return False

    except Exception as e:
        log.warning(f"Login failed: {e}")
        return False


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  PDF Downloader
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def download_frcl_pdf(session: requests.Session, doc_id: str,
                      pdf_url: str = "") -> Optional[bytes]:
    """Download a foreclosure PDF using its encrypted viewer URL.

    The county clerk uses encrypted token URLs (ViewECdocs.aspx?ID=<token>)
    that are only available from the search results page.  Plain doc-ID URLs
    (ViewECdocs.aspx?id=FRCL-2026-612) return HTML, not a PDF.
    """
    url = pdf_url if pdf_url else f"{FRCL_DOC_URL}?id={doc_id}"
    log.info(f"  Downloading PDF: {url[:120]}...")

    try:
        resp = session.get(url, timeout=60, allow_redirects=True)
        log.info(f"  Response: status={resp.status_code}, type={resp.headers.get('Content-Type','')}, size={len(resp.content)} bytes, url={resp.url[:120]}")
        resp.raise_for_status()

        # Check if we got redirected to the login page
        if "Login.aspx" in resp.url:
            log.warning(f"  Redirected to login page â session expired!")
            return None

        # Check if we got a PDF
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" in content_type.lower() or resp.content[:4] == b"%PDF":
            log.info(f"  PDF downloaded OK: {len(resp.content):,} bytes")
            return resp.content
        else:
            log.warning(f"  Non-PDF response for {doc_id}: {content_type}")
            # Log first 200 chars of response to help debug
            log.warning(f"  Response preview: {resp.text[:200]}")
            return None
    except Exception as e:
        log.warning(f"  Failed to download {doc_id}: {e}")
        return None


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  Main
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def main():
    log.info("Harris County Foreclosure Scraper")
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"Date: {today}")

    # Create directories
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    # Load previously seen doc IDs
    seen_ids = set()
    existing_records = []
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f:
            seen_data = json.load(f)
            seen_ids = set(seen_data.get("seen_ids", []))
            log.info(f"Loaded {len(seen_ids)} previously seen foreclosure IDs")

    # Load existing foreclosure records
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing_data = json.load(f)
            existing_records = existing_data.get("records", [])
            log.info(f"Loaded {len(existing_records)} existing foreclosure records")

    # Generate batch ID for this run (B1, B2, B3, ...)
    existing_batch_nums = set()
    for rec in existing_records:
        bid = rec.get("batch_id", "")
        if bid.startswith("B"):
            try:
                existing_batch_nums.add(int(bid[1:]))
            except ValueError:
                pass
    next_batch_num = max(existing_batch_nums, default=0) + 1
    batch_id = f"B{next_batch_num}"
    now_ts = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    run_number2 = os.environ.get("GITHUB_RUN_NUMBER", "")
    run_tag2 = f"Run #{run_number2} / " if run_number2 else ""
    batch_label = f"{run_tag2}Batch {next_batch_num} â {now_ts}"
    log.info(f"This run: {batch_id} ({batch_label})")

    # Set up HTTP session with realistic browser headers
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx",
    })

    # Log in to county clerk (required to download PDFs)
    logged_in = login_to_cclerk(session)
    if not logged_in:
        log.warning("Proceeding without login â PDF downloads may fail")

    # Get target months (next 3 months)
    target_months = get_target_months()
    log.info(f"Target months: {[f'{MONTH_NAMES[m]} {y}' for y, m in target_months]}")

    # Scrape FRCL list for each month
    all_listings = []
    for year, month in target_months:
        listings = scrape_frcl_list(session, year, month)
        all_listings.extend(listings)

    log.info(f"Total foreclosure listings found: {len(all_listings)}")

    # Find doc IDs that need OCR reprocessing (previously failed to extract text)
    ocr_retry_ids = set()
    reparse_ids = set()
    for rec in existing_records:
        if rec.get("needs_ocr") and not rec.get("grantor"):
            ocr_retry_ids.add(rec["doc_id"])
        # Re-process records that have no grantor AND no address â parser likely failed
        elif not rec.get("grantor") and (not rec.get("prop_address") or rec.get("prop_address") == "Not found"):
            reparse_ids.add(rec["doc_id"])
    if ocr_retry_ids:
        log.info(f"Found {len(ocr_retry_ids)} records needing OCR reprocessing")
    if reparse_ids:
        log.info(f"Found {len(reparse_ids)} records with missing grantor+address â will re-parse")

    # Filter to new doc IDs PLUS OCR retries PLUS records needing re-parsing
    retry_ids = ocr_retry_ids | reparse_ids
    new_listings = [l for l in all_listings if l["doc_id"] not in seen_ids or l["doc_id"] in retry_ids]
    log.info(f"New listings to process: {len(new_listings)} (skipping {len(all_listings) - len(new_listings)} already seen)")

    if not new_listings:
        log.info("No new foreclosure listings to process")
        # Still save output to update the dashboard
        _save_output(existing_records, seen_ids, today, target_months)
        return

    # Apply batch limit to stay within GitHub Actions timeout
    if len(new_listings) > MAX_PDFS_PER_RUN:
        log.info(f"Batch limit: processing first {MAX_PDFS_PER_RUN} of {len(new_listings)} new listings")
        new_listings = new_listings[:MAX_PDFS_PER_RUN]

    # Load HCAD bulk data for address enrichment
    # Use a SEPARATE session so we don't clobber the clerk login cookies
    hcad = HCADMatcher()
    hcad_session = requests.Session()
    hcad_session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    if len(new_listings) >= 100:
        try:
            hcad.load(hcad_session)
        except Exception as e:
            log.warning(f"HCAD bulk load failed (will use API fallback): {e}")
    else:
        log.info("Small batch â skipping HCAD bulk download, using ArcGIS API only")

    # Re-login before PDF downloads (session may have gone stale during HCAD load)
    log.info("Re-authenticating with county clerk before PDF downloads...")
    logged_in = login_to_cclerk(session)
    if not logged_in:
        log.warning("Re-login failed â PDF downloads may fail")

    # Process each new listing
    new_records = []
    api_session = requests.Session()
    api_session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })

    log.info(f"=== Starting PDF processing: {len(new_listings)} PDFs to download ===")

    # Diagnostic: dump full extracted text for first 5 PDFs to help debug parsing
    DIAG_LIMIT = 10
    diag_count = 0

    # Per-PDF timeout handler
    class PdfTimeout(Exception):
        pass

    def _timeout_handler(signum, frame):
        raise PdfTimeout("PDF processing exceeded time limit")

    for i, listing in enumerate(new_listings):
        doc_id = listing["doc_id"]
        t0 = time.time()
        log.info(f"Processing {doc_id} ({i+1}/{len(new_listings)})...")

        try:
            # Set per-PDF timeout
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(PDF_TIMEOUT)

            # Download PDF using encrypted token URL from search results
            pdf_bytes = download_frcl_pdf(session, doc_id, listing.get("pdf_url", ""))
            if not pdf_bytes:
                log.warning(f"  Skipping {doc_id} - could not download PDF")
                seen_ids.add(doc_id)  # Mark as seen so we don't retry
                signal.alarm(0)
                continue

            # Parse PDF
            parsed = parse_foreclosure_pdf(pdf_bytes, doc_id)
            signal.alarm(0)  # Cancel timeout after successful parse
            log.info(f"  PDF parsed in {time.time()-t0:.1f}s â grantor={parsed.get('grantor','')[:30]}")

            # Diagnostic: dump full extracted text for first N PDFs
            if diag_count < DIAG_LIMIT:
                diag_count += 1
                try:
                    with pdfplumber.open(BytesIO(pdf_bytes)) as dpdf:
                        diag_text = ""
                        for pg in dpdf.pages:
                            diag_text += (pg.extract_text() or "") + "\n"
                    log.info(f"  ===== DIAGNOSTIC TEXT DUMP ({doc_id}) =====")
                    # Print first 2000 chars so we can see the actual content
                    for chunk_start in range(0, min(len(diag_text), 2000), 200):
                        chunk = diag_text[chunk_start:chunk_start + 200].replace('\n', ' | ')
                        log.info(f"  DIAG[{chunk_start}]: {chunk}")
                    log.info(f"  ===== END DIAGNOSTIC ({doc_id}, total {len(diag_text)} chars) ====="  )
                except Exception as de:
                    log.info(f"  DIAG ERROR for {doc_id}: {de}")
        except PdfTimeout:
            signal.alarm(0)
            log.warning(f"  TIMEOUT: {doc_id} exceeded {PDF_TIMEOUT}s â skipping")
            seen_ids.add(doc_id)
            continue
        except Exception as e:
            signal.alarm(0)
            log.warning(f"  ERROR processing {doc_id}: {e} â skipping")
            seen_ids.add(doc_id)
            continue

        # Build record
        record = {
            "doc_id": doc_id,
            "doc_type": "FRCL",
            "cat": "FORECLOSURE",
            "cat_label": "Foreclosure Posting",
            "sale_date": listing["sale_date"] or parsed.get("sale_date", ""),
            "file_date": listing["file_date"],
            "date_scraped": today,
            "batch_id": batch_id,
            "batch_label": batch_label,
            "grantor": parsed.get("grantor", ""),
            "co_borrower": parsed.get("co_borrower", ""),
            "amount": parsed.get("amount", ""),
            "mortgagee": parsed.get("mortgagee", ""),
            "legal_description": parsed.get("legal_description", ""),
            "prop_address": parsed.get("property_address", ""),
            "prop_city": parsed.get("property_city", ""),
            "prop_state": parsed.get("property_state", "TX"),
            "prop_zip": parsed.get("property_zip", ""),
            "match_confidence": "none",
            "hcad_url": "",
            "clerk_url": listing.get("pdf_url", f"https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx?id={doc_id}"),
            "needs_ocr": parsed.get("needs_ocr", False),
        }

        # ââ Enrich with HCAD data ââ
        # Step 1: If we got address from PDF, we're done (high confidence)
        if record["prop_address"]:
            record["match_confidence"] = "high"
            log.info(f"  Address from PDF: {record['prop_address']}, {record['prop_city']}")
        else:
            # Step 2: Try legal description matching
            if parsed.get("legal_description"):
                legal_parsed = convert_legal_numbers(parsed["legal_description"])
                hcad_match = hcad.match_legal(legal_parsed)
                if hcad_match:
                    record["prop_address"] = hcad_match["prop_address"]
                    record["prop_city"] = hcad_match.get("prop_city", "Houston")
                    record["prop_state"] = "TX"
                    record["prop_zip"] = hcad_match.get("prop_zip", "")
                    record["match_confidence"] = "high"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{hcad_match['acct']}"
                    log.info(f"  Legal desc match: {record['prop_address']}")

            # Step 3: Try owner name matching against bulk data
            if not record["prop_address"] and parsed.get("grantor"):
                grantor_name = parsed["grantor"].upper()
                hcad_match = hcad.match_owner(grantor_name)
                if hcad_match:
                    record["prop_address"] = hcad_match["prop_address"]
                    record["prop_city"] = hcad_match.get("prop_city", "Houston")
                    record["prop_state"] = "TX"
                    record["prop_zip"] = hcad_match.get("prop_zip", "")
                    record["match_confidence"] = "medium"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{hcad_match['acct']}"
                    log.info(f"  Owner name match: {record['prop_address']}")

            # Step 4: Try HCAD ArcGIS API as last resort
            if not record["prop_address"] and parsed.get("grantor"):
                grantor_name = parsed["grantor"].upper()
                api_result = hcad_api_search(api_session, grantor_name)
                if api_result:
                    record["prop_address"] = api_result["street"]
                    record["prop_city"] = api_result["city"]
                    record["prop_state"] = "TX"
                    record["prop_zip"] = api_result["zip"]
                    record["match_confidence"] = "medium"
                    record["hcad_url"] = f"{HCAD_DETAIL_BASE}{api_result['acct']}"
                time.sleep(0.25)

        # If still no address, set "Not found"
        if not record["prop_address"]:
            record["prop_address"] = "Not found"
            log.info(f"  No address found for {doc_id}")

        new_records.append(record)
        seen_ids.add(doc_id)
        elapsed = time.time() - t0
        log.info(f"  Done {doc_id} in {elapsed:.1f}s â addr={record['prop_address'][:40]}")

        # Rate limit between PDF downloads
        time.sleep(0.3)

    log.info(f"Processed {len(new_records)} new foreclosure records")

    # Merge with existing records (update if doc_id already exists)
    existing_by_id = {r["doc_id"]: r for r in existing_records}
    for rec in new_records:
        existing_by_id[rec["doc_id"]] = rec

    all_records = list(existing_by_id.values())

    # Remove records with sale dates that have already passed
    now = datetime.now()
    active_records = []
    for rec in all_records:
        # Keep record if we can't parse the sale date (be safe)
        try:
            sd = rec.get("sale_date", "")
            if sd:
                # Try parsing various date formats
                for fmt in ("%m/%d/%Y", "%B %d, %Y", "%Y-%m-%d"):
                    try:
                        sale_dt = datetime.strptime(sd, fmt)
                        if sale_dt < now - timedelta(days=7):
                            # Sale date has passed by more than a week, skip
                            continue
                        break
                    except ValueError:
                        continue
            active_records.append(rec)
        except:
            active_records.append(rec)

    _save_output(active_records, seen_ids, today, target_months)


def _save_output(records: list, seen_ids: set, today: str, target_months: list):
    """Save foreclosure records and seen IDs."""
    # Sort by sale date
    records.sort(key=lambda r: r.get("sale_date", ""), reverse=False)

    with_address = sum(1 for r in records if r.get("prop_address", "").strip()
                       and r["prop_address"] != "Not found")

    output = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Harris County Clerk - Foreclosure Postings (cclerk.hctx.net)",
        "target_months": [f"{MONTH_NAMES[m]} {y}" for y, m in target_months],
        "total": len(records),
        "with_address": with_address,
        "records": records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    log.info(f"Saved {OUTPUT_FILE}: {len(records)} records, {with_address} with address")

    # Save seen IDs
    with open(SEEN_FILE, "w") as f:
        json.dump({"seen_ids": sorted(seen_ids), "last_updated": today}, f, indent=2)
    log.info(f"Saved {SEEN_FILE}: {len(seen_ids)} tracked IDs")

    log.info(f"Done. Total: {len(records)} | With address: {with_address}")


if __name__ == "__main__":
    main()
