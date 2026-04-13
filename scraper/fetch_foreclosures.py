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
    from PIL import Image
    from pdf2image import convert_from_bytes
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    print("WARNING: pytesseract/Pillow/pdf2image not installed. OCR will be unavailable.")

# ═══════════════════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════════
FRCL_SEARCH_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
FRCL_DOC_URL = "https://www.cclerk.hctx.net/applications/websearch/ViewECdocs.aspx"
CCLERK_LOGIN_URL = "https://www.cclerk.hctx.net/Applications/WebSearch/Registration/Login.aspx"

HCAD_BULK_URL = "https://download.hcad.org/data/CAMA/2026/Real_acct_owner.zip"

# Batch limit: process at most this many PDFs per run to stay within
# GitHub Actions timeout.  Remaining PDFs are picked up on the next run
# because we track seen_ids incrementally.
MAX_PDFS_PER_RUN = 30

# OCR resolution — 200 DPI is a good balance of speed and accuracy
OCR_DPI = 200

# Per-PDF timeout in seconds — kill stuck OCR processing
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Word-to-Number Converter (for legal descriptions)
# ═══════════════════════════════════════════════════════════════════════════════
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
        "TWENTY-FOUR" → 24
        "EIGHT" → 8
        "ONE HUNDRED TWENTY-THREE" → 123
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
    # Pattern: "OF <SUBDIVISION NAME>, SEC..."  or  "OF <NAME>, A SUBDIVISION"
    m = re.search(r'(?:IN\s+BLOCK\s+\S+\s+OF|,\s+OF)\s+([A-Z][A-Z\s&\']+?)(?:\s*,|\s+SEC|\s+A\s+SUB|\s+IN\s+HARRIS)',
                  text, re.IGNORECASE)
    if m:
        subdiv = m.group(1).strip()
    else:
        # Try: "BLOCK X, <NAME> SEC Y"
        m = re.search(r'BLOCK\s+\S+\s*,?\s+([A-Z][A-Z\s&\']+?)(?:\s+SEC|\s*,\s*A\s+SUB|\s+IN\s+HARRIS)',
                      text, re.IGNORECASE)
        if m:
            subdiv = m.group(1).strip()
            # Remove leading "OF " if present
            subdiv = re.sub(r'^OF\s+', '', subdiv, flags=re.IGNORECASE)

    return {
        "lot": lot_num,
        "block": blk_num,
        "section": sec_num,
        "subdivision": subdiv,
        "raw": text,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  HCAD Bulk Data Loader (shared with main scraper)
# ═══════════════════════════════════════════════════════════════════════════════
class HCADMatcher:
    """Loads HCAD bulk data and matches by legal description or owner name."""

    def __init__(self):
        self.lookup = {}       # normalized owner name → address dict
        self.legal_index = {}  # "SUBDIVISION|BLKn" → address dict

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

        # Column mapping (71 columns, tab-delimited)
        # acct=0, mailto=2, site_addr_1=17, site_addr_3=19,
        # mail_city=5, lgl_1=67, lgl_2=68, lgl_3=69
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
            lgl_1 = cols[67].strip().upper() if len(cols) > 67 else ""
            lgl_2 = cols[68].strip().upper() if len(cols) > 68 else ""

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
            added = 0
            for line in owner_lines[1:]:
                cols = line.split("\t")
                if len(cols) < 3:
                    continue
                acct = cols[0].strip()
                name = cols[2].strip().upper()
                norm = re.sub(r'\s+', ' ', name).strip()
                if norm and norm not in self.lookup:
                    # Find the address from the main lookup by acct
                    for existing_name, existing_info in self.lookup.items():
                        if existing_info.get("acct") == acct:
                            self.lookup[norm] = existing_info
                            added += 1
                            break
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

        return None

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


# ═══════════════════════════════════════════════════════════════════════════════
#  HCAD ArcGIS API (fallback for when bulk data doesn't match)
# ═══════════════════════════════════════════════════════════════════════════════
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
    pfx = (best.get("site_str_pfx") or "").strip()
    num = (best.get("site_str_num") or "").strip()
    sname = (best.get("site_str_name") or "").strip()
    sfx = (best.get("site_str_sfx") or "").strip()
    sdir = (best.get("site_str_sfx_dir") or "").strip()
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

    city = (best.get("site_city") or "").strip()
    zipcode = (best.get("site_zip") or "").strip()
    acct = (best.get("HCAD_NUM") or "").strip()
    owner = (best.get("owner_name_1") or "").strip()

    log.info(f"HCAD API: found residential '{owner}' -> {street}, {city} {zipcode}")
    return {
        "acct": acct,
        "owner_name": owner,
        "street": street,
        "city": city or "Houston",
        "state": "TX",
        "zip": zipcode,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF Parser
# ═══════════════════════════════════════════════════════════════════════════════
def parse_foreclosure_pdf(pdf_bytes: bytes, doc_id: str) -> dict:
    """Extract key fields from a foreclosure posting PDF.

    Handles two main formats:
    1. Format A: Labeled fields (Grantor(s):, Legal Description:, etc.)
    2. Format B: "NOTICE OF FORECLOSURE SALE AND APPOINTMENT OF SUBSTITUTE TRUSTEE"
       with address at top, "Property:" section, "Obligation Secured:" section
    """
    result = {
        "grantor": "",
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
            if has_scanned_pages and not full_text.strip():
                # PDF is entirely scanned images — run OCR to extract text
                if HAS_OCR:
                    log.info(f"  {doc_id} is a scanned PDF — running OCR at {OCR_DPI} DPI...")
                    try:
                        images = convert_from_bytes(pdf_bytes, dpi=OCR_DPI)
                        for page_num, img in enumerate(images, 1):
                            page_text = pytesseract.image_to_string(img)
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
                    log.info(f"  {doc_id} is a scanned PDF — OCR not available, flagging for VA lookup")
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

    lines = full_text.split("\n")

    # ── Try Format A: labeled fields ──
    if "Grantor(s):" in full_text or "Grantor (s):" in full_text:
        for line in lines:
            line_s = line.strip()

            # Grantor
            m = re.match(r'Grantor\s*\(s?\)\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["grantor"] = m.group(1).strip()

            # Legal Description
            m = re.match(r'Legal\s+Description\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["legal_description"] = m.group(1).strip()

            # Amount
            m = re.match(r'Amount\s*:\s*\$?([\d,\.]+)', line_s, re.IGNORECASE)
            if m:
                result["amount"] = "$" + m.group(1)

            # Date of Sale — strip time/hours portion
            m = re.match(r'Date\s+of\s+Sale\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                raw_date = m.group(1).strip()
                # Clean: "May 5, 2026 between the hours..." → "May 5, 2026"
                cleaned = re.sub(r'\s+between\s+.*', '', raw_date, flags=re.IGNORECASE)
                result["sale_date"] = cleaned

            # Current Mortgagee
            m = re.match(r'Current\s+Mortgagee\s*:\s*(.+)', line_s, re.IGNORECASE)
            if m:
                result["mortgagee"] = m.group(1).strip()

        return result

    # ── Format B: Structured paragraphs ──
    # Street suffixes (including full words for OCR accuracy)
    _SFX = (r"(?:ST|RD|DR|LN|CT|AVE|BLVD|WAY|PL|CIR|TRL|PKWY|CV|"
            r"STREET|ROAD|DRIVE|LANE|COURT|AVENUE|BOULEVARD|PLACE|"
            r"CIRCLE|TRAIL|PARKWAY|COVE)")

    # Check for address at top of document (first 10 lines)
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

    # Check for "Commonly known as:" line
    if not result["property_address"]:
        m = re.search(r'[Cc]ommonly\s+known\s+as\s*:?\s*(\d+\s+[A-Z][A-Z\s]+?' + _SFX + r'\.?)\s*[,]?\s*'
                      r'([A-Z]+)\s*[,]?\s*(?:TX|TEXAS)\s+(\d{5}(?:-\d{4})?)', full_text, re.IGNORECASE)
        if m:
            result["property_address"] = m.group(1).strip()
            result["property_city"] = m.group(2).strip()
            result["property_zip"] = m.group(3).strip()

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
            # Sanity check: city should be a real word, not OCR junk
            if len(city) >= 3 and city.isalpha():
                result["property_address"] = addr
                result["property_city"] = city
                result["property_state"] = "TX"
                result["property_zip"] = zipcode
                log.info(f"  Address (broad match): {addr}, {city}, TX {zipcode}")

    # Extract Property / Legal Description section
    # Try multiple patterns since OCR can garble the text
    legal_found = False
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

    # Extract grantor — try multiple patterns
    # Pattern 1: "executed by GRANTOR NAME"
    m = re.search(r'(?:executed\s+by|delivered\s+by)\s+([A-Z][A-Za-z\s,\.&]+?)(?:\s*,?\s*as\s+[Gg]rantor|\s+(?:and\s+wife|delivered|executed|in\s+favor|securing|to\s+))',
                  full_text, re.IGNORECASE)
    if m:
        result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 2: "WHEREAS, on DATE, GRANTOR NAME, ... as Grantor"
    if not result["grantor"]:
        m = re.search(r'WHEREAS\s*,\s*on\s+\d+/\d+/\d+\s*,\s*([A-Za-z\s,\.]+?)\s*,\s*(?:and\s+wife|as\s+[Gg]rantor)',
                      full_text, re.IGNORECASE)
        if m:
            result["grantor"] = m.group(1).strip().rstrip(",. ")

    # Pattern 3: "NAME secures the repay" (OCR-garbled version)
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

    # Clean up grantor — remove OCR junk that gets appended after the name
    if result["grantor"]:
        g = result["grantor"]
        # Remove newlines and everything after them (OCR artifact)
        if "\n" in g:
            # Keep text before the first newline that starts with lowercase/junk
            parts = g.split("\n")
            clean_parts = [parts[0]]
            for p in parts[1:]:
                p_stripped = p.strip()
                # Keep if it looks like part of a name (e.g., "AND WIFE", "HUSBAND AND WIFE", "AN UNMARRIED WOMAN")
                if re.match(r'^(?:AND\s+(?:WIFE|HUSBAND)|(?:AN?\s+)?(?:UN)?MARRIED|HUSBAND|WIFE|JR|SR|III?|IV)', p_stripped, re.IGNORECASE):
                    clean_parts.append(p_stripped)
                else:
                    break  # Stop at first non-name line
            g = " ".join(clean_parts)
        # Remove common OCR junk phrases that get captured
        g = re.sub(r'\s+(?:payment|ayment|indebtedness|securing|of\s+the|in\s+favor|the\s+repay|but\s+not\s+limited).*', '', g, flags=re.IGNORECASE)
        # Remove trailing junk punctuation
        g = g.strip().rstrip(",. ;:")
        result["grantor"] = g

    # Extract amount — multiple patterns
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

    # Extract mortgagee / servicer — multiple patterns
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

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  FRCL List Scraper
# ═══════════════════════════════════════════════════════════════════════════════
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

    # Parse all pages — with safety limits to prevent infinite loops
    MAX_PAGES = 100  # Hard cap: 100 pages × 20 results = 2,000 max
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

        # If this page added zero new records, we've seen them all — stop
        if new_on_page == 0 and page_num > 1:
            log.info(f"  No new records on page {page_num} — pagination complete")
            break

        # Hard cap to prevent runaway loops
        if page_num >= MAX_PAGES:
            log.warning(f"  Hit max page limit ({MAX_PAGES}) — stopping pagination")
            break

        # Check for next page — pager row has class "pagination-ys"
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
                log.info(f"  No more page links found — pagination complete")
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Login to County Clerk (required to download PDF documents)
# ═══════════════════════════════════════════════════════════════════════════════
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
        log.warning("CCLERK_USERNAME / CCLERK_PASSWORD not set — PDF downloads will fail")
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


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF Downloader
# ═══════════════════════════════════════════════════════════════════════════════
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
            log.warning(f"  Redirected to login page — session expired!")
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
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
        log.warning("Proceeding without login — PDF downloads may fail")

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
    for rec in existing_records:
        if rec.get("needs_ocr") and not rec.get("grantor"):
            ocr_retry_ids.add(rec["doc_id"])
    if ocr_retry_ids:
        log.info(f"Found {len(ocr_retry_ids)} records needing OCR reprocessing")

    # Filter to new doc IDs PLUS OCR retries
    new_listings = [l for l in all_listings if l["doc_id"] not in seen_ids or l["doc_id"] in ocr_retry_ids]
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
        log.info("Small batch — skipping HCAD bulk download, using ArcGIS API only")

    # Re-login before PDF downloads (session may have gone stale during HCAD load)
    log.info("Re-authenticating with county clerk before PDF downloads...")
    logged_in = login_to_cclerk(session)
    if not logged_in:
        log.warning("Re-login failed — PDF downloads may fail")

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
            log.info(f"  PDF parsed in {time.time()-t0:.1f}s — grantor={parsed.get('grantor','')[:30]}")
        except PdfTimeout:
            signal.alarm(0)
            log.warning(f"  TIMEOUT: {doc_id} exceeded {PDF_TIMEOUT}s — skipping")
            seen_ids.add(doc_id)
            continue
        except Exception as e:
            signal.alarm(0)
            log.warning(f"  ERROR processing {doc_id}: {e} — skipping")
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
            "grantor": parsed.get("grantor", ""),
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

        # ── Enrich with HCAD data ──
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
        log.info(f"  Done {doc_id} in {elapsed:.1f}s — addr={record['prop_address'][:40]}")

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
