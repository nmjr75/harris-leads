"""
Harris County Motivated Seller Lead Scraper
Pulls distress records from Harris County Clerk portal + HCAD bulk parcel data.
Correct portal: https://cclerk.hctx.net/applications/websearch/RP.aspx
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PwTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

CLERK_SEARCH_URL = "https://cclerk.hctx.net/applications/websearch/RP.aspx"
CLERK_BASE       = "https://cclerk.hctx.net"

HCAD_BULK_BASE     = "https://pdata.hcad.org/download/"
HCAD_DOWNLOAD_PAGE = "https://pdata.hcad.org/download/index.html"

DOC_TYPE_MAP = {
    "L/P":    ("LP",      "Lis Pendens"),
    "NOTICE": ("NOFC",    "Notice of Tax Foreclosure / Trustee Sale"),
    "TRSALE": ("NOFC",    "Trustee Sale"),
    "T/L":    ("TAXLIEN", "Notice of Federal Tax Lien"),
    "LIEN":   ("LIEN",    "Lien (HOA / State Tax / Child Support / Other)"),
    "JUDGE":  ("JUD",     "Judgment"),
    "A/J":    ("JUD",     "Abstract of Judgment"),
    "LEVY":   ("LEVY",    "Notice of Levy on Real Estate"),
    "PROB":   ("PROBATE", "Probate Proceedings"),
    "AFFT":   ("PROBATE", "Affidavit of Heirship / Small Estate"),
    "WILL":   ("PROBATE", "Certified Copy of Probated Will"),
    "BNKRCY": ("BNKRCY",  "Bankruptcy"),
    "REL":    ("REL",     "Release (Lien / Judgment / Tax Lien / DOT)"),
}

TARGET_TYPES = list(DOC_TYPE_MAP.keys())

LLC_PATTERN = re.compile(
    r"\b(LLC|L\.L\.C|INC|CORP|TRUST|ESTATE|HOLDINGS|PROPERTIES|PARTNERS|LP|LTD)\b",
    re.IGNORECASE,
)

def compute_flags_and_score(record: dict, all_records: list) -> tuple:
    flags = []
    score = 30

    cat          = record.get("cat", "")
    amount       = record.get("amount") or 0
    owner        = record.get("owner", "") or ""
    filed        = record.get("filed", "") or ""
    prop_address = record.get("prop_address", "") or ""

    if cat == "LP":
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure / trustee sale")
    if cat == "JUD":
        flags.append("Judgment lien")
    if cat == "TAXLIEN":
        flags.append("Federal tax lien")
    if cat == "LIEN":
        flags.append("Lien (HOA / state tax / other)")
    if cat == "LEVY":
        flags.append("Levy on real estate")
    if cat == "PROBATE":
        flags.append("Probate / estate")
    if cat == "BNKRCY":
        flags.append("Bankruptcy")
    if LLC_PATTERN.search(owner):
        flags.append("LLC / corp owner")

    try:
        filed_dt = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_dt).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    score += len(flags) * 10

    if owner:
        owner_cats = {r.get("cat") for r in all_records if r.get("owner") == owner}
        if "LP" in owner_cats and "NOFC" in owner_cats:
            score += 20

    try
