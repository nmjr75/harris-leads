"""
Harris County Motivated Seller Lead Scraper
Portal: https://cclerk.hctx.net/applications/websearch/RP.aspx

Enriches records with HCAD parcel data for property + mailing addresses.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

CLERK_SEARCH_URL = "https://cclerk.hctx.net/applications/websearch/RP.aspx"
CLERK_BASE       = "https://cclerk.hctx.net"

# ── HCAD bulk-download base (confirmed April 2026) ──────────────────────────
# The old pdata.hcad.org/download/ prefix no longer resolves;
# HCAD now hosts files under download.hcad.org/data/CAMA/{year}/
HCAD_BULK_BASE = "https://download.hcad.org/data/CAMA/"

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


# ═══════════════════════════════════════════════════════════════════════════════
#  Scoring
# ═══════════════════════════════════════════════════════════════════════════════
def compute_flags_and_score(record: dict, all_records: list) -> tuple:
    flags = []
    score = 30
    cat          = record.get("cat", "")
    amount       = record.get("amount") or 0
    owner        = record.get("owner", "") or ""
    filed        = record.get("filed", "") or ""
    prop_address = record.get("prop_address", "") or ""

    if cat == "LP":      flags.append("Lis pendens")
    if cat == "NOFC":    flags.append("Pre-foreclosure / trustee sale")
    if cat == "JUD":     flags.append("Judgment lien")
    if cat == "TAXLIEN": flags.append("Federal tax lien")
    if cat == "LIEN":    flags.append("Lien (HOA / state tax / other)")
    if cat == "LEVY":    flags.append("Levy on real estate")
    if cat == "PROBATE": flags.append("Probate / estate")
    if cat == "BNKRCY":  flags.append("Bankruptcy")
    if LLC_PATTERN.search(owner): flags.append("LLC / corp owner")

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

    try:
        amt = float(str(amount).replace(",", "").replace("$", ""))
        if amt > 100_000:   score += 15
        elif amt > 50_000:  score += 10
    except Exception:
        pass

    if prop_address.strip():
        score += 5

    return flags, min(score, 100)


# ═══════════════════════════════════════════════════════════════════════════════
#  HCAD Parcel Loader  (address enrichment)
# ═══════════════════════════════════════════════════════════════════════════════
class HCADParcelLoader:
    """Download HCAD bulk Real_acct_owner data and build an owner→address lookup."""

    # Try current year first, then fall back
    FALLBACK_YEARS = [2026, 2025, 2024]

    # (File selection now handled dynamically in _parse_zip)

    def __init__(self):
        self.lookup: dict = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120"})

    # ── name helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _norm(name: str) -> str:
        """Uppercase, collapse whitespace, strip punctuation."""
        n = re.sub(r"[^\w\s]", " ", name.upper())
        return re.sub(r"\s+", " ", n).strip()

    def _index(self, name: str, info: dict):
        if not name:
            return
        n = self._norm(name)
        if not n:
            return
        self.lookup[n] = info

        # Also index "FIRST LAST" if input is "LAST, FIRST"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                alt = self._norm(f"{parts[1]} {parts[0]}")
                self.lookup[alt] = info

    def lookup_owner(self, name: str) -> Optional[dict]:
        """Try to match a clerk-record owner name against the HCAD index.

        Handles semicolon-separated names (tries each individually),
        "LAST FIRST" vs "FIRST LAST" permutations, and partial prefix
        matching as a last resort.
        """
        if not name or not self.lookup:
            return None

        # Split semicolons – clerk records often list multiple grantees
        candidates = [c.strip() for c in name.split(";") if c.strip()]
        if not candidates:
            return None

        for cand in candidates:
            # Skip LLC/Corp names – they rarely match HCAD owner names
            if LLC_PATTERN.search(cand):
                continue
            result = self._try_match(cand)
            if result:
                return result

        # If all were LLCs, try them anyway
        for cand in candidates:
            result = self._try_match(cand)
            if result:
                return result

        return None

    def _try_match(self, name: str) -> Optional[dict]:
        """Attempt exact, reversed, and prefix match for a single name."""
        n = self._norm(name)
        if not n:
            return None

        # 1. Exact match
        if n in self.lookup:
            return self.lookup[n]

        # 2. Try "FIRST LAST" → "LAST FIRST" and vice-versa
        parts = n.split()
        if len(parts) >= 2:
            reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
            if reversed_name in self.lookup:
                return self.lookup[reversed_name]

        # 3. Try just "LAST FIRST" (first two tokens)
        if len(parts) >= 3:
            short = f"{parts[0]} {parts[1]}"
            if short in self.lookup:
                return self.lookup[short]

        # 4. Prefix match – "SMITH JOHN" matches "SMITH JOHN A" or
        #    "SMITH JOHN WILLIAM III" etc.
        if len(n) >= 8:  # avoid matching on very short prefixes
            for key, val in self.lookup.items():
                if key.startswith(n) or n.startswith(key):
                    return val

        return None

    # ── download & parse ──────────────────────────────────────────────────
    def _download(self, url: str) -> Optional[bytes]:
        for attempt in range(3):
            try:
                log.info(f"  Downloading {url} (attempt {attempt+1})")
                r = self.session.get(url, timeout=180, stream=True)
                if r.status_code == 200:
                    log.info(f"  Download OK – {len(r.content):,} bytes")
                    return r.content
                log.warning(f"  HTTP {r.status_code} for {url}")
            except Exception as e:
                log.warning(f"  Download error: {e}")
            time.sleep(3)
        return None

    def _detect_delimiter(self, sample: str) -> str:
        """Auto-detect whether the file is tab, pipe, or comma delimited."""
        tab_count   = sample.count("\t")
        pipe_count  = sample.count("|")
        comma_count = sample.count(",")
        if tab_count >= pipe_count and tab_count >= comma_count:
            return "\t"
        if pipe_count >= comma_count:
            return "|"
        return ","

    def _find_column(self, headers: list, *candidates: str) -> Optional[int]:
        """Find the index of the first matching column name (case-insensitive)."""
        header_upper = [h.upper().strip() for h in headers]
        for cand in candidates:
            cu = cand.upper().strip()
            if cu in header_upper:
                return header_upper.index(cu)
        return None

    def _read_tabular(self, zf, filename: str):
        """Read a tab-delimited file from a zip, return (headers, rows_iterator)."""
        f = zf.open(filename)
        raw = io.TextIOWrapper(f, encoding="latin-1")

        # Read header + a few sample lines to detect delimiter
        sample_lines = []
        for i, line in enumerate(raw):
            sample_lines.append(line)
            if i >= 5:
                break

        if not sample_lines:
            return None, None, None, None

        delimiter = self._detect_delimiter(sample_lines[0])
        headers = sample_lines[0].strip().split(delimiter)

        # Return headers, delimiter, remaining iterator, and sample data lines
        return headers, delimiter, raw, sample_lines[1:]

    def _parse_zip(self, data: bytes) -> int:
        """Extract owner→address records from HCAD CAMA zip.

        The zip contains separate files that must be joined:
        - owners.txt:    owner name + mailing address + account number
        - real_acct.txt: site/property address + account number
        We join on account number to build a complete owner→address lookup.
        """
        count = 0
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                names = zf.namelist()
                log.info(f"  Zip contains: {names}")

                # Locate files by basename
                file_map = {}
                for n in names:
                    base = n.lower().rsplit("/", 1)[-1].rsplit(".", 1)[0]
                    file_map[base] = n

                def safe_get(fields, idx):
                    if idx is not None and idx < len(fields):
                        return fields[idx].strip()
                    return ""

                # ── Step 1: Parse real_acct.txt → {acct: site address} ────
                site_by_acct = {}
                if "real_acct" in file_map:
                    ra_file = file_map["real_acct"]
                    log.info(f"  Parsing site addresses from: {ra_file}")
                    headers, delim, stream, sample_data = self._read_tabular(zf, ra_file)
                    if headers:
                        log.info(f"  Columns ({len(headers)}): {headers[:15]}{'...' if len(headers) > 15 else ''}")
                        col_acct = self._find_column(headers, "ACCT", "ACCOUNT", "ACCT_NUM")
                        col_site = self._find_column(
                            headers, "SITE_ADDR_1", "SITEADDR", "SITE_ADDR",
                            "SITE_ADDRESS", "SITE_ADDRESS_1", "PROP_ADDR",
                            "PROPERTY_ADDRESS", "STR_NUM",
                        )
                        col_city = self._find_column(
                            headers, "SITE_ADDR_3", "SITE_CITY",
                        )
                        col_zip = self._find_column(
                            headers, "SITE_ADDR_4", "SITE_ZIP",
                        )
                        if col_acct is not None and col_site is not None:
                            log.info(f"  real_acct columns: acct={col_acct}, site={col_site}, city={col_city}, zip={col_zip}")
                            for line in (sample_data + list(stream)):
                                fields = line.strip().split(delim)
                                acct = safe_get(fields, col_acct)
                                addr = safe_get(fields, col_site)
                                if acct and addr:
                                    site_by_acct[acct.strip()] = {
                                        "prop_address": addr,
                                        "prop_city":    safe_get(fields, col_city) or "Houston",
                                        "prop_zip":     safe_get(fields, col_zip),
                                    }
                            log.info(f"  Site addresses loaded: {len(site_by_acct):,}")
                        else:
                            log.warning(f"  real_acct.txt missing acct or site_addr column: {headers[:15]}")

                # ── Step 2: Parse owners.txt → owner name index ───────────
                if "owners" in file_map:
                    ow_file = file_map["owners"]
                    log.info(f"  Parsing owner records from: {ow_file}")
                    headers, delim, stream, sample_data = self._read_tabular(zf, ow_file)
                    if headers:
                        log.info(f"  Columns ({len(headers)}): {headers[:15]}{'...' if len(headers) > 15 else ''}")
                        col_acct = self._find_column(headers, "ACCT", "ACCOUNT", "ACCT_NUM")
                        col_owner = self._find_column(
                            headers, "OWN_NAME", "OWNER_NAME", "OWN1", "OWNER",
                            "NAME", "OWNER_1", "OWN_NAME1",
                        )
                        # HCAD owners.txt uses OWN_ADDR_1 for mailing address
                        col_mail = self._find_column(
                            headers, "OWN_ADDR_1", "MAIL_ADDR_1", "MAILADR1",
                            "MAIL_ADDRESS", "MAIL_ADDR", "MAILING_ADDRESS",
                        )
                        col_mail_city = self._find_column(
                            headers, "OWN_ADDR_3", "MAIL_CITY", "MAILCITY", "OWN_CITY",
                        )
                        col_mail_state = self._find_column(
                            headers, "OWN_STATE", "MAIL_STATE", "MAILSTATE",
                        )
                        col_mail_zip = self._find_column(
                            headers, "OWN_ZIP", "MAIL_ZIP", "MAILZIP",
                        )

                        if col_owner is None:
                            log.warning(f"  Cannot find owner column in owners.txt: {headers}")
                        else:
                            log.info(f"  owners.txt columns: owner={col_owner}, acct={col_acct}, "
                                     f"mail={col_mail}, city={col_mail_city}")
                            for line in (sample_data + list(stream)):
                                fields = line.strip().split(delim)
                                owner = safe_get(fields, col_owner)
                                if not owner:
                                    continue

                                info = {
                                    "prop_address": "",
                                    "prop_city":    "Houston",
                                    "prop_state":   "TX",
                                    "prop_zip":     "",
                                    "mail_address": safe_get(fields, col_mail),
                                    "mail_city":    safe_get(fields, col_mail_city),
                                    "mail_state":   safe_get(fields, col_mail_state) or "TX",
                                    "mail_zip":     safe_get(fields, col_mail_zip),
                                }

                                # Merge site address from real_acct via account number
                                if col_acct is not None:
                                    acct = safe_get(fields, col_acct).strip()
                                    site = site_by_acct.get(acct)
                                    if site:
                                        info["prop_address"] = site["prop_address"]
                                        info["prop_city"]    = site.get("prop_city", "Houston")
                                        info["prop_zip"]     = site.get("prop_zip", "")

                                self._index(owner, info)
                                count += 1

                # ── Fallback: try a single combined file ──────────────────
                if count == 0:
                    # Look for a combined file like real_acct_owner.txt
                    target = None
                    for basename in ["real_acct_owner", "real_acct_owner_2026",
                                     "real_acct_owner_2025", "real_acct_owner_2024"]:
                        if basename in file_map:
                            target = file_map[basename]
                            break
                    if target:
                        log.info(f"  Trying combined file: {target}")
                        count = self._parse_single_file(zf, target)

        except zipfile.BadZipFile:
            log.warning("  Not a valid zip file")
        except Exception as e:
            log.warning(f"  Parse error: {e}", exc_info=True)
        return count

    def _parse_single_file(self, zf, target: str) -> int:
        """Fallback: parse a single file that has both owner and address columns."""
        count = 0
        headers, delim, stream, sample_data = self._read_tabular(zf, target)
        if not headers:
            return 0

        log.info(f"  Columns ({len(headers)}): {headers[:15]}{'...' if len(headers) > 15 else ''}")

        col_owner = self._find_column(
            headers, "OWN_NAME", "OWNER_NAME", "OWN1", "OWNER", "NAME",
        )
        col_site_addr = self._find_column(
            headers, "SITE_ADDR_1", "SITEADDR", "SITE_ADDR",
            "SITE_ADDRESS", "PROP_ADDR", "PROPERTY_ADDRESS",
        )
        col_site_city = self._find_column(headers, "SITE_ADDR_3", "SITE_CITY", "CITY")
        col_site_zip = self._find_column(headers, "SITE_ADDR_4", "SITE_ZIP", "ZIP")
        col_mail_addr = self._find_column(
            headers, "OWN_ADDR_1", "MAIL_ADDR_1", "MAILADR1",
            "MAIL_ADDRESS", "MAIL_ADDR",
        )
        col_mail_city = self._find_column(headers, "OWN_ADDR_3", "MAIL_CITY", "OWN_CITY")
        col_mail_state = self._find_column(headers, "OWN_STATE", "MAIL_STATE")
        col_mail_zip = self._find_column(headers, "OWN_ZIP", "MAIL_ZIP")

        if col_owner is None:
            log.warning(f"  Cannot find owner column in: {headers}")
            return 0

        def safe_get(fields, idx):
            if idx is not None and idx < len(fields):
                return fields[idx].strip()
            return ""

        for line in (sample_data + list(stream)):
            fields = line.strip().split(delim)
            owner = safe_get(fields, col_owner)
            if not owner:
                continue
            info = {
                "prop_address": safe_get(fields, col_site_addr),
                "prop_city":    safe_get(fields, col_site_city) or "Houston",
                "prop_state":   "TX",
                "prop_zip":     safe_get(fields, col_site_zip),
                "mail_address": safe_get(fields, col_mail_addr),
                "mail_city":    safe_get(fields, col_mail_city),
                "mail_state":   safe_get(fields, col_mail_state) or "TX",
                "mail_zip":     safe_get(fields, col_mail_zip),
            }
            self._index(owner, info)
            count += 1

        return count

    def load(self) -> int:
        """Download HCAD data and build the owner→address lookup table."""
        log.info("Loading HCAD parcel data for address enrichment...")

        for year in self.FALLBACK_YEARS:
            url = f"{HCAD_BULK_BASE}{year}/Real_acct_owner.zip"
            data = self._download(url)
            if data:
                n = self._parse_zip(data)
                if n > 0:
                    log.info(f"  Loaded {n:,} parcels from {year} data")
                    return n
                else:
                    log.warning(f"  Downloaded {year} zip but parsed 0 records")

        # Fallback: try the old URL pattern too
        log.info("  Trying legacy pdata.hcad.org URL pattern...")
        for year in self.FALLBACK_YEARS:
            url = f"https://pdata.hcad.org/download/{year}/Real_acct_owner.zip"
            data = self._download(url)
            if data:
                n = self._parse_zip(data)
                if n > 0:
                    log.info(f"  Loaded {n:,} parcels from legacy URL ({year})")
                    return n

        log.warning("  Could not load HCAD data. Address enrichment will be skipped.")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
#  Clerk Scraper  (Playwright-based)
# ═══════════════════════════════════════════════════════════════════════════════
class ClerkScraper:
    NAV_TIMEOUT    = 60_000
    SEARCH_TIMEOUT = 60_000
    MAX_PER_SEARCH = 200   # clerk portal hard cap per search

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date   = end_date
        self.results: list = []

    def _fmt(self, dt: datetime) -> str:
        return dt.strftime("%m/%d/%Y")

    async def _search_one(self, page, doc_type: str, start: datetime, end: datetime) -> list:
        """Scrape one doc type for a given date range. Handles pagination."""
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        records = []

        for attempt in range(3):
            try:
                await page.goto(CLERK_SEARCH_URL, timeout=self.NAV_TIMEOUT)
                await page.wait_for_load_state("networkidle", timeout=self.NAV_TIMEOUT)
                await asyncio.sleep(2)

                await page.wait_for_selector(
                    "[id*='txtInstrument']",
                    state="attached",
                    timeout=self.NAV_TIMEOUT,
                )

                await page.evaluate(f"""
                    document.querySelectorAll('[id*="txtInstrument"]')[0].value = '{doc_type}';
                    document.querySelectorAll('[id*="txtFrom"]')[0].value = '{self._fmt(start)}';
                    document.querySelectorAll('[id*="txtTo"]')[0].value = '{self._fmt(end)}';
                """)

                await page.evaluate("""
                    document.querySelectorAll('[id*="btnSearch"]')[0].click();
                """)
                await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)
                await asyncio.sleep(2)

                page_num = 0
                while True:
                    page_num += 1
                    html  = await page.content()
                    soup  = BeautifulSoup(html, "lxml")
                    batch = self._parse_results(soup, doc_type, cat, cat_label)
                    log.info(f"    {doc_type} page {page_num}: {len(batch)} rows")
                    records.extend(batch)

                    # ── Pagination: try multiple selectors ────────────────
                    next_found = False

                    # ASP.NET input button
                    next_btn = page.locator("input[value='NEXT']")
                    if await next_btn.count() > 0:
                        await next_btn.click()
                        next_found = True

                    # ASP.NET link-style pagination
                    if not next_found:
                        next_link = page.locator("a:has-text('NEXT'), a:has-text('Next')")
                        if await next_link.count() > 0:
                            await next_link.first.click()
                            next_found = True

                    # Generic ">" or ">>" buttons
                    if not next_found:
                        next_arrow = page.locator("a:has-text('>')").last
                        if await next_arrow.count() > 0:
                            text = await next_arrow.text_content()
                            if text and text.strip() in [">", ">>", "Next >", "Next"]:
                                await next_arrow.click()
                                next_found = True

                    if not next_found:
                        break

                    await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)
                    await asyncio.sleep(1)

                log.info(f"  {doc_type} [{self._fmt(start)} – {self._fmt(end)}]: {len(records)} total")
                return records

            except Exception as e:
                log.warning(f"  {doc_type} attempt {attempt+1} error: {e}")
                await asyncio.sleep(3)

        return records

    async def _search_with_splitting(self, page, doc_type: str, start: datetime, end: datetime) -> list:
        """Search a doc type; if results hit the cap, split the date range and recurse."""
        records = await self._search_one(page, doc_type, start, end)

        # If we hit exactly MAX_PER_SEARCH, we likely missed records.
        # Split the date range in half and search each half.
        if len(records) >= self.MAX_PER_SEARCH and (end - start).days > 1:
            log.info(f"  {doc_type}: hit {self.MAX_PER_SEARCH}-record cap, splitting date range")
            mid = start + (end - start) / 2
            records_a = await self._search_with_splitting(page, doc_type, start, mid)
            records_b = await self._search_with_splitting(page, doc_type, mid + timedelta(days=1), end)
            return records_a + records_b

        return records

    def _parse_results(self, soup: BeautifulSoup, doc_type: str, cat: str, cat_label: str) -> list:
        records = []
        file_spans = soup.find_all(
            "span", id=re.compile(r"ListViewl_ctrl\d+_lblFileNo", re.IGNORECASE)
        )
        if not file_spans:
            file_spans = [
                s for s in soup.find_all("span")
                if re.match(r"RP-\d+", s.get_text(strip=True))
            ]

        log.info(f"    Found {len(file_spans)} file number spans")

        for span in file_spans:
            try:
                file_num = span.get_text(strip=True)
                if not file_num.startswith("RP-"):
                    continue

                span_id = span.get("id", "")
                base = span_id.replace("_lblFileNo", "")

                def get(field_id):
                    el = soup.find(id=field_id)
                    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""

                file_date = get(f"{base}_lblFileDate")

                # Grantor / Grantee names
                grantor  = ""
                grantees = []
                ctrl_idx = 0
                while True:
                    row_id  = f"{base}_lvOR_ctrl{ctrl_idx}_row"
                    name_id = f"{base}_lvOR_ctrl{ctrl_idx}_lblNames"
                    row_el  = soup.find(id=row_id)
                    name_el = soup.find(id=name_id)
                    if not row_el and not name_el:
                        break
                    row_text  = row_el.get_text(strip=True)  if row_el  else ""
                    name_text = name_el.get_text(strip=True) if name_el else ""
                    if name_text:
                        if row_text.lower().startswith("grantee"):
                            grantees.append(name_text)
                        elif row_text.lower().startswith("grantor") and not grantor:
                            grantor = name_text
                    ctrl_idx += 1
                    if ctrl_idx > 20:
                        break

                owner = "; ".join(grantees) if grantees else grantor

                # Legal description
                subdiv  = get(f"{base}_lvLegal_ctrl0_lblSubDivAdd")
                section = get(f"{base}_lvLegal_ctrl0_lblSection")
                lot     = get(f"{base}_lvLegal_ctrl0_lblLot")
                block   = get(f"{base}_lvLegal_ctrl0_lblBlock")
                legal_parts = []
                if subdiv:          legal_parts.append(subdiv)
                if section.strip(): legal_parts.append(f"Sec: {section.strip()}")
                if lot.strip():     legal_parts.append(f"Lot: {lot.strip()}")
                if block.strip():   legal_parts.append(f"Block: {block.strip()}")
                legal = " | ".join(legal_parts)

                clerk_url = f"{CLERK_BASE}/applications/websearch/RPImage.aspx?ID={file_num}"

                filed_norm = ""
                for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                    try:
                        filed_norm = datetime.strptime(file_date, fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                records.append({
                    "doc_num":      file_num,
                    "doc_type":     doc_type,
                    "filed":        filed_norm or file_date,
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        owner,
                    "grantee":      grantor,
                    "amount":       "",
                    "legal":        legal,
                    "clerk_url":    clerk_url,
                    "prop_address": "",
                    "prop_city":    "Houston",
                    "prop_state":   "TX",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "TX",
                    "mail_zip":     "",
                    "flags":        [],
                    "score":        0,
                })
            except Exception as e:
                log.warning(f"  Row parse error: {e}")
                continue

        return records

    async def run(self) -> list:
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright not installed.")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 Chrome/120 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()

            for doc_type in TARGET_TYPES:
                log.info(f"Searching: {doc_type}")
                recs = await self._search_with_splitting(
                    page, doc_type, self.start_date, self.end_date,
                )
                self.results.extend(recs)
                await asyncio.sleep(2)

            await browser.close()

        return self.results


# ═══════════════════════════════════════════════════════════════════════════════
#  GHL CSV Export
# ═══════════════════════════════════════════════════════════════════════════════
def export_ghl_csv(records: list, path: str):
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            owner = rec.get("owner", "") or ""
            first_owner = owner.split(";")[0].strip()
            parts = first_owner.strip().split()
            first = parts[0] if len(parts) >= 2 else ""
            last  = " ".join(parts[1:]) if len(parts) >= 2 else first_owner

            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", "TX"),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", "Houston"),
                "Property State":         rec.get("prop_state", "TX"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount", ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                "Source":                 "Harris County Clerk",
                "Public Records URL":     rec.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved to {path}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
async def main():
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    log.info("Harris County Motivated Seller Scraper")
    log.info(f"Date range: {start_date.date()} to {end_date.date()}")

    # 1. Load HCAD parcel data
    parcel = HCADParcelLoader()
    parcel_count = parcel.load()

    # 2. Scrape clerk records
    scraper = ClerkScraper(start_date, end_date)
    raw     = await scraper.run()
    log.info(f"Raw records: {len(raw)}")

    # 3. Deduplicate
    seen, deduped = set(), []
    for rec in raw:
        key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(rec)
    log.info(f"After dedup: {len(deduped)}")

    # 4. Enrich with HCAD addresses
    with_address = 0
    match_count  = 0
    for rec in deduped:
        p = parcel.lookup_owner(rec.get("owner", ""))
        if p:
            match_count += 1
            rec.update(p)
            if p.get("prop_address"):
                with_address += 1
    log.info(f"HCAD matches: {match_count} | With property address: {with_address}")

    # 5. Compute flags & scores
    for rec in deduped:
        flags, score = compute_flags_and_score(rec, deduped)
        rec["flags"] = flags
        rec["score"] = score

    deduped.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 6. Save outputs
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Harris County Clerk (cclerk.hctx.net)",
        "date_range":   {
            "from": start_date.strftime("%Y-%m-%d"),
            "to":   end_date.strftime("%Y-%m-%d"),
        },
        "total":        len(deduped),
        "with_address": with_address,
        "records":      deduped,
    }

    for out_path in ["dashboard/records.json", "data/records.json"]:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info(f"Saved to {out_path}")

    today = datetime.now().strftime("%Y%m%d")
    export_ghl_csv(deduped, f"data/ghl_export_{today}.csv")

    log.info(f"Done. Total: {len(deduped)} | With address: {with_address}")


if __name__ == "__main__":
    asyncio.run(main())
