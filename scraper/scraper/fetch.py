"""
Harris County Motivated Seller Lead Scraper
Pulls distress records from Harris County Clerk portal + HCAD bulk parcel data.
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

# ---------------------------------------------------------------------------
# Optional Playwright import (graceful fallback for local testing w/o install)
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import async_playwright, TimeoutError as PwTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional dbfread import
# ---------------------------------------------------------------------------
try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE = "https://cclerk.hctx.net"
CLERK_SEARCH_URL = f"{CLERK_BASE}/publicrecords.aspx"

# Harris County Appraisal District bulk data (HCAD)
HCAD_DOWNLOAD_PAGE = "https://pdata.hcad.org/download/index.html"
HCAD_BULK_BASE = "https://pdata.hcad.org/download/"

# Document type codes → categories
DOC_TYPE_MAP = {
    # Lis Pendens
    "LP":       ("LP",       "Lis Pendens"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
    # Foreclosure
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    # Tax Deed
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    # Judgments
    "JUD":      ("JUD",      "Judgment"),
    "CCJ":      ("JUD",      "Certified Judgment"),
    "DRJUD":    ("JUD",      "Domestic Relations Judgment"),
    # Liens – Corp/IRS/Federal
    "LNCORPTX": ("TAXLIEN",  "Corp Tax Lien"),
    "LNIRS":    ("TAXLIEN",  "IRS Lien"),
    "LNFED":    ("TAXLIEN",  "Federal Lien"),
    # Liens – General
    "LN":       ("LIEN",     "Lien"),
    "LNMECH":   ("LIEN",     "Mechanic Lien"),
    "LNHOA":    ("LIEN",     "HOA Lien"),
    "MEDLN":    ("LIEN",     "Medicaid Lien"),
    # Probate
    "PRO":      ("PROBATE",  "Probate Document"),
    # Notice of Commencement
    "NOC":      ("NOC",      "Notice of Commencement"),
}

TARGET_TYPES = list(DOC_TYPE_MAP.keys())

# ---------------------------------------------------------------------------
# Seller score helpers
# ---------------------------------------------------------------------------
LLC_PATTERN = re.compile(
    r"\b(LLC|L\.L\.C|INC|CORP|TRUST|ESTATE|HOLDINGS|PROPERTIES|PARTNERS|LP|LTD)\b",
    re.IGNORECASE,
)

def compute_flags_and_score(record: dict, all_records: list) -> tuple[list[str], int]:
    flags = []
    score = 30  # base

    cat = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    amount = record.get("amount") or 0
    owner = record.get("owner", "") or ""
    filed = record.get("filed", "") or ""
    prop_address = record.get("prop_address", "") or ""

    # Flag logic
    if cat in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if cat == "NOFC" or doc_type in ("NOFC", "LP"):
        flags.append("Pre-foreclosure")
    if cat in ("JUD",):
        flags.append("Judgment lien")
    if cat == "TAXLIEN":
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PROBATE":
        flags.append("Probate / estate")
    if LLC_PATTERN.search(owner):
        flags.append("LLC / corp owner")

    # "New this week" – filed within last 7 days
    try:
        filed_dt = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_dt).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    # Score from flags
    for f in flags:
        score += 10

    # LP + Foreclosure combo bonus
    doc_nums = [r.get("doc_num") for r in all_records]
    owner_records = [r for r in all_records if r.get("owner") == owner and owner]
    owner_cats = {r.get("cat") for r in owner_records}
    if "LP" in owner_cats and "NOFC" in owner_cats:
        score += 20

    # Amount bonuses
    try:
        amt = float(str(amount).replace(",", "").replace("$", ""))
        if amt > 100_000:
            score += 15
        elif amt > 50_000:
            score += 10
    except Exception:
        pass

    # Has address bonus
    if prop_address.strip():
        score += 5

    score = min(score, 100)
    return flags, score


# ---------------------------------------------------------------------------
# HCAD Parcel Data Download
# ---------------------------------------------------------------------------
class HCADParcelLoader:
    """
    Downloads HCAD bulk parcel data and builds an owner-name lookup.
    Tries CSV first (faster), falls back to DBF.
    """

    KNOWN_CSV_URL = "https://pdata.hcad.org/download/2024/Real_acct_owner.zip"
    # Fallback patterns to try
    FALLBACK_YEARS = [2024, 2023, 2025]

    def __init__(self):
        self.lookup: dict[str, dict] = {}  # normalised_name -> parcel info
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; HarrisLeadBot/1.0)"})

    # ------------------------------------------------------------------
    def _norm_name(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _index_owner(self, name: str, info: dict):
        if not name:
            return
        n = self._norm_name(name)
        self.lookup[n] = info
        # Also index "LAST, FIRST" -> "FIRST LAST"
        if "," in n:
            parts = [p.strip() for p in n.split(",", 1)]
            alt = f"{parts[1]} {parts[0]}"
            self.lookup[alt] = info

    def lookup_owner(self, name: str) -> Optional[dict]:
        if not name:
            return None
        n = self._norm_name(name)
        if n in self.lookup:
            return self.lookup[n]
        # Try partial: last-name first word
        parts = n.split()
        if parts:
            for key in self.lookup:
                if key.startswith(parts[0]):
                    return self.lookup[key]
        return None

    # ------------------------------------------------------------------
    def _try_download_zip(self, url: str) -> Optional[bytes]:
        for attempt in range(3):
            try:
                log.info(f"  Downloading {url} (attempt {attempt+1})")
                r = self.session.get(url, timeout=120, stream=True)
                if r.status_code == 200:
                    return r.content
                log.warning(f"  HTTP {r.status_code} for {url}")
            except Exception as e:
                log.warning(f"  Download error: {e}")
            time.sleep(3)
        return None

    def _load_from_csv_bytes(self, data: bytes) -> int:
        """Parse zip containing CSV file."""
        count = 0
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_files:
                    return 0
                with zf.open(csv_files[0]) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"))
                    for row in reader:
                        owner = (
                            row.get("OWN1") or row.get("OWNER") or
                            row.get("owner_name") or row.get("OWNERNAME") or ""
                        ).strip()
                        info = {
                            "prop_address": (row.get("SITEADDR") or row.get("SITE_ADDR") or row.get("site_addr") or "").strip(),
                            "prop_city":    (row.get("SITE_CITY") or row.get("site_city") or "Houston").strip(),
                            "prop_state":   "TX",
                            "prop_zip":     (row.get("SITE_ZIP") or row.get("site_zip") or "").strip(),
                            "mail_address": (row.get("MAILADR1") or row.get("ADDR_1") or row.get("mail_addr") or "").strip(),
                            "mail_city":    (row.get("MAILCITY") or row.get("CITY") or "").strip(),
                            "mail_state":   (row.get("STATE") or "TX").strip(),
                            "mail_zip":     (row.get("MAILZIP") or row.get("ZIP") or "").strip(),
                        }
                        self._index_owner(owner, info)
                        count += 1
        except Exception as e:
            log.warning(f"  CSV parse error: {e}")
        return count

    def _load_from_dbf_bytes(self, data: bytes) -> int:
        """Parse zip containing DBF file (requires dbfread)."""
        if not DBFREAD_AVAILABLE:
            log.warning("dbfread not installed, skipping DBF parse")
            return 0
        count = 0
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_files:
                    return 0
                # Extract to temp
                tmpdir = Path("/tmp/hcad_dbf")
                tmpdir.mkdir(exist_ok=True)
                zf.extractall(tmpdir)
                for dbf_name in dbf_files:
                    dbf_path = tmpdir / Path(dbf_name).name
                    table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
                    for row in table:
                        r = dict(row)
                        owner = (
                            r.get("OWN1") or r.get("OWNER") or
                            r.get("OWNERNAME") or ""
                        )
                        if isinstance(owner, bytes):
                            owner = owner.decode("latin-1", errors="replace")
                        owner = str(owner).strip()
                        info = {
                            "prop_address": str(r.get("SITEADDR") or r.get("SITE_ADDR") or "").strip(),
                            "prop_city":    str(r.get("SITE_CITY") or "Houston").strip(),
                            "prop_state":   "TX",
                            "prop_zip":     str(r.get("SITE_ZIP") or "").strip(),
                            "mail_address": str(r.get("MAILADR1") or r.get("ADDR_1") or "").strip(),
                            "mail_city":    str(r.get("MAILCITY") or r.get("CITY") or "").strip(),
                            "mail_state":   str(r.get("STATE") or "TX").strip(),
                            "mail_zip":     str(r.get("MAILZIP") or r.get("ZIP") or "").strip(),
                        }
                        self._index_owner(owner, info)
                        count += 1
        except Exception as e:
            log.warning(f"  DBF parse error: {e}")
        return count

    def load(self) -> int:
        """Attempt to load parcel data. Returns number of records loaded."""
        log.info("Loading HCAD parcel data...")

        # Try known CSV zip first
        for year in self.FALLBACK_YEARS:
            url = f"https://pdata.hcad.org/download/{year}/Real_acct_owner.zip"
            data = self._try_download_zip(url)
            if data:
                n = self._load_from_csv_bytes(data)
                if n > 0:
                    log.info(f"  Loaded {n:,} parcels from CSV ({year})")
                    return n
                # Try DBF fallback
                n = self._load_from_dbf_bytes(data)
                if n > 0:
                    log.info(f"  Loaded {n:,} parcels from DBF ({year})")
                    return n

        # Try to discover current URL from download page
        try:
            r = requests.get(HCAD_DOWNLOAD_PAGE, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "Real_acct" in href and href.endswith(".zip"):
                    full = href if href.startswith("http") else HCAD_BULK_BASE + href.lstrip("/")
                    data = self._try_download_zip(full)
                    if data:
                        n = self._load_from_csv_bytes(data) or self._load_from_dbf_bytes(data)
                        if n > 0:
                            log.info(f"  Loaded {n:,} parcels from discovered URL: {full}")
                            return n
        except Exception as e:
            log.warning(f"  Could not scrape HCAD download page: {e}")

        log.warning("  Could not load HCAD parcel data. Address enrichment will be skipped.")
        return 0


# ---------------------------------------------------------------------------
# Harris County Clerk Scraper (Playwright)
# ---------------------------------------------------------------------------
class ClerkScraper:
    """
    Scrapes https://cclerk.hctx.net/publicrecords.aspx via Playwright.
    Performs date-range search for each target document type.
    """

    SEARCH_TIMEOUT = 60_000   # ms
    NAV_TIMEOUT   = 90_000

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date   = end_date
        self.results: list[dict] = []

    def _fmt_date(self, dt: datetime) -> str:
        return dt.strftime("%m/%d/%Y")

    # ------------------------------------------------------------------
    async def _search_doc_type(self, page, doc_type: str) -> list[dict]:
        records = []
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))

        for attempt in range(3):
            try:
                await page.goto(CLERK_SEARCH_URL, timeout=self.NAV_TIMEOUT)
                await page.wait_for_load_state("networkidle", timeout=self.NAV_TIMEOUT)

                # Select "Document Type" search option if available
                try:
                    await page.select_option("select[name*='SearchType'], #ddlSearchType", "DT")
                except Exception:
                    pass

                # Fill document type field
                for sel in ["#txtDocType", "input[name*='DocType']", "input[id*='DocType']"]:
                    try:
                        await page.fill(sel, doc_type)
                        break
                    except Exception:
                        pass

                # Fill date range
                for sel in ["#txtFromDate", "input[name*='FromDate']", "input[id*='From']"]:
                    try:
                        await page.fill(sel, self._fmt_date(self.start_date))
                        break
                    except Exception:
                        pass
                for sel in ["#txtToDate", "input[name*='ToDate']", "input[id*='To']"]:
                    try:
                        await page.fill(sel, self._fmt_date(self.end_date))
                        break
                    except Exception:
                        pass

                # Submit search
                for sel in ["#btnSearch", "input[type='submit']", "button[type='submit']"]:
                    try:
                        await page.click(sel)
                        break
                    except Exception:
                        pass

                await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)

                # Collect results across pages
                while True:
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    page_records = self._parse_results_table(soup, doc_type, cat, cat_label)
                    records.extend(page_records)

                    # Try "Next" pagination
                    next_btn = soup.find("a", string=re.compile(r"Next|>", re.I))
                    if not next_btn:
                        break
                    # Click next via __doPostBack or href
                    href = next_btn.get("href", "")
                    if "__doPostBack" in href:
                        match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
                        if match:
                            et, ea = match.group(1), match.group(2)
                            await page.evaluate(f"__doPostBack('{et}','{ea}')")
                            await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)
                            continue
                    if href:
                        await page.click(f"a[href='{href}']")
                        await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)
                        continue
                    break

                log.info(f"  {doc_type}: found {len(records)} records")
                return records

            except PwTimeout:
                log.warning(f"  Timeout on {doc_type} (attempt {attempt+1})")
            except Exception as e:
                log.warning(f"  Error on {doc_type} attempt {attempt+1}: {e}")
            await asyncio.sleep(3)

        return records

    # ------------------------------------------------------------------
    def _parse_results_table(self, soup: BeautifulSoup, doc_type: str, cat: str, cat_label: str) -> list[dict]:
        records = []
        table = soup.find("table", id=re.compile(r"(grid|result|tbl)", re.I))
        if not table:
            # Try first data table
            tables = soup.find_all("table")
            for t in tables:
                if t.find("tr") and len(t.find_all("tr")) > 1:
                    table = t
                    break
        if not table:
            return records

        rows = table.find_all("tr")
        headers = []
        for row in rows:
            cells = row.find_all(["th", "td"])
            if not headers:
                if any(c.get("class") or c.name == "th" for c in cells):
                    headers = [c.get_text(strip=True).lower() for c in cells]
                    continue
            if len(cells) < 3:
                continue
            text = [c.get_text(strip=True) for c in cells]

            # Try to map columns
            def get(keys):
                for k in keys:
                    for i, h in enumerate(headers):
                        if k in h and i < len(text):
                            return text[i]
                # Fallback positional
                return ""

            doc_num  = get(["doc num", "document no", "docnum", "doc_num", "instrument"])
            filed    = get(["filed", "file date", "date", "record date"])
            grantor  = get(["grantor", "owner", "grantors"])
            grantee  = get(["grantee", "grantees"])
            legal    = get(["legal", "description"])
            amount   = get(["amount", "consideration", "value"])

            # Build clerk URL (try to find hyperlink in doc_num cell)
            clerk_url = ""
            for cell in cells:
                a = cell.find("a", href=True)
                if a:
                    href = a["href"]
                    clerk_url = href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")
                    break

            if not doc_num and not grantor:
                continue  # skip empty rows

            # Normalise filed date
            filed_norm = ""
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                try:
                    filed_norm = datetime.strptime(filed, fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass

            records.append({
                "doc_num":    doc_num,
                "doc_type":   doc_type,
                "filed":      filed_norm or filed,
                "cat":        cat,
                "cat_label":  cat_label,
                "owner":      grantor,
                "grantee":    grantee,
                "amount":     amount,
                "legal":      legal,
                "clerk_url":  clerk_url,
                # Address fields filled by parcel enrichment later
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

        return records

    # ------------------------------------------------------------------
    async def run(self) -> list[dict]:
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()

            for doc_type in TARGET_TYPES:
                log.info(f"Searching clerk for doc type: {doc_type}")
                recs = await self._search_doc_type(page, doc_type)
                self.results.extend(recs)
                await asyncio.sleep(1)   # polite delay

            await browser.close()

        return self.results


# ---------------------------------------------------------------------------
# Fallback: Static HTTP scraper (for environments without Playwright)
# ---------------------------------------------------------------------------
class StaticClerkScraper:
    """
    Uses requests + BeautifulSoup + __doPostBack POST for clerk portal.
    Less reliable than Playwright but works as fallback.
    """

    VIEWSTATE_RE = re.compile(r'id="__VIEWSTATE"\s+value="([^"]*)"')
    VSTGEN_RE    = re.compile(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"')
    EVENT_VAL_RE = re.compile(r'id="__EVENTVALIDATION"\s+value="([^"]*)"')

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date   = end_date
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; HarrisLeadBot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self.results: list[dict] = []

    def _fmt_date(self, dt: datetime) -> str:
        return dt.strftime("%m/%d/%Y")

    def _extract_hidden(self, html: str) -> dict:
        vs  = self.VIEWSTATE_RE.search(html)
        vsg = self.VSTGEN_RE.search(html)
        ev  = self.EVENT_VAL_RE.search(html)
        return {
            "__VIEWSTATE":          vs.group(1)  if vs  else "",
            "__VIEWSTATEGENERATOR": vsg.group(1) if vsg else "",
            "__EVENTVALIDATION":    ev.group(1)  if ev  else "",
        }

    def _search(self, doc_type: str) -> list[dict]:
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        records = []

        for attempt in range(3):
            try:
                r = self.session.get(CLERK_SEARCH_URL, timeout=30)
                hidden = self._extract_hidden(r.text)

                payload = {
                    **hidden,
                    "__EVENTTARGET":   "",
                    "__EVENTARGUMENT": "",
                    "ddlSearchType":   "DT",
                    "txtDocType":      doc_type,
                    "txtFromDate":     self._fmt_date(self.start_date),
                    "txtToDate":       self._fmt_date(self.end_date),
                    "btnSearch":       "Search",
                }

                r2 = self.session.post(CLERK_SEARCH_URL, data=payload, timeout=30)
                soup = BeautifulSoup(r2.text, "lxml")
                recs = ClerkScraper(self.start_date, self.end_date)._parse_results_table(
                    soup, doc_type, cat, cat_label
                )
                records.extend(recs)
                log.info(f"  {doc_type}: {len(recs)} records (static)")
                return records
            except Exception as e:
                log.warning(f"  Static scrape error {doc_type} attempt {attempt+1}: {e}")
                time.sleep(3)
        return records

    def run(self) -> list[dict]:
        for doc_type in TARGET_TYPES:
            recs = self._search(doc_type)
            self.results.extend(recs)
        return self.results


# ---------------------------------------------------------------------------
# GHL CSV Export
# ---------------------------------------------------------------------------
def export_ghl_csv(records: list[dict], path: str):
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
            # Split owner name
            parts = owner.strip().split()
            first = parts[0] if len(parts) >= 2 else ""
            last  = " ".join(parts[1:]) if len(parts) >= 2 else owner

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
    log.info(f"GHL CSV exported → {path}")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
async def main():
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    log.info(f"Harris County Motivated Seller Scraper")
    log.info(f"Date range: {start_date.date()} → {end_date.date()}")

    # ---- 1. Load HCAD parcel data ----------------------------------------
    parcel_loader = HCADParcelLoader()
    parcel_count  = parcel_loader.load()

    # ---- 2. Scrape clerk portal -------------------------------------------
    if PLAYWRIGHT_AVAILABLE:
        log.info("Using Playwright scraper")
        scraper = ClerkScraper(start_date, end_date)
        raw_records = await scraper.run()
    else:
        log.info("Playwright unavailable – using static HTTP scraper")
        static_scraper = StaticClerkScraper(start_date, end_date)
        raw_records = static_scraper.run()

    log.info(f"Total raw records from clerk: {len(raw_records)}")

    # ---- 3. Deduplicate by doc_num ----------------------------------------
    seen = set()
    deduped = []
    for rec in raw_records:
        key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(rec)
    log.info(f"After dedup: {len(deduped)} records")

    # ---- 4. Enrich with parcel data --------------------------------------
    with_address = 0
    for rec in deduped:
        parcel = parcel_loader.lookup_owner(rec.get("owner", ""))
        if parcel:
            rec.update(parcel)
            if parcel.get("prop_address"):
                with_address += 1

    log.info(f"Records with address: {with_address}")

    # ---- 5. Score & flag --------------------------------------------------
    for rec in deduped:
        flags, score = compute_flags_and_score(rec, deduped)
        rec["flags"] = flags
        rec["score"] = score

    # Sort by score desc
    deduped.sort(key=lambda r: r.get("score", 0), reverse=True)

    # ---- 6. Build output payload -----------------------------------------
    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Harris County Clerk (cclerk.hctx.net)",
        "date_range":    {"from": start_date.strftime("%Y-%m-%d"), "to": end_date.strftime("%Y-%m-%d")},
        "total":         len(deduped),
        "with_address":  with_address,
        "records":       deduped,
    }

    # ---- 7. Save JSON -------------------------------------------------------
    for out_path in ["dashboard/records.json", "data/records.json"]:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info(f"Saved → {out_path}")

    # ---- 8. GHL CSV export -----------------------------------------------
    today = datetime.now().strftime("%Y%m%d")
    export_ghl_csv(deduped, f"data/ghl_export_{today}.csv")

    log.info("Done.")
    return payload


if __name__ == "__main__":
    asyncio.run(main())
