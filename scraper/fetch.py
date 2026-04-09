"""
Harris County Motivated Seller Lead Scraper
Portal: https://cclerk.hctx.net/applications/websearch/RP.aspx
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
HCAD_BULK_BASE   = "https://pdata.hcad.org/download/"

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


class HCADParcelLoader:
    FALLBACK_YEARS = [2024, 2023, 2025]

    def __init__(self):
        self.lookup: dict = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _norm(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _index(self, name: str, info: dict):
        if not name:
            return
        n = self._norm(name)
        self.lookup[n] = info
        if "," in n:
            parts = [p.strip() for p in n.split(",", 1)]
            self.lookup[f"{parts[1]} {parts[0]}"] = info

    def lookup_owner(self, name: str) -> Optional[dict]:
        if not name:
            return None
        n = self._norm(name)
        if n in self.lookup:
            return self.lookup[n]
        parts = n.split()
        if parts:
            for key in self.lookup:
                if key.startswith(parts[0]):
                    return self.lookup[key]
        return None

    def _download(self, url: str) -> Optional[bytes]:
        for attempt in range(3):
            try:
                log.info(f"  Downloading {url} (attempt {attempt+1})")
                r = self.session.get(url, timeout=120, stream=True)
                if r.status_code == 200:
                    return r.content
            except Exception as e:
                log.warning(f"  Download error: {e}")
            time.sleep(3)
        return None

    def _parse_csv_zip(self, data: bytes) -> int:
        count = 0
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_files:
                    return 0
                with zf.open(csv_files[0]) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"))
                    for row in reader:
                        owner = (row.get("OWN1") or row.get("OWNER") or "").strip()
                        info = {
                            "prop_address": (row.get("SITEADDR") or row.get("SITE_ADDR") or "").strip(),
                            "prop_city":    (row.get("SITE_CITY") or "Houston").strip(),
                            "prop_state":   "TX",
                            "prop_zip":     (row.get("SITE_ZIP") or "").strip(),
                            "mail_address": (row.get("MAILADR1") or row.get("ADDR_1") or "").strip(),
                            "mail_city":    (row.get("MAILCITY") or row.get("CITY") or "").strip(),
                            "mail_state":   (row.get("STATE") or "TX").strip(),
                            "mail_zip":     (row.get("MAILZIP") or row.get("ZIP") or "").strip(),
                        }
                        self._index(owner, info)
                        count += 1
        except Exception as e:
            log.warning(f"  CSV parse error: {e}")
        return count

    def load(self) -> int:
        log.info("Loading HCAD parcel data...")
        for year in self.FALLBACK_YEARS:
            url = f"{HCAD_BULK_BASE}{year}/Real_acct_owner.zip"
            data = self._download(url)
            if data:
                n = self._parse_csv_zip(data)
                if n > 0:
                    log.info(f"  Loaded {n:,} parcels ({year})")
                    return n
        log.warning("  Could not load HCAD data. Address enrichment skipped.")
        return 0


class ClerkScraper:
    NAV_TIMEOUT    = 60_000
    SEARCH_TIMEOUT = 60_000

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date   = end_date
        self.results: list = []

    def _fmt(self, dt: datetime) -> str:
        return dt.strftime("%m/%d/%Y")

    async def _search_one(self, page, doc_type: str) -> list:
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
                    timeout=self.NAV_TIMEOUT
                )

                await page.evaluate(f"""
                    document.querySelectorAll('[id*="txtInstrument"]')[0].value = '{doc_type}';
                    document.querySelectorAll('[id*="txtFrom"]')[0].value = '{self._fmt(self.start_date)}';
                    document.querySelectorAll('[id*="txtTo"]')[0].value = '{self._fmt(self.end_date)}';
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

                    next_btn = page.locator("input[value='NEXT']")
                    if await next_btn.count() == 0:
                        break
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=self.SEARCH_TIMEOUT)

                log.info(f"  {doc_type}: {len(records)} total records")
                return records

            except Exception as e:
                log.warning(f"  {doc_type} attempt {attempt+1} error: {e}")
                await asyncio.sleep(3)

        return records

    def _parse_results(self, soup: BeautifulSoup, doc_type: str, cat: str, cat_label: str) -> list:
        records = []

        # Find all file number spans
        file_spans = soup.find_all("span", id=re.compile(r"ListViewl_ctrl\d+_lblFileNo", re.IGNORECASE))
        if not file_spans:
            file_spans = [s for s in soup.find_all("span") if re.match(r"RP-\d+", s.get_text(strip=True))]

        log.info(f"    Found {len(file_spans)} file number spans")

        for span in file_spans:
            try:
                file_num = span.get_text(strip=True)
                if not file_num.startswith("RP-"):
                    continue

                span_id = span.get("id", "")
                ctrl_match = re.search(r"(ListViewl_ctrl\d+)", span_id, re.IGNORECASE)
                ctrl_prefix = ctrl_match.group(1) if ctrl_match else ""

                def find_text(pattern):
                    el = soup.find("span", id=re.compile(pattern, re.IGNORECASE))
                    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""

                # File date
                file_date = find_text(ctrl_prefix + r"_lblFileDate")

                # Get ALL name spans for this specific record only
                # Confirmed pattern: ListViewl_ctrl{N}_lvOR_ctrl{M}_lblNames
                # ctrl0 = Grantor, ctrl1 = first Grantee (owner)
                record_name_spans = soup.find_all(
                    "span",
                    id=re.compile(
                        r"^.*?" + re.escape(ctrl_prefix) + r"_lvOR_ctrl\d+_lblNames$",
                        re.IGNORECASE
                    )
                )

                grantor = ""
                owner   = ""

                if record_name_spans:
                    grantor = record_name_spans[0].get_text(strip=True)
                    if len(record_name_spans) >= 2:
                        owner = record_name_spans[1].get_text(strip=True)
                    else:
                        owner = grantor

                # Legal description
                subdiv_el  = soup.find("span", id=re.compile(
                    r"^.*?" + re.escape(ctrl_prefix) + r"_lvLegal_ctrl\d+_lblSubDivAdd$",
                    re.IGNORECASE
                ))
                section_el = soup.find("span", id=re.compile(
                    r"^.*?" + re.escape(ctrl_prefix) + r"_lvLegal_ctrl\d+_lblSection$",
                    re.IGNORECASE
                ))
                lot_el     = soup.find("span", id=re.compile(
                    r"^.*?" + re.escape(ctrl_prefix) + r"_lvLegal_ctrl\d+_lblLot$",
                    re.IGNORECASE
                ))
                block_el   = soup.find("span", id=re.compile(
                    r"^.*?" + re.escape(ctrl_prefix) + r"_lvLegal_ctrl\d+_lblBlock$",
                    re.IGNORECASE
                ))

                subdiv  = subdiv_el.get_text(strip=True)  if subdiv_el  else ""
                section = section_el.get_text(strip=True) if section_el else ""
                lot     = lot_el.get_text(strip=True)     if lot_el     else ""
                block   = block_el.get_text(strip=True)   if block_el   else ""

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
            ctx  = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()

            for doc_type in TARGET_TYPES:
                log.info(f"Searching: {doc_type}")
                recs = await self._search_one(page, doc_type)
                self.results.extend(recs)
                await asyncio.sleep(2)

            await browser.close()

        return self.results


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
            owner  = rec.get("owner", "") or ""
            parts  = owner.strip().split()
            first  = parts[0] if len(parts) >= 2 else ""
            last   = " ".join(parts[1:]) if len(parts) >= 2 else owner
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


async def main():
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    log.info("Harris County Motivated Seller Scraper")
    log.info(f"Date range: {start_date.date()} to {end_date.date()}")

    parcel = HCADParcelLoader()
    parcel.load()

    scraper = ClerkScraper(start_date, end_date)
    raw     = await scraper.run()
    log.info(f"Raw records: {len(raw)}")

    seen, deduped = set(), []
    for rec in raw:
        key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(rec)
    log.info(f"After dedup: {len(deduped)}")

    with_address = 0
    for rec in deduped:
        p = parcel.lookup_owner(rec.get("owner", ""))
        if p:
            rec.update(p)
            if p.get("prop_address"):
                with_address += 1

    for rec in deduped:
        flags, score = compute_flags_and_score(rec, deduped)
        rec["flags"] = flags
        rec["score"] = score

    deduped.sort(key=lambda r: r.get("score", 0), reverse=True)

    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Harris County Clerk (cclerk.hctx.net)",
        "date_range":   {"from": start_date.strftime("%Y-%m-%d"), "to": end_date.strftime("%Y-%m-%d")},
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
