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

    # HCAD public property detail URL pattern
    HCAD_DETAIL_URL = "https://public.hcad.org/records/details.asp?cession=A&acct="

    def __init__(self):
        self.lookup: dict = {}           # name → info (address dict)
        self.legal_index: dict = {}      # normalized_legal_key → info
        self.acct_index: dict = {}       # acct → info (for HCAD URL lookups)
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

    def _index_legal(self, lgl_1: str, lgl_2: str, lgl_3: str, info: dict):
        """Build a normalized legal key from HCAD lgl_1 + lgl_2 + lgl_3 fields
        and index it for fast lookup.

        HCAD format:
          lgl_1 = "LT 5 BLK 3"  (lot/block/tract)
          lgl_2 = "WILDHEATHER SEC 1"  (subdivision name, sometimes "(NM)")
          lgl_3 = additional info (sometimes holds the actual subdivision)
        """
        # Combine all legal fields
        combined = f"{lgl_1} {lgl_2} {lgl_3}".upper()
        combined = re.sub(r"[^\w\s]", " ", combined)
        combined = re.sub(r"\s+", " ", combined).strip()
        if not combined or len(combined) < 5:
            return

        # Extract subdivision name (from lgl_2, or lgl_3 if lgl_2 is short/empty)
        subdiv = lgl_2.strip()
        if subdiv.startswith("(") and subdiv.endswith(")"):
            subdiv = subdiv[1:-1]  # strip parens
        if len(subdiv) <= 3 or subdiv == "NM":
            subdiv = lgl_3.strip()
            if subdiv.startswith("(") and subdiv.endswith(")"):
                subdiv = subdiv[1:-1]

        subdiv_norm = re.sub(r"[^\w\s]", " ", subdiv.upper())
        subdiv_norm = re.sub(r"\s+", " ", subdiv_norm).strip()

        # Extract lot and block numbers from lgl_1
        lot_match = re.search(r"\bL(?:O?T|TS?)?\s*(\d+)\b", lgl_1.upper())
        blk_match = re.search(r"\bBL(?:OC)?K?\s*(\d+)\b", lgl_1.upper())

        lot_num = lot_match.group(1) if lot_match else ""
        blk_num = blk_match.group(1) if blk_match else ""

        # Build multiple index keys for flexible matching
        if subdiv_norm and lot_num and blk_num:
            key = f"{subdiv_norm}|LOT{lot_num}|BLK{blk_num}"
            self.legal_index[key] = info
        if subdiv_norm and lot_num:
            key2 = f"{subdiv_norm}|LOT{lot_num}"
            if key2 not in self.legal_index:
                self.legal_index[key2] = info
        if subdiv_norm and blk_num:
            key3 = f"{subdiv_norm}|BLK{blk_num}"
            if key3 not in self.legal_index:
                self.legal_index[key3] = info

    @staticmethod
    def _parse_clerk_legal(legal: str) -> dict:
        """Parse a clerk filing legal description into subdivision, lot, block.

        Clerk format: "WILDHEATHER SEC 1 | Sec: 1 | Lot: 5 | Block: 3"
        """
        result = {"subdiv": "", "lot": "", "block": "", "section": ""}
        if not legal:
            return result

        parts = [p.strip() for p in legal.split("|")]
        for part in parts:
            if part.lower().startswith("lot:"):
                result["lot"] = part.split(":", 1)[1].strip()
            elif part.lower().startswith("block:"):
                result["block"] = part.split(":", 1)[1].strip()
            elif part.lower().startswith("sec:"):
                result["section"] = part.split(":", 1)[1].strip()
            elif not result["subdiv"]:
                result["subdiv"] = part.strip()

        return result

    def lookup_by_legal(self, legal: str) -> Optional[dict]:
        """Try to match a clerk filing against the HCAD legal description index.

        Returns the info dict if matched, or None.
        """
        if not legal or not self.legal_index:
            return None

        parsed = self._parse_clerk_legal(legal)
        subdiv = parsed["subdiv"]
        lot = parsed["lot"]
        block = parsed["block"]

        if not subdiv:
            return None

        subdiv_norm = re.sub(r"[^\w\s]", " ", subdiv.upper())
        subdiv_norm = re.sub(r"\s+", " ", subdiv_norm).strip()

        # Extract just the lot/block numbers (strip leading zeros)
        lot_num = re.sub(r"^0+", "", lot) if lot else ""
        blk_num = re.sub(r"^0+", "", block) if block else ""

        # Try full match first (subdiv + lot + block)
        if subdiv_norm and lot_num and blk_num:
            key = f"{subdiv_norm}|LOT{lot_num}|BLK{blk_num}"
            if key in self.legal_index:
                return self.legal_index[key]

        # Try subdiv + lot only
        if subdiv_norm and lot_num:
            key = f"{subdiv_norm}|LOT{lot_num}"
            if key in self.legal_index:
                return self.legal_index[key]

        # Try subdiv + block only
        if subdiv_norm and blk_num:
            key = f"{subdiv_norm}|BLK{blk_num}"
            if key in self.legal_index:
                return self.legal_index[key]

        return None

    # Suffixes to strip from probate grantor names (only on retry, not first pass)
    PROBATE_SUFFIXES = re.compile(
        r"\b(EST|ESTATE|DECEASED|DECD|DEC D|DECEDENT)\s*$",
        re.IGNORECASE,
    )
    # Prefixes HCAD sometimes uses: "ESTATE OF JOHN SMITH"
    ESTATE_PREFIX_RE = re.compile(
        r"^(ESTATE\s+OF\s+|EST\s+OF\s+)",
        re.IGNORECASE,
    )

    @classmethod
    def _clean_probate_name(cls, name: str) -> str:
        """Strip trailing 'EST', 'ESTATE', 'DECEASED' etc. from a probate name.

        Example: 'BLUM MITCHELL AARON EST' → 'BLUM MITCHELL AARON'
        Does NOT strip leading 'ESTATE OF' — that's a valid HCAD format.
        """
        cleaned = cls.PROBATE_SUFFIXES.sub("", name).strip()
        cleaned = cleaned.rstrip(" ,;-")
        return cleaned if cleaned else name

    def _try_name_with_rotations(self, name: str) -> Optional[dict]:
        """Try exact/fuzzy match, then rotate word order to find HCAD match.

        Returns {"info": dict, "level": str} or None.
        """
        # First try exact/fuzzy via the standard pipeline
        result = self._lookup_owner_exact(name)
        if result:
            return result

        # Try rotating words: "A B C" → "C A B", "B C A"
        parts = self._norm(name).split()
        if len(parts) >= 2:
            for i in range(1, len(parts)):
                rotated = " ".join(parts[i:] + parts[:i])
                if rotated in self.lookup:
                    return {"info": self.lookup[rotated], "level": "medium"}

        return None

    def _try_grantor_name_variants(self, name: str) -> Optional[dict]:
        """Try multiple name variants for a single grantor against HCAD.

        Order: as-filed → "ESTATE OF name" → stripped (no EST/ESTATE suffix)
        At each step, also tries word rotations.
        Returns {"info": dict, "level": str} or None.
        """
        if not name:
            return None

        # Step A: Try name exactly as filed (e.g. "BLUM MITCHELL AARON EST")
        result = self._try_name_with_rotations(name)
        if result:
            return result

        # Step B: Try "ESTATE OF [clean name]"
        #   HCAD sometimes stores as "ESTATE OF ANDRES JOHN LEDAY SR"
        clean_name = self._clean_probate_name(name)
        base_name = self.ESTATE_PREFIX_RE.sub("", clean_name).strip()
        if base_name:
            estate_of = f"ESTATE OF {base_name}"
            result = self._try_name_with_rotations(estate_of)
            if result:
                return result

        # Step C: Try just the clean name (no EST/ESTATE suffix)
        if clean_name != name:
            result = self._try_name_with_rotations(clean_name)
            if result:
                return result

        return None

    def enrich_record(self, record: dict) -> dict:
        """Enrich a clerk record with HCAD data using tiered matching.

        Returns dict with address fields + match_confidence + hcad_url.
        Confidence levels:
          high   = legal description matched (subdivision + lot/block)
          medium = exact owner name matched
          low    = fuzzy/prefix owner name matched
          none   = no match found

        PROBATE LOGIC (cat == "PROBATE"):
          - Property Address = GRANTOR's address (the deceased person's property)
            Tries each grantor on HCAD until one matches.
          - Mail Address = GRANTEE's address (the heir/executor you contact)
            Tries each grantee on HCAD. If none found, sets "Grantee not found".
          - Tries each grantor name with variants: as-filed, "ESTATE OF", stripped.
        NON-PROBATE:
          - Standard: legal desc → owner name → fuzzy match.
        """
        legal = record.get("legal", "")
        owner = record.get("owner", "")
        cat   = record.get("cat", "")
        is_probate = cat == "PROBATE"

        # ── Tier 1: Legal description match (highest confidence, all types) ──
        result = self.lookup_by_legal(legal)
        if result:
            out = dict(result)
            out["match_confidence"] = "high"
            out["hcad_url"] = self._get_hcad_url(result)
            return out

        # ── PROBATE: separate grantor (property) and grantee (mail) lookups ──
        if is_probate:
            grantor_str = record.get("grantor_name", "") or ""
            grantee_str = record.get("grantee_names", "") or ""

            # Split into individual names (semicolon-separated)
            grantor_list = [g.strip() for g in grantor_str.split(";") if g.strip()]
            grantee_list = [g.strip() for g in grantee_str.split(";") if g.strip()]

            # ── Grantor lookup → Property Address ──
            # Try each grantor until one matches HCAD.
            # IMPORTANT: only accept "medium" (exact) confidence from bulk
            # data.  "low" (fuzzy/prefix) matches produce too many false
            # positives for probate grantors (title companies, partial
            # name collisions, etc.).  Low-confidence records will be
            # picked up by the HCAD ArcGIS API verification step instead.
            prop_result = None
            for grantor_name in grantor_list:
                candidate = self._try_grantor_name_variants(grantor_name)
                if candidate and candidate.get("level") in ("medium", "high"):
                    prop_result = candidate
                    break

            # ── Grantee lookup → Mail Address ──
            # Same rule: only accept medium+ confidence for grantees.
            mail_result = None
            for grantee_name in grantee_list:
                candidate = self._try_name_with_rotations(grantee_name)
                if candidate and candidate.get("level") in ("medium", "high"):
                    mail_result = candidate
                    break

            # Build the output
            out = {}
            if prop_result:
                prop_info = prop_result["info"]
                out["prop_address"] = prop_info.get("prop_address", "")
                out["prop_city"]    = prop_info.get("prop_city", "Houston")
                out["prop_state"]   = prop_info.get("prop_state", "TX")
                out["prop_zip"]     = prop_info.get("prop_zip", "")
                out["match_confidence"] = prop_result["level"]
                out["hcad_url"] = self._get_hcad_url(prop_info)
            else:
                out["match_confidence"] = "none"
                out["hcad_url"] = ""

            if mail_result:
                mail_info = mail_result["info"]
                out["mail_address"] = mail_info.get("prop_address", "") or mail_info.get("mail_address", "")
                out["mail_city"]    = mail_info.get("prop_city", "") or mail_info.get("mail_city", "")
                out["mail_state"]   = mail_info.get("prop_state", "") or mail_info.get("mail_state", "TX")
                out["mail_zip"]     = mail_info.get("prop_zip", "") or mail_info.get("mail_zip", "")
            else:
                out["mail_address"] = "Grantee not found"
                out["mail_city"]    = ""
                out["mail_state"]   = ""
                out["mail_zip"]     = ""

            return out

        # ── NON-PROBATE: standard name match + rotations ──
        if owner:
            result = self._try_name_with_rotations(owner)
            if result:
                out = dict(result["info"])
                out["match_confidence"] = result["level"]
                out["hcad_url"] = self._get_hcad_url(result["info"])
                return out

        # ── No match ──
        return {
            "match_confidence": "none",
            "hcad_url": "",
        }

    def _get_hcad_url(self, info: dict) -> str:
        """Build HCAD public detail URL from account number stored in info."""
        acct = info.get("_acct", "")
        if acct:
            return f"{self.HCAD_DETAIL_URL}{acct.strip()}"
        return ""

    def _lookup_owner_exact(self, name: str) -> Optional[dict]:
        """Try to match owner name, returning confidence level.

        Returns {"info": dict, "level": "medium"|"low"} or None.
        """
        if not name or not self.lookup:
            return None

        # Split semicolons – clerk records often list multiple grantees
        candidates = [c.strip() for c in name.split(";") if c.strip()]
        if not candidates:
            return None

        # First pass: skip LLCs
        for cand in candidates:
            if LLC_PATTERN.search(cand):
                continue
            result = self._try_match_with_level(cand)
            if result:
                return result

        # Second pass: try LLCs too
        for cand in candidates:
            result = self._try_match_with_level(cand)
            if result:
                return result

        return None

    def _try_match_with_level(self, name: str) -> Optional[dict]:
        """Attempt exact, reversed, and prefix match. Returns level."""
        n = self._norm(name)
        if not n:
            return None

        # 1. Exact match → medium confidence
        if n in self.lookup:
            return {"info": self.lookup[n], "level": "medium"}

        # 2. Try "FIRST LAST" → "LAST FIRST" → medium confidence
        parts = n.split()
        if len(parts) >= 2:
            reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
            if reversed_name in self.lookup:
                return {"info": self.lookup[reversed_name], "level": "medium"}

        # 3. Try just "LAST FIRST" (first two tokens) → low confidence
        if len(parts) >= 3:
            short = f"{parts[0]} {parts[1]}"
            if short in self.lookup:
                return {"info": self.lookup[short], "level": "low"}

        # 4. Prefix match → low confidence
        #    Both the search name AND the key must be ≥ 8 chars,
        #    and the shorter string must be at least 60% the length of
        #    the longer one to avoid wild mismatches (e.g. key="S"
        #    matching any name starting with S).
        if len(n) >= 8:
            for key, val in self.lookup.items():
                if len(key) < 8:
                    continue
                if key.startswith(n) or n.startswith(key):
                    shorter = min(len(n), len(key))
                    longer  = max(len(n), len(key))
                    if shorter / longer >= 0.6:
                        return {"info": val, "level": "low"}

        return None

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
        #    Both name and key must be >= 8 chars, and the shorter must
        #    be at least 60% the length of the longer to avoid wild
        #    mismatches (e.g. a key of "S" matching every S-name).
        if len(n) >= 8:
            for key, val in self.lookup.items():
                if len(key) < 8:
                    continue
                if key.startswith(n) or n.startswith(key):
                    shorter = min(len(n), len(key))
                    longer  = max(len(n), len(key))
                    if shorter / longer >= 0.6:
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

        Actual HCAD zip structure (confirmed April 2026):
        - real_acct.txt: PRIMARY file — has mailto (owner name), mail_addr_1,
          mail_city, mail_state, mail_zip, site_addr_1, site_addr_3 (city),
          plus acct (account number).  Contains ALL addresses.
        - owners.txt: SUPPLEMENT — has acct, name, aka.  No addresses, but
          provides additional owner name variants for matching.
        - deeds.txt: Deed records only (acct, dos, clerk_yr, clerk_id, deed_id).
          NOT useful for address enrichment.
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

                # ── Step 1: Parse real_acct.txt (primary — has all addresses) ──
                acct_to_info = {}  # {acct: address info dict}
                if "real_acct" in file_map:
                    ra_file = file_map["real_acct"]
                    log.info(f"  Parsing primary address data from: {ra_file}")
                    headers, delim, stream, sample_data = self._read_tabular(zf, ra_file)
                    if headers:
                        log.info(f"  Columns ({len(headers)}): {headers[:20]}{'...' if len(headers) > 20 else ''}")

                        # Column mapping for real_acct.txt
                        col_acct = self._find_column(headers, "ACCT", "ACCOUNT")
                        col_mailto = self._find_column(
                            headers, "MAILTO", "MAIL_TO", "OWN_NAME", "OWNER_NAME",
                            "OWNER", "NAME",
                        )
                        # Site / property address
                        col_site = self._find_column(
                            headers, "SITE_ADDR_1", "SITE_ADDRESS", "SITEADDR",
                            "SITE_ADDR", "PROP_ADDR",
                        )
                        col_site_city = self._find_column(
                            headers, "SITE_ADDR_3", "SITE_CITY",
                        )
                        # Mailing address
                        col_mail = self._find_column(
                            headers, "MAIL_ADDR_1", "MAIL_ADDRESS", "OWN_ADDR_1",
                        )
                        col_mail_city = self._find_column(
                            headers, "MAIL_CITY", "OWN_CITY",
                        )
                        col_mail_state = self._find_column(
                            headers, "MAIL_STATE", "OWN_STATE",
                        )
                        col_mail_zip = self._find_column(
                            headers, "MAIL_ZIP", "OWN_ZIP",
                        )
                        # Legal description columns
                        col_lgl1 = self._find_column(headers, "LGL_1", "LEGAL_1")
                        col_lgl2 = self._find_column(headers, "LGL_2", "LEGAL_2")
                        col_lgl3 = self._find_column(headers, "LGL_3", "LEGAL_3")

                        log.info(f"  Column mapping: acct={col_acct}, mailto={col_mailto}, "
                                 f"site={col_site}, site_city={col_site_city}, "
                                 f"mail={col_mail}, mail_city={col_mail_city}, "
                                 f"lgl_1={col_lgl1}, lgl_2={col_lgl2}, lgl_3={col_lgl3}")

                        if col_mailto is None and col_acct is None:
                            log.warning(f"  Cannot find mailto or acct column in real_acct.txt")
                        else:
                            legal_indexed = 0
                            for line in (sample_data + list(stream)):
                                fields = line.strip().split(delim)
                                mailto = safe_get(fields, col_mailto) if col_mailto is not None else ""
                                acct   = safe_get(fields, col_acct) if col_acct is not None else ""

                                if not mailto and not acct:
                                    continue

                                info = {
                                    "prop_address": safe_get(fields, col_site),
                                    "prop_city":    safe_get(fields, col_site_city) or "Houston",
                                    "prop_state":   "TX",
                                    "prop_zip":     "",
                                    "mail_address": safe_get(fields, col_mail),
                                    "mail_city":    safe_get(fields, col_mail_city),
                                    "mail_state":   safe_get(fields, col_mail_state) or "TX",
                                    "mail_zip":     safe_get(fields, col_mail_zip),
                                    "_acct":        acct.strip(),
                                }

                                # Index by mailto name (primary owner match)
                                if mailto:
                                    self._index(mailto, info)
                                    count += 1

                                # Index by legal description for parcel-level matching
                                lgl_1 = safe_get(fields, col_lgl1) if col_lgl1 is not None else ""
                                lgl_2 = safe_get(fields, col_lgl2) if col_lgl2 is not None else ""
                                lgl_3 = safe_get(fields, col_lgl3) if col_lgl3 is not None else ""
                                if lgl_1 or lgl_2:
                                    self._index_legal(lgl_1, lgl_2, lgl_3, info)
                                    legal_indexed += 1

                                # Store by account for owners.txt cross-reference
                                # and for HCAD URL generation
                                if acct:
                                    acct_to_info[acct.strip()] = info
                                    self.acct_index[acct.strip()] = info

                            log.info(f"  real_acct.txt: indexed {count:,} owner names, "
                                     f"{len(acct_to_info):,} account records, "
                                     f"{legal_indexed:,} legal descriptions, "
                                     f"{len(self.legal_index):,} unique legal keys")

                # ── Step 2: Parse owners.txt (supplement — extra name variants) ──
                extra = 0
                if "owners" in file_map and acct_to_info:
                    ow_file = file_map["owners"]
                    log.info(f"  Parsing supplemental owner names from: {ow_file}")
                    headers, delim, stream, sample_data = self._read_tabular(zf, ow_file)
                    if headers:
                        log.info(f"  Columns ({len(headers)}): {headers}")
                        col_acct = self._find_column(headers, "ACCT", "ACCOUNT")
                        col_name = self._find_column(
                            headers, "NAME", "OWN_NAME", "OWNER_NAME", "OWNER",
                        )

                        if col_acct is not None and col_name is not None:
                            for line in (sample_data + list(stream)):
                                fields = line.strip().split(delim)
                                acct = safe_get(fields, col_acct).strip()
                                name = safe_get(fields, col_name)
                                if not acct or not name:
                                    continue

                                # Look up address info from real_acct via account
                                info = acct_to_info.get(acct)
                                if info:
                                    norm = self._norm(name)
                                    if norm and norm not in self.lookup:
                                        self._index(name, info)
                                        extra += 1

                    log.info(f"  owners.txt: added {extra:,} supplemental name variants")
                    count += extra

                # ── Fallback: try a single combined file ──────────────────
                if count == 0:
                    log.warning("  real_acct.txt + owners.txt approach yielded 0 records")
                    for basename in ["real_acct_owner", "real_acct_owner_2026",
                                     "real_acct_owner_2025", "real_acct_owner_2024"]:
                        if basename in file_map:
                            log.info(f"  Trying combined file: {file_map[basename]}")
                            count = self._parse_single_file(zf, file_map[basename])
                            if count > 0:
                                break

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
            headers, "MAILTO", "OWN_NAME", "OWNER_NAME", "OWNER", "NAME",
        )
        col_site_addr = self._find_column(
            headers, "SITE_ADDR_1", "SITEADDR", "SITE_ADDR",
            "SITE_ADDRESS", "PROP_ADDR",
        )
        col_site_city = self._find_column(headers, "SITE_ADDR_3", "SITE_CITY")
        col_mail_addr = self._find_column(
            headers, "MAIL_ADDR_1", "MAIL_ADDRESS", "OWN_ADDR_1",
        )
        col_mail_city = self._find_column(headers, "MAIL_CITY", "OWN_CITY")
        col_mail_state = self._find_column(headers, "MAIL_STATE", "OWN_STATE")
        col_mail_zip = self._find_column(headers, "MAIL_ZIP", "OWN_ZIP")

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
                "prop_zip":     "",
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
#  HCAD Live Search  (ArcGIS REST API — probate verification only)
# ═══════════════════════════════════════════════════════════════════════════════
HCAD_ARCGIS_URL = (
    "https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0/query"
)
HCAD_DETAIL_BASE = "https://public.hcad.org/records/details.asp?cession=A&acct="


def hcad_api_search(session: requests.Session, name: str) -> Optional[dict]:
    """Search HCAD via the ArcGIS REST API by owner name.

    Uses a simple HTTP GET → JSON response.  No Playwright / WebSocket needed.
    Address fields are split in the API (site_str_num, site_str_name, etc.)
    so we reassemble them here.
    Returns dict with acct, owner_name, street, city, state, zip or None.
    """
    if not name or not name.strip():
        return None

    # Sanitise for SQL LIKE clause (escape single quotes)
    safe = name.strip().upper().replace("'", "''")

    # Search ALL three owner name fields — deceased owners are often
    # listed as owner_name_2 or _3 when co-owned with a spouse/trust.
    where = (
        f"upper(owner_name_1) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_2) LIKE upper('%{safe}%') OR "
        f"upper(owner_name_3) LIKE upper('%{safe}%')"
    )

    params = {
        "where":              where,
        "outFields":          ("HCAD_NUM,owner_name_1,owner_name_2,owner_name_3,"
                               "site_str_num,site_str_name,site_str_sfx,"
                               "site_str_pfx,site_str_sfx_dir,"
                               "site_city,site_zip,state_class"),
        "returnGeometry":     "false",
        "f":                  "json",
        "resultRecordCount":  "10",
    }

    try:
        resp = session.get(HCAD_ARCGIS_URL, params=params, timeout=30)
        if resp.status_code != 200:
            log.warning(f"    HCAD API HTTP {resp.status_code} for '{name}'")
            return None

        data = resp.json()
        if "error" in data:
            log.warning(f"    HCAD API error response for '{name}': {data['error']}")
            return None

        features = data.get("features", [])
        if not features:
            log.info(f"    HCAD API: no results for '{name}'")
            return None

        # Reassemble split address fields into a street string
        # e.g. site_str_num=1302, site_str_name="RUDEL", site_str_sfx="DR"
        #    → "1302 RUDEL DR"
        best = None
        for feat in features:
            attr = feat.get("attributes", {})
            acct  = str(attr.get("HCAD_NUM", "")).strip()
            owner = (attr.get("owner_name_1") or "").strip()

            num   = str(attr.get("site_str_num") or "").strip()
            pfx   = (attr.get("site_str_pfx") or "").strip()
            sname = (attr.get("site_str_name") or "").strip()
            sfx   = (attr.get("site_str_sfx") or "").strip()
            sdir  = (attr.get("site_str_sfx_dir") or "").strip()
            city  = (attr.get("site_city") or "").strip()
            zipcd = (attr.get("site_zip") or "").strip()
            sclass = (attr.get("state_class") or "").strip()

            # Build street: "1302 RUDEL DR"
            parts = [p for p in [pfx, num, sname, sfx, sdir] if p and p != "0"]
            street = " ".join(parts)

            if not acct or not street:
                continue

            result = {
                "acct": acct, "owner_name": owner,
                "street": street, "city": city or "Houston",
                "state": "TX", "zip": zipcd,
            }

            # Prefer residential over commercial
            if sclass and sclass.startswith(("A", "B")):
                log.info(f"    HCAD API: found residential '{owner}' → {street}, {city} {zipcd}")
                return result

            if best is None:
                best = result

        if best:
            log.info(f"    HCAD API: found '{best['owner_name']}' → {best['street']}, {best['city']} {best['zip']}")
        else:
            log.info(f"    HCAD API: results but no usable address for '{name}'")
        return best

    except Exception as e:
        log.warning(f"    HCAD API error for '{name}': {e}")
        return None


def parse_hcad_address(address_str: str) -> dict:
    """Parse an HCAD address string like '1302 RUDEL DR, TOMBALL, TX 77375'
    into structured fields.  (Kept for backwards compat but the API
    version now returns pre-parsed fields.)"""
    parts = [p.strip() for p in address_str.split(",")]
    result = {"street": "", "city": "Houston", "state": "TX", "zip": ""}

    if len(parts) >= 1:
        result["street"] = parts[0]
    if len(parts) >= 2:
        result["city"] = parts[1]
    if len(parts) >= 3:
        # Last part might be "TX 77375" or just "TX"
        state_zip = parts[2].strip().split()
        if state_zip:
            result["state"] = state_zip[0]
        if len(state_zip) >= 2:
            result["zip"] = state_zip[1]

    return result


def verify_probate_via_hcad(http_session: requests.Session,
                            probate_records: list) -> int:
    """Run live HCAD API searches for probate records with no/low confidence.

    For each qualifying probate record:
      - Tries each grantor name variant via the ArcGIS REST API
      - If found, updates prop_address with the HCAD result
      - Also tries each grantee for mailing address

    Returns count of records improved.
    """
    to_verify = [r for r in probate_records
                 if r.get("cat") == "PROBATE"
                 and r.get("match_confidence") in ("none", "low")]

    if not to_verify:
        log.info("No probate records need HCAD live verification")
        return 0

    log.info(f"HCAD live verification: {len(to_verify)} probate records to check")
    improved = 0
    deadline = time.time() + 15 * 60  # 15-minute time cap

    for rec in to_verify:
        # ── Time cap: stop if we've been running too long ──
        if time.time() > deadline:
            remaining = len(to_verify) - to_verify.index(rec)
            log.warning(f"HCAD live verification: 15-min time cap reached, "
                        f"skipping remaining {remaining} records")
            break

        grantor_str = rec.get("grantor_name", "") or ""
        grantee_str = rec.get("grantee_names", "") or ""
        grantor_list = [g.strip() for g in grantor_str.split(";") if g.strip()]
        grantee_list = [g.strip() for g in grantee_str.split(";") if g.strip()]

        # ── Search for grantor's property (the house to buy) ──
        #    Variants: as-filed name, then cleaned (no EST/ESTATE).
        #    "ESTATE OF" variant removed — HCAD never stores names
        #    that way, so it always returns zero results.
        prop_found = False
        for gname in grantor_list:
            variants = [gname]
            cleaned = HCADParcelLoader._clean_probate_name(gname)
            if cleaned != gname:
                variants.append(cleaned)

            for variant in variants:
                result = hcad_api_search(http_session, variant)
                if result:
                    rec["prop_address"] = result["street"]
                    rec["prop_city"] = result["city"]
                    rec["prop_state"] = result["state"]
                    rec["prop_zip"] = result["zip"]
                    rec["match_confidence"] = "medium"
                    rec["hcad_url"] = f"{HCAD_DETAIL_BASE}{result['acct']}"
                    prop_found = True
                    improved += 1
                    break
                time.sleep(0.25)  # gentle rate limit

            if prop_found:
                break

        # ── Search for grantee's address (where the heir lives) ──
        if not rec.get("mail_address") or rec.get("mail_address") == "Grantee not found":
            for gname in grantee_list:
                result = hcad_api_search(http_session, gname)
                if result:
                    rec["mail_address"] = result["street"]
                    rec["mail_city"] = result["city"]
                    rec["mail_state"] = result["state"]
                    rec["mail_zip"] = result["zip"]
                    break
                time.sleep(0.25)

        time.sleep(0.15)  # rate limit between records

    log.info(f"HCAD live verification: improved {improved} of {len(to_verify)} records")
    return improved


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

                # Grantor / Grantee names — capture ALL of each
                grantors = []
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
                        elif row_text.lower().startswith("grantor"):
                            grantors.append(name_text)
                    ctrl_idx += 1
                    if ctrl_idx > 20:
                        break

                grantor = grantors[0] if grantors else ""
                owner = "; ".join(grantees) if grantees else grantor

                # Store ALL grantors and grantees for probate enrichment
                # Probate: grantors = deceased/filing parties, grantees = heirs/executors
                grantor_name = "; ".join(grantors) if grantors else ""
                grantee_names = "; ".join(grantees) if grantees else ""

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
                    "doc_num":           file_num,
                    "doc_type":          doc_type,
                    "filed":             filed_norm or file_date,
                    "cat":               cat,
                    "cat_label":         cat_label,
                    "owner":             owner,
                    "grantor_name":      grantor_name,
                    "grantee_names":     grantee_names,
                    "amount":            "",
                    "legal":             legal,
                    "clerk_url":         clerk_url,
                    "prop_address":      "",
                    "prop_city":         "Houston",
                    "prop_state":        "TX",
                    "prop_zip":          "",
                    "mail_address":      "",
                    "mail_city":         "",
                    "mail_state":        "TX",
                    "mail_zip":          "",
                    "match_confidence":  "none",
                    "hcad_url":          "",
                    "flags":             [],
                    "score":             0,
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
        "Grantor (Deceased/Filing Party)", "Grantee (Heirs/Applicants)",
        "Match Confidence", "HCAD URL",
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
                "Grantor (Deceased/Filing Party)": rec.get("grantor_name", ""),
                "Grantee (Heirs/Applicants)":      rec.get("grantee_names", ""),
                "Match Confidence":       rec.get("match_confidence", ""),
                "HCAD URL":              rec.get("hcad_url", ""),
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

    # 4. Enrich with HCAD addresses (tiered: legal desc → exact name → fuzzy name)
    #    For PROBATE: grantor → prop_address, grantee → mail_address
    with_address = 0
    confidence_counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for rec in deduped:
        enrichment = parcel.enrich_record(rec)
        confidence = enrichment.pop("match_confidence", "none")
        hcad_url   = enrichment.pop("hcad_url", "")

        # Remove internal _acct field from address data before merging
        enrichment.pop("_acct", None)

        rec["match_confidence"] = confidence
        rec["hcad_url"] = hcad_url

        # Always merge enrichment — for probate cases this includes
        # mail_address="Grantee not found" even when confidence is "none"
        if enrichment:
            rec.update(enrichment)
        if rec.get("prop_address", "").strip():
            with_address += 1

        confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1

    log.info(f"HCAD bulk enrichment: {with_address} with address")
    log.info(f"  Confidence: high={confidence_counts['high']}, "
             f"medium={confidence_counts['medium']}, "
             f"low={confidence_counts['low']}, "
             f"none={confidence_counts['none']}")

    # 4b. HCAD LIVE VERIFICATION — probate cases only
    #     For probate records with no/low confidence, query the HCAD
    #     ArcGIS REST API to find the grantor's property address.
    #     This is a simple HTTP GET → JSON call (no Playwright needed).
    probate_needing_verify = [r for r in deduped
                              if r.get("cat") == "PROBATE"
                              and r.get("match_confidence") in ("none", "low")]
    if probate_needing_verify:
        log.info(f"Starting HCAD live verification for {len(probate_needing_verify)} probate records...")
        try:
            api_session = requests.Session()
            api_session.headers.update({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120",
            })
            improved = verify_probate_via_hcad(api_session, deduped)

            # Recount addresses after live verification
            with_address = sum(1 for r in deduped if r.get("prop_address", "").strip())
        except Exception as e:
            log.warning(f"HCAD live verification failed: {e}")
    else:
        log.info("All probate records already have medium/high confidence — skipping live verification")

    # 4c. Set "Not found" for probate records with no property address
    #     After both bulk enrichment AND live API verification, any
    #     probate record that still has no property address should
    #     display "Not found" rather than being blank (or worse,
    #     showing a wrong address from a bad match).
    for rec in deduped:
        if rec.get("cat") == "PROBATE" and not rec.get("prop_address", "").strip():
            rec["prop_address"] = "Not found"

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
