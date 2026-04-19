"""One-time import of records.json + foreclosures.json into the Supabase records table.

Run once to seed the database with existing data. Safe to re-run: uses
upsert on doc_num, so it won't create duplicates. Later scraper runs will
keep data fresh via dual-write.

Usage:
    pip install -r supabase/requirements.txt
    cp supabase/.env.example supabase/.env   # then fill in secrets
    python supabase/import_data.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "supabase" / ".env")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    print("ERROR: set SUPABASE_URL and SUPABASE_SECRET_KEY in supabase/.env", file=sys.stderr)
    sys.exit(1)

DASHBOARD_DIR = ROOT / "dashboard"
RECORDS_JSON = DASHBOARD_DIR / "records.json"
FORECLOSURES_JSON = DASHBOARD_DIR / "foreclosures.json"

BATCH_SIZE = 500


def parse_date(value: str | None) -> str | None:
    """Parse a date string to ISO (YYYY-MM-DD). Accepts ISO or MM/DD/YYYY."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def clean(value):
    """Empty strings → None. Everything else passes through."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def normalize_clerk(r: dict) -> dict:
    """Map a records.json record to the unified records schema."""
    return {
        "doc_num": r["doc_num"],
        "doc_type": clean(r.get("doc_type")),
        "cat": clean(r.get("cat")),
        "cat_label": clean(r.get("cat_label")),
        "county": "Harris",
        "state": "TX",
        "filed_date": parse_date(r.get("filed")),
        "owner": clean(r.get("owner")),
        "grantor_name": clean(r.get("grantor_name")),
        "grantee_names": clean(r.get("grantee_names")),
        "prop_address": clean(r.get("prop_address")),
        "prop_city": clean(r.get("prop_city")),
        "prop_state": clean(r.get("prop_state")) or "TX",
        "prop_zip": clean(r.get("prop_zip")),
        "legal_description": clean(r.get("legal")),
        "amount": clean(r.get("amount")),
        "mail_address": clean(r.get("mail_address")),
        "mail_city": clean(r.get("mail_city")),
        "mail_state": clean(r.get("mail_state")) or "TX",
        "mail_zip": clean(r.get("mail_zip")),
        "clerk_url": clean(r.get("clerk_url")),
        "hcad_url": clean(r.get("hcad_url")),
        "match_confidence": clean(r.get("match_confidence")),
        "address_source": clean(r.get("address_source")),
        "flags": r.get("flags") or [],
        "score": r.get("score"),
    }


def normalize_foreclosure(r: dict) -> dict:
    """Map a foreclosures.json record to the unified records schema."""
    return {
        "doc_num": r["doc_id"],
        "doc_type": clean(r.get("doc_type")) or "FRCL",
        "cat": clean(r.get("cat")),
        "cat_label": clean(r.get("cat_label")),
        "county": "Harris",
        "state": "TX",
        "filed_date": parse_date(r.get("file_date")),
        "sale_date": parse_date(r.get("sale_date")),
        "grantor_name": clean(r.get("grantor")),
        "co_borrower": clean(r.get("co_borrower")),
        "mortgagee": clean(r.get("mortgagee")),
        "amount": clean(r.get("amount")),
        "prop_address": clean(r.get("prop_address")),
        "prop_city": clean(r.get("prop_city")),
        "prop_state": clean(r.get("prop_state")) or "TX",
        "prop_zip": clean(r.get("prop_zip")),
        "legal_description": clean(r.get("legal_description")),
        "clerk_url": clean(r.get("clerk_url")),
        "hcad_url": clean(r.get("hcad_url")),
        "match_confidence": clean(r.get("match_confidence")),
        "batch_id": clean(r.get("batch_id")),
        "batch_label": clean(r.get("batch_label")),
        "date_scraped": parse_date(r.get("date_scraped")),
        "needs_ocr": bool(r.get("needs_ocr", False)),
        "flags": r.get("flags") or [],
    }


def main() -> None:
    client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

    if not RECORDS_JSON.exists():
        print(f"ERROR: {RECORDS_JSON} not found", file=sys.stderr)
        sys.exit(1)
    if not FORECLOSURES_JSON.exists():
        print(f"ERROR: {FORECLOSURES_JSON} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {RECORDS_JSON.name}...")
    clerk_data = json.loads(RECORDS_JSON.read_text(encoding="utf-8"))
    clerk_records = [normalize_clerk(r) for r in clerk_data.get("records", [])]
    print(f"  {len(clerk_records):,} clerk records")

    print(f"Reading {FORECLOSURES_JSON.name}...")
    foreclosure_data = json.loads(FORECLOSURES_JSON.read_text(encoding="utf-8"))
    foreclosure_records = [normalize_foreclosure(r) for r in foreclosure_data.get("records", [])]
    print(f"  {len(foreclosure_records):,} foreclosure records")

    combined = clerk_records + foreclosure_records

    seen: set[str] = set()
    deduped: list[dict] = []
    for rec in combined:
        if rec["doc_num"] in seen:
            continue
        seen.add(rec["doc_num"])
        deduped.append(rec)
    print(f"\nTotal unique records: {len(deduped):,}")

    total = len(deduped)
    inserted = 0
    for i in range(0, total, BATCH_SIZE):
        batch = deduped[i:i + BATCH_SIZE]
        client.table("records").upsert(batch, on_conflict="doc_num").execute()
        inserted += len(batch)
        print(f"  Upserted {inserted:,} / {total:,}")

    print("\nDone.")

    result = client.table("records").select("id", count="exact").execute()
    print(f"Records now in database: {result.count:,}")


if __name__ == "__main__":
    main()
