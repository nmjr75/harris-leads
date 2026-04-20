"""SIFTstack queue worker — MVP (HCAD enrichment only).

Runs in GitHub Actions (.github/workflows/siftstack-worker.yml) on a cron.
Pulls pending rows from siftstack_queue, looks up each record in HCAD by
property address, and upserts what it finds into the property_data table.

Scope (MVP):
  - Only HCAD property lookup. No obituary / people search / Smarty / DataSift.
  - Address-first lookup. Records without prop_address are marked failed.
  - One row at a time, sequential. No parallelism.

The workflow checks out the private nmjr75/SiftStack repo to ./SiftStack
and sets SIFTSTACK_SRC=./SiftStack/src so we can import its HCAD code.

Environment variables:
    SUPABASE_URL           — https://xxx.supabase.co
    SUPABASE_SECRET_KEY    — service-role key (bypasses RLS)
    SIFTSTACK_SRC          — path to SiftStack src/ (default ./SiftStack/src)
    SIFTSTACK_BATCH_SIZE   — rows per run (default 25)

Exit codes:
    0 — ran to completion (even if some rows failed; per-row errors stored in DB)
    1 — fatal: missing env vars, Supabase connect failure, SiftStack import failure
"""
from __future__ import annotations

import logging
import os
import sys
import traceback
from pathlib import Path

log = logging.getLogger("siftstack_worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ── Env ────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
SIFTSTACK_SRC = os.environ.get("SIFTSTACK_SRC", "./SiftStack/src").strip()
BATCH_SIZE = int(os.environ.get("SIFTSTACK_BATCH_SIZE", "25"))

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY — cannot run")
    sys.exit(1)


# ── Wire SiftStack into PYTHONPATH before first import ────────────────
siftstack_path = Path(SIFTSTACK_SRC).resolve()
if not siftstack_path.is_dir():
    log.error("SIFTSTACK_SRC does not exist: %s", siftstack_path)
    sys.exit(1)
sys.path.insert(0, str(siftstack_path))
log.info("Added SiftStack src to PYTHONPATH: %s", siftstack_path)


# ── Imports (after PYTHONPATH fixup) ──────────────────────────────────
try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed — pip install supabase")
    sys.exit(1)

try:
    # Only pull the raw HCAD lookup function — avoids NoticeData coupling.
    from harris.hcad_enricher import _hcad_search_by_address  # type: ignore
except Exception as e:
    log.error("Failed to import SiftStack HCAD module: %s", e)
    log.error("Make sure SIFTSTACK_SRC points to SiftStack/src/")
    sys.exit(1)


# ── Supabase client ────────────────────────────────────────────────────
try:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    log.error("Supabase client init failed: %s", e)
    sys.exit(1)


# ── Value coercion ─────────────────────────────────────────────────────
def _to_int(v):
    if v in (None, ""):
        return None
    try:
        return int(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _to_float(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


# ── DB helpers ─────────────────────────────────────────────────────────
def fetch_pending(n: int) -> list[dict]:
    """Fetch pending queue rows joined with the records table."""
    # PostgREST resource embedding: records!inner lets us join on FK.
    resp = (
        client.table("siftstack_queue")
        .select(
            "id, record_id, status, "
            "records!inner(id, doc_num, prop_address, prop_city, prop_state, "
            "prop_zip, parcel_acct, county, state)"
        )
        .eq("status", "pending")
        .order("queued_at", desc=False)
        .limit(n)
        .execute()
    )
    return resp.data or []


def mark_processing(queue_id: int) -> None:
    client.table("siftstack_queue").update({"status": "processing"}).eq(
        "id", queue_id
    ).execute()


def mark_completed(queue_id: int) -> None:
    client.table("siftstack_queue").update({"status": "completed", "error": None}).eq(
        "id", queue_id
    ).execute()


def mark_failed(queue_id: int, error: str) -> None:
    client.table("siftstack_queue").update(
        {"status": "failed", "error": error[:500]}
    ).eq("id", queue_id).execute()


def upsert_property_data(parcel_acct: str, hcad: dict) -> None:
    """Write HCAD lookup result into property_data keyed on parcel_acct."""
    row = {
        "parcel_acct":   parcel_acct,
        "county":        "Harris",
        "state":         "TX",
        "bedrooms":      _to_int(hcad.get("bedrooms")),
        "bathrooms":     _to_float(hcad.get("bathrooms")),
        "sqft":          _to_int(hcad.get("sqft")),
        "year_built":    _to_int(hcad.get("year_built")),
        "lot_size_sqft": _to_int(hcad.get("lot_size")),
        "market_value":  _to_float(hcad.get("value")),
    }
    # Drop null fields so we don't clobber existing data with blanks.
    row = {
        k: v for k, v in row.items()
        if v is not None or k in ("parcel_acct", "county", "state")
    }
    client.table("property_data").upsert(row, on_conflict="parcel_acct").execute()


def update_record_parcel(record_id: str, parcel_acct: str) -> None:
    """Back-fill records.parcel_acct if HCAD found an account for the address."""
    client.table("records").update({"parcel_acct": parcel_acct}).eq(
        "id", record_id
    ).execute()


# ── Per-row worker ─────────────────────────────────────────────────────
def process_item(item: dict) -> None:
    queue_id = item["id"]
    record = item.get("records") or {}
    doc_num = record.get("doc_num") or "?"
    prop_address = (record.get("prop_address") or "").strip()
    existing_parcel = (record.get("parcel_acct") or "").strip()

    if not prop_address:
        raise ValueError("record has no prop_address — cannot HCAD lookup")

    # Build a fuller search string if city/zip present (improves HCAD match rate).
    query = prop_address
    city = (record.get("prop_city") or "").strip()
    zip_ = (record.get("prop_zip") or "").strip()
    if city:
        query = f"{query}, {city}"
    if zip_:
        query = f"{query} {zip_}"

    log.info("[%s] HCAD lookup: %s", doc_num, query)
    result = _hcad_search_by_address(query)
    if not result:
        raise LookupError(f"HCAD returned no match for '{query}'")

    parcel = (result.get("account_number") or "").strip() or existing_parcel
    if not parcel:
        raise LookupError("HCAD match but no account number returned")

    upsert_property_data(parcel, result)

    if parcel and parcel != existing_parcel:
        update_record_parcel(record["id"], parcel)

    log.info(
        "[%s] property_data upserted (parcel=%s, value=%s, sqft=%s, yr=%s)",
        doc_num, parcel, result.get("value"), result.get("sqft"),
        result.get("year_built"),
    )


# ── Main ───────────────────────────────────────────────────────────────
def main() -> int:
    pending = fetch_pending(BATCH_SIZE)
    log.info("Fetched %d pending queue rows (batch size %d)", len(pending), BATCH_SIZE)
    if not pending:
        return 0

    ok, failed = 0, 0
    for item in pending:
        qid = item["id"]
        try:
            mark_processing(qid)
            process_item(item)
            mark_completed(qid)
            ok += 1
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            log.warning("queue id %s failed: %s", qid, msg)
            log.debug(traceback.format_exc())
            try:
                mark_failed(qid, msg)
            except Exception as inner:
                log.error("mark_failed also failed for id %s: %s", qid, inner)
            failed += 1

    log.info("Run complete: %d succeeded, %d failed", ok, failed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
