"""Backfill scraper_runs + scraper_run_records from existing batch_id/batch_label data.

Background
----------
The Harris foreclosure scraper has been stamping records with batch_id (B1..B33)
and batch_label ("Run #42 / Batch 33 - 2026-04-26 03:47 AM CDT") since the start.
Migration 010 added the scraper_runs + scraper_run_records tables to enable a
dashboard run-picker. This script populates both from the existing batch_id data
so the run-picker has full history from day one.

What it does (foreclosure records only)
---------------------------------------
1. Pulls every record with a batch_id (currently 1184 foreclosures).
2. Groups records by batch_id.
3. For each unique batch_id:
   - Parses gh_run_number from batch_label ("Run #42 ..." -> 42; older batches
     without the prefix get NULL).
   - Parses started_at from batch_label timestamp.
   - INSERTs a scraper_runs row (idempotent: checked by gh_run_number when
     present, otherwise by run_id text matching batch_id).
   - INSERTs scraper_run_records rows with action='created' for every record
     in that batch (idempotent via UNIQUE(run_id, record_id) constraint —
     duplicates are silently skipped on re-run).
4. Updates each scraper_runs row's totals (total_seen, total_created).

Why lead-scraper runs are NOT backfilled
----------------------------------------
The lead scraper (fetch.py) does not stamp records with batch_id. Without that
data we cannot accurately reconstruct which lead-scraper run touched which
records. Past lead runs will appear in the dashboard ONLY after Step 3 ships
(scraper instrumentation). Going forward, every run gets full audit trail.

Idempotency
-----------
- Re-running the script does not duplicate anything.
- scraper_runs gets one row per unique batch_id (matched by run_id text field).
- scraper_run_records is protected by UNIQUE(run_id, record_id).

Run from harris-leads root:
    python supabase/backfill_runs.py
"""
from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scraper"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "supabase" / ".env")
except ImportError:
    pass

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"].strip()

sb = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


# ── Parsing helpers ─────────────────────────────────────────────────────────

GH_RUN_RE = re.compile(r"Run\s*#(\d+)", re.IGNORECASE)

# Matches both:
#   "Batch 7 - 2026-04-14 01:07 AM CDT"
#   "Run #42 / Batch 33 - 2026-04-26 03:47 AM CDT"
# Captures the date portion: 2026-04-14 01:07 AM CDT
TIMESTAMP_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}\s*(?:AM|PM)\s*\w+)",
    re.IGNORECASE,
)


def parse_gh_run(label: str) -> Optional[int]:
    if not label:
        return None
    m = GH_RUN_RE.search(label)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def parse_started_at(label: str) -> Optional[str]:
    """Returns ISO-format string or None.

    Tries parsing "YYYY-MM-DD HH:MM AM/PM TZ" patterns. Timezone parsing in
    Python's strptime is unreliable across locales, so we strip the TZ suffix
    and store as naive timestamp; Postgres timestamptz interprets it as UTC
    if no offset is supplied. For our use case (display-only timestamps in
    the run-picker dropdown) this is acceptable.
    """
    if not label:
        return None
    m = TIMESTAMP_RE.search(label)
    if not m:
        return None
    raw = m.group(1)
    # Strip the trailing tz word (CDT, CST, UTC, etc.) — strptime can't handle
    # arbitrary TZ abbreviations reliably.
    cleaned = re.sub(r"\s*(CDT|CST|EDT|EST|PDT|PST|MDT|MST|UTC|GMT)\s*$", "",
                     raw, flags=re.IGNORECASE).strip()
    for fmt in ("%Y-%m-%d %I:%M %p", "%Y-%m-%d %I:%M%p"):
        try:
            return datetime.strptime(cleaned, fmt).isoformat()
        except ValueError:
            continue
    return None


# ── Step 1: pull all records with batch_id ──────────────────────────────────

print("\n[1/4] Pulling all records with batch_id from Supabase...")
all_recs = []
offset = 0
while True:
    page = (
        sb.table("records")
        .select("id,doc_num,doc_type,cat,batch_id,batch_label,filed_date,created_at")
        .not_.is_("batch_id", "null")
        .range(offset, offset + 999)
        .execute()
        .data
    )
    if not page:
        break
    all_recs.extend(page)
    if len(page) < 1000:
        break
    offset += 1000

print(f"      Loaded {len(all_recs)} records with batch_id")


# ── Step 2: group records by batch_id ───────────────────────────────────────

print("\n[2/4] Grouping records by batch_id...")
by_batch: dict[str, list[dict]] = defaultdict(list)
for r in all_recs:
    by_batch[r["batch_id"]].append(r)

print(f"      Found {len(by_batch)} unique batches:")
for bid, recs in sorted(by_batch.items(),
                        key=lambda kv: int(re.sub(r"\D", "", kv[0]) or 0)):
    label = (recs[0].get("batch_label") or "").encode("ascii", "replace").decode()
    gh = parse_gh_run(recs[0].get("batch_label", ""))
    print(f"      {bid:6s}  count={len(recs):4d}  gh_run={gh}  "
          f"label={label[:60]}")


# ── Step 3: upsert scraper_runs rows ────────────────────────────────────────

print("\n[3/4] Creating/updating scraper_runs rows...")

# Build a map of (gh_run_number OR run_id-as-batch_id) -> existing scraper_runs.id
# so this script is idempotent and can be re-run safely.
existing_runs = sb.table("scraper_runs").select("id,run_id,gh_run_number").execute().data
runs_by_run_id_text = {r["run_id"]: r for r in existing_runs if r.get("run_id")}
runs_by_gh = {r["gh_run_number"]: r for r in existing_runs if r.get("gh_run_number")}

batch_to_run_pk: dict[str, int] = {}    # batch_id -> scraper_runs.id

for bid, recs in by_batch.items():
    label = recs[0].get("batch_label") or bid
    gh = parse_gh_run(label)
    started_at = parse_started_at(label)
    earliest_filed = min(
        (r["filed_date"] for r in recs if r.get("filed_date")),
        default=None,
    )

    # Resolve existing scraper_runs row if any
    existing = None
    if gh is not None and gh in runs_by_gh:
        existing = runs_by_gh[gh]
    elif bid in runs_by_run_id_text:
        existing = runs_by_run_id_text[bid]

    payload = {
        "run_id":           bid,                       # text identifier we kept
        "gh_run_number":    gh,
        "workflow_name":    "scrape-foreclosures.yml",
        "started_at":       started_at,
        "completed_at":     started_at,                # backfill: use same ts
        "status":           "completed",
        "total_seen":       len(recs),
        "total_created":    len(recs),                 # all backfilled as created
        "total_updated":    0,
        "records_ingested": len(recs),
    }
    # Strip nulls so we don't overwrite with NULL on update
    payload = {k: v for k, v in payload.items() if v is not None}

    if existing:
        sb.table("scraper_runs").update(payload).eq("id", existing["id"]).execute()
        batch_to_run_pk[bid] = existing["id"]
    else:
        ins = sb.table("scraper_runs").insert(payload).execute()
        batch_to_run_pk[bid] = ins.data[0]["id"]

print(f"      {len(batch_to_run_pk)} scraper_runs rows ready")


# ── Step 4: insert junction rows (idempotent via UNIQUE constraint) ─────────

print("\n[4/4] Inserting scraper_run_records junction rows...")
inserted = 0
skipped = 0
errors = 0

# Pull existing junction rows so we know what to skip — much faster than
# letting every insert fail on UNIQUE violation in a 1k+ batch.
existing_pairs: set[tuple[int, str]] = set()
for run_pk in batch_to_run_pk.values():
    page = (sb.table("scraper_run_records")
            .select("run_id,record_id")
            .eq("run_id", run_pk)
            .execute()
            .data)
    for row in page:
        existing_pairs.add((row["run_id"], row["record_id"]))

# Build all pending inserts, then chunk for efficient inserts
pending = []
for bid, recs in by_batch.items():
    run_pk = batch_to_run_pk[bid]
    for r in recs:
        if (run_pk, r["id"]) in existing_pairs:
            skipped += 1
            continue
        pending.append({
            "run_id":     run_pk,
            "record_id":  r["id"],
            "doc_num":    r["doc_num"],
            "action":     "created",
            "fields_changed": None,
        })

# Insert in batches of 500 to be polite to the API
CHUNK = 500
for i in range(0, len(pending), CHUNK):
    chunk = pending[i:i + CHUNK]
    try:
        sb.table("scraper_run_records").insert(chunk).execute()
        inserted += len(chunk)
        print(f"      inserted batch {i // CHUNK + 1}: "
              f"{len(chunk)} rows  (total {inserted}/{len(pending)})")
    except Exception as e:
        errors += len(chunk)
        msg = str(e)[:120].encode("ascii", "replace").decode()
        print(f"      ERROR on batch {i // CHUNK + 1}: {msg}")

print(f"\n      Junction: {inserted} inserted, {skipped} already-present, {errors} errors")


# ── Summary ─────────────────────────────────────────────────────────────────

print("\n=== Backfill complete ===")
print(f"  scraper_runs rows:           {len(batch_to_run_pk)}")
print(f"  scraper_run_records rows:    {inserted} new (+ {skipped} existing)")
print(f"  records covered:             {len(all_recs)}")
print()
print("  Next: ship Step 3 (scraper instrumentation) so future runs auto-populate.")
print()
