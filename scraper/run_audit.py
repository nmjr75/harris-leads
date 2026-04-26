"""Per-run audit trail helpers — populates scraper_runs + scraper_run_records.

Used by both fetch.py (lead scraper) and fetch_foreclosures.py to record
exactly which records each scraper run created / updated / verified.

Design
------
- Caller invokes start_run() at scraper start, gets run_id back.
- Caller passes run_id into upsert_and_autoqueue() (or any other Supabase
  write site). The classify_and_audit() helper diffs each record against
  the existing DB row, classifies the action, and writes a junction row.
- Caller invokes finish_run() at the end with the final totals.

Failure mode
------------
Every function in this module is wrapped to never raise to the caller.
If Supabase is unreachable or RLS rejects the write, the audit data is
lost for that run but the scrape itself continues unaffected. Audit must
not block production work.

Idempotency
-----------
- start_run() inserts a new row; safe to call multiple times (each call
  creates a new run, which is the expected behavior on retry).
- classify_and_audit() relies on UNIQUE(run_id, record_id) in the
  junction table to silently de-dup if called twice with same args.
- finish_run() does an UPDATE, idempotent by definition.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Iterable, Optional

log = logging.getLogger(__name__)

# Columns that count as "meaningful" changes for the audit diff. Anything
# changed outside this list is treated as background noise (e.g., timestamps,
# row counts, scraper-internal metadata). Keep this list aligned with the
# fields humans actually care about for marketing / decisions.
AUDIT_COLUMNS = (
    "prop_address",
    "prop_city",
    "prop_zip",
    "mail_address",
    "mail_city",
    "mail_zip",
    "grantor_name",
    "grantee_names",
    "owner",
    "amount",
    "sale_date",
    "filed_date",
    "match_confidence",
    "parcel_acct",
    "legal_description",
    "co_borrower",
    "mortgagee",
)


def _get_client():
    """Return supabase client or None. Quiet on missing env."""
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SECRET_KEY") or "").strip()
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception as e:
        log.warning(f"run_audit: supabase init failed — {e}")
        return None


def start_run(
    workflow_name: str,
    *,
    date_range_from: Optional[str] = None,
    date_range_to: Optional[str] = None,
    run_id_text: Optional[str] = None,
) -> Optional[int]:
    """Insert a scraper_runs row and return its primary key (bigint).

    Returns None on any failure — caller must tolerate None and skip
    audit writes for the rest of the run.

    workflow_name: e.g. "scrape.yml" or "scrape-foreclosures.yml"
    date_range_from / date_range_to: optional ISO date strings
    run_id_text: optional human-friendly identifier (defaults to a
                 timestamp string if GITHUB_RUN_NUMBER not present)
    """
    client = _get_client()
    if not client:
        return None

    gh_run_number = None
    raw = (os.environ.get("GITHUB_RUN_NUMBER") or "").strip()
    if raw.isdigit():
        gh_run_number = int(raw)

    if not run_id_text:
        run_id_text = (
            f"GH#{gh_run_number}" if gh_run_number
            else f"local-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        )

    payload: dict[str, Any] = {
        "run_id":          run_id_text,
        "gh_run_number":   gh_run_number,
        "workflow_name":   workflow_name,
        "started_at":      datetime.utcnow().isoformat(),
        "status":          "running",
        "total_seen":      0,
        "total_created":   0,
        "total_updated":   0,
    }
    if date_range_from:
        payload["date_range_from"] = date_range_from
    if date_range_to:
        payload["date_range_to"] = date_range_to

    try:
        resp = client.table("scraper_runs").insert(payload).execute()
        run_pk = resp.data[0]["id"] if resp.data else None
        if run_pk:
            log.info(f"run_audit: started run {run_pk} "
                     f"(gh_run={gh_run_number}, workflow={workflow_name})")
        return run_pk
    except Exception as e:
        log.warning(f"run_audit: start_run failed — {e}")
        return None


def classify_and_audit(
    client,
    run_id: int,
    records_to_upsert: list[dict],
) -> dict:
    """Diff records against existing DB rows, write junction entries, return counts.

    Should be called RIGHT AFTER the upsert into the records table — at that
    point both old (snapshot we capture before upsert) and new (the dict we
    just upserted) values are known. The records table guarantees doc_num
    uniqueness so we look up by doc_num.

    Returns dict: {seen, created, updated, enriched, junction_inserted, errors}.
    """
    counts = {
        "seen": 0, "created": 0, "updated": 0, "enriched": 0,
        "junction_inserted": 0, "errors": 0,
    }
    if not run_id or not records_to_upsert or not client:
        return counts

    doc_nums = [r["doc_num"] for r in records_to_upsert if r.get("doc_num")]
    if not doc_nums:
        return counts

    # 1. Snapshot existing rows (one bulk fetch). NOTE: the caller MUST call
    #    this BEFORE upsert if they want a true diff. To keep this function
    #    self-contained, we fetch from the DB AFTER the upsert and compare to
    #    the payload we were handed. This means "updated" detection works only
    #    when caller passes the OUTGOING dict (post-upsert state). For pre-vs-
    #    post diff, see classify_with_pre_snapshot() below.
    existing_map: dict[str, dict] = {}
    for i in range(0, len(doc_nums), 500):
        chunk = doc_nums[i:i + 500]
        try:
            page = (client.table("records")
                    .select("id,doc_num," + ",".join(AUDIT_COLUMNS))
                    .in_("doc_num", chunk)
                    .execute()
                    .data) or []
            for row in page:
                existing_map[row["doc_num"]] = row
        except Exception as e:
            log.warning(f"run_audit: existing fetch failed — {e}")
            counts["errors"] += 1

    # 2. We don't have the pre-upsert snapshot here, so every doc_num found
    #    in existing_map is treated as 'seen' (no diff possible). Use
    #    classify_with_pre_snapshot() for full diff support; this function
    #    is a fallback for the simple case.
    junction_rows = []
    for rec in records_to_upsert:
        doc_num = rec.get("doc_num")
        if not doc_num:
            continue
        existing = existing_map.get(doc_num)
        if not existing:
            continue  # shouldn't happen post-upsert, but safe
        action = "seen"
        counts[action] += 1
        junction_rows.append({
            "run_id":     run_id,
            "record_id":  existing["id"],
            "doc_num":    doc_num,
            "action":     action,
            "fields_changed": None,
        })

    # 3. Bulk insert junction rows
    for i in range(0, len(junction_rows), 500):
        chunk = junction_rows[i:i + 500]
        try:
            client.table("scraper_run_records").insert(chunk).execute()
            counts["junction_inserted"] += len(chunk)
        except Exception as e:
            counts["errors"] += len(chunk)
            log.warning(f"run_audit: junction insert failed — {str(e)[:120]}")

    return counts


def classify_with_pre_snapshot(
    client,
    run_id: int,
    pre_snapshot: dict[str, dict],
    records_after_upsert: list[dict],
) -> dict:
    """Diff using a pre-upsert snapshot — provides full action classification.

    pre_snapshot: dict mapping doc_num -> existing record dict (from BEFORE
                  upsert). Caller is responsible for capturing this.
    records_after_upsert: the records that were just upserted. Their UUIDs
                          come from a post-upsert lookup we do here.

    Returns dict: {seen, created, updated, enriched, junction_inserted, errors}.
    """
    counts = {
        "seen": 0, "created": 0, "updated": 0, "enriched": 0,
        "junction_inserted": 0, "errors": 0,
    }
    if not run_id or not records_after_upsert or not client:
        return counts

    doc_nums = [r["doc_num"] for r in records_after_upsert if r.get("doc_num")]
    if not doc_nums:
        return counts

    # Re-fetch UUIDs (they're stable for existing rows; new rows need a lookup)
    id_map: dict[str, str] = {}
    for i in range(0, len(doc_nums), 500):
        chunk = doc_nums[i:i + 500]
        try:
            page = (client.table("records")
                    .select("id,doc_num")
                    .in_("doc_num", chunk)
                    .execute()
                    .data) or []
            for row in page:
                id_map[row["doc_num"]] = row["id"]
        except Exception as e:
            log.warning(f"run_audit: id fetch failed — {e}")
            counts["errors"] += 1

    junction_rows = []
    for rec in records_after_upsert:
        doc_num = rec.get("doc_num")
        rec_uuid = id_map.get(doc_num)
        if not doc_num or not rec_uuid:
            continue

        before = pre_snapshot.get(doc_num)
        if before is None:
            counts["created"] += 1
            junction_rows.append({
                "run_id": run_id, "record_id": rec_uuid, "doc_num": doc_num,
                "action": "created", "fields_changed": None,
            })
            continue

        # Diff the audit columns
        diff: dict[str, list] = {}
        null_fills = 0
        nonnull_changes = 0
        for col in AUDIT_COLUMNS:
            old = before.get(col)
            new = rec.get(col)
            old_norm = (old or "")
            new_norm = (new or "")
            if old_norm == new_norm:
                continue
            diff[col] = [old, new]
            if not old_norm and new_norm:
                null_fills += 1
            else:
                nonnull_changes += 1

        if not diff:
            counts["seen"] += 1
            action = "seen"
            fields_changed = None
        elif nonnull_changes == 0:
            # All changes are NULL -> value fills
            counts["enriched"] += 1
            action = "enriched"
            fields_changed = diff
        else:
            counts["updated"] += 1
            action = "updated"
            fields_changed = diff

        junction_rows.append({
            "run_id": run_id, "record_id": rec_uuid, "doc_num": doc_num,
            "action": action, "fields_changed": fields_changed,
        })

    for i in range(0, len(junction_rows), 500):
        chunk = junction_rows[i:i + 500]
        try:
            client.table("scraper_run_records").insert(chunk).execute()
            counts["junction_inserted"] += len(chunk)
        except Exception as e:
            counts["errors"] += len(chunk)
            log.warning(f"run_audit: junction insert failed — {str(e)[:120]}")

    return counts


def snapshot_existing(client, doc_nums: Iterable[str]) -> dict[str, dict]:
    """Capture pre-upsert state for diff comparison. Pass to
    classify_with_pre_snapshot() after upsert."""
    if not client:
        return {}
    nums = [n for n in doc_nums if n]
    if not nums:
        return {}
    out: dict[str, dict] = {}
    for i in range(0, len(nums), 500):
        chunk = nums[i:i + 500]
        try:
            page = (client.table("records")
                    .select("id,doc_num," + ",".join(AUDIT_COLUMNS))
                    .in_("doc_num", chunk)
                    .execute()
                    .data) or []
            for row in page:
                out[row["doc_num"]] = row
        except Exception as e:
            log.warning(f"run_audit: snapshot failed — {e}")
    return out


def finish_run(
    run_id: int,
    *,
    status: str,
    total_seen: int = 0,
    total_created: int = 0,
    total_updated: int = 0,
    error_summary: Optional[str] = None,
) -> None:
    """Mark the run as completed/failed and write final counts."""
    client = _get_client()
    if not client or not run_id:
        return
    payload: dict[str, Any] = {
        "status":          status,
        "completed_at":    datetime.utcnow().isoformat(),
        "total_seen":      total_seen,
        "total_created":   total_created,
        "total_updated":   total_updated,
    }
    if error_summary:
        payload["error_summary"] = error_summary[:1000]
    try:
        client.table("scraper_runs").update(payload).eq("id", run_id).execute()
        log.info(f"run_audit: finished run {run_id} status={status} "
                 f"seen={total_seen} created={total_created} "
                 f"updated={total_updated}")
    except Exception as e:
        log.warning(f"run_audit: finish_run failed — {e}")
