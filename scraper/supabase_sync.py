"""Supabase dual-write + SIFTstack auto-queue for harris-leads scrapers.

Called at the end of each scraper run (fetch.py, fetch_foreclosures.py).
Upserts all records into Supabase, then auto-queues any record that has
both a property address AND an owner/grantor name into siftstack_queue.

Silent no-op if SUPABASE_URL + SUPABASE_SECRET_KEY env vars are missing,
so local dev runs without those secrets still work.

Environment variables:
    SUPABASE_URL          — e.g., https://xxx.supabase.co
    SUPABASE_SECRET_KEY   — service-role key (bypasses RLS)
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Iterable, Literal, Optional

log = logging.getLogger(__name__)

BATCH_SIZE = 500
ID_LOOKUP_CHUNK = 200


# ── Value cleaners ─────────────────────────────────────────────────
def _parse_date(value):
    if not value:
        return None
    if not isinstance(value, str):
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


def _clean(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


# ── Normalizers ─────────────────────────────────────────────────────
# Shape must match the public.records column set. Mirrors the logic in
# supabase/import_data.py so new scraper output matches the one-time
# import format.

def normalize_clerk(r: dict) -> dict:
    return {
        "doc_num":          r["doc_num"],
        "doc_type":         _clean(r.get("doc_type")),
        "cat":              _clean(r.get("cat")),
        "cat_label":        _clean(r.get("cat_label")),
        "county":           "Harris",
        "state":            "TX",
        "filed_date":       _parse_date(r.get("filed")),
        "owner":            _clean(r.get("owner")),
        "grantor_name":     _clean(r.get("grantor_name")),
        "grantee_names":    _clean(r.get("grantee_names")),
        "prop_address":     _clean(r.get("prop_address")),
        "prop_city":        _clean(r.get("prop_city")),
        "prop_state":       _clean(r.get("prop_state")) or "TX",
        "prop_zip":         _clean(r.get("prop_zip")),
        "legal_description": _clean(r.get("legal")),
        "amount":           _clean(r.get("amount")),
        "mail_address":     _clean(r.get("mail_address")),
        "mail_city":        _clean(r.get("mail_city")),
        "mail_state":       _clean(r.get("mail_state")) or "TX",
        "mail_zip":         _clean(r.get("mail_zip")),
        "clerk_url":        _clean(r.get("clerk_url")),
        "hcad_url":         _clean(r.get("hcad_url")),
        "parcel_acct":      _clean(r.get("parcel_acct")),
        "match_confidence": _clean(r.get("match_confidence")),
        "address_source":   _clean(r.get("address_source")),
        "flags":            r.get("flags") or [],
        "score":            r.get("score"),
    }


def normalize_foreclosure(r: dict) -> dict:
    return {
        "doc_num":          r.get("doc_num") or r.get("doc_id"),
        "doc_type":         _clean(r.get("doc_type")) or "FRCL",
        "cat":              _clean(r.get("cat")),
        "cat_label":        _clean(r.get("cat_label")),
        "county":           "Harris",
        "state":            "TX",
        "filed_date":       _parse_date(r.get("file_date") or r.get("filed")),
        "sale_date":        _parse_date(r.get("sale_date")),
        "grantor_name":     _clean(r.get("grantor") or r.get("grantor_name")),
        "co_borrower":      _clean(r.get("co_borrower")),
        "mortgagee":        _clean(r.get("mortgagee")),
        "amount":           _clean(r.get("amount")),
        "prop_address":     _clean(r.get("prop_address")),
        "prop_city":        _clean(r.get("prop_city")),
        "prop_state":       _clean(r.get("prop_state")) or "TX",
        "prop_zip":         _clean(r.get("prop_zip")),
        "legal_description": _clean(r.get("legal_description") or r.get("legal")),
        "clerk_url":        _clean(r.get("clerk_url")),
        "hcad_url":         _clean(r.get("hcad_url")),
        "parcel_acct":      _clean(r.get("parcel_acct")),
        "match_confidence": _clean(r.get("match_confidence")),
        "batch_id":         _clean(r.get("batch_id")),
        "batch_label":      _clean(r.get("batch_label")),
        "date_scraped":     _parse_date(r.get("date_scraped")),
        "needs_ocr":        bool(r.get("needs_ocr", False)),
        "flags":            r.get("flags") or [],
    }


def _qualifies_for_autoqueue(r: dict) -> bool:
    addr = r.get("prop_address")
    has_addr = bool(addr and addr.strip())
    name = (r.get("owner") or r.get("grantor_name") or "").strip()
    return has_addr and bool(name)


# ── Main entry point ────────────────────────────────────────────────
def upsert_and_autoqueue(
    records: Iterable[dict],
    source: Literal["clerk", "foreclosure"],
    run_id: Optional[int] = None,
) -> dict:
    """Upsert records to Supabase + auto-queue qualifying rows.

    Silent no-op if Supabase creds are missing or supabase package is
    not installed. Any exception during the upsert or queue step is
    logged as a warning and swallowed — this function must never block
    the scraper from completing.

    Per-run audit: when run_id is provided, captures a pre-upsert snapshot
    of every doc_num being touched, then after upsert classifies each
    record as 'created'/'updated'/'enriched'/'seen' and writes a row to
    scraper_run_records. Caller (the scraper main()) provides run_id from
    run_audit.start_run() and uses the returned counts in run_audit.finish_run().

    Returns dict with keys: seen, created, updated, enriched, total.
    Always returns a populated dict (zeros on early exit) so callers can
    rely on the shape.
    """
    empty = {"seen": 0, "created": 0, "updated": 0, "enriched": 0, "total": 0}
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SECRET_KEY") or "").strip()
    if not url or not key:
        log.info("Supabase not configured (SUPABASE_URL/SUPABASE_SECRET_KEY missing) "
                 "— skipping dual-write + auto-queue")
        return empty

    try:
        from supabase import create_client
    except ImportError:
        log.warning("supabase package not installed — skipping dual-write")
        return empty

    try:
        client = create_client(url, key)
    except Exception as e:
        log.warning(f"Supabase client init failed: {e} — skipping dual-write")
        return empty

    normalize = normalize_clerk if source == "clerk" else normalize_foreclosure
    records = list(records)
    normalized = [normalize(r) for r in records if r.get("doc_num") or r.get("doc_id")]
    if not normalized:
        log.info("No records to sync")
        return empty

    # ── Pre-upsert snapshot for audit diff (only if run_id given) ────
    pre_snapshot: dict[str, dict] = {}
    if run_id:
        try:
            try:
                from .run_audit import snapshot_existing
            except ImportError:
                from run_audit import snapshot_existing  # type: ignore
            pre_snapshot = snapshot_existing(
                client, [r["doc_num"] for r in normalized if r.get("doc_num")]
            )
            log.info(f"Audit snapshot: {len(pre_snapshot)} existing records "
                     f"captured for diff")
        except Exception as e:
            log.warning(f"Audit snapshot failed (continuing without diff): {e}")
            pre_snapshot = {}

    log.info(f"Syncing {len(normalized)} records to Supabase ({source})...")
    upserted = 0
    for i in range(0, len(normalized), BATCH_SIZE):
        batch = normalized[i:i + BATCH_SIZE]
        try:
            client.table("records").upsert(batch, on_conflict="doc_num").execute()
            upserted += len(batch)
        except Exception as e:
            log.warning(f"records upsert batch {i // BATCH_SIZE} failed: {e}")
            return empty
    log.info(f"Upserted {upserted} records to Supabase")

    # ── Audit: classify + write junction rows ────────────────────────
    audit_counts = {"seen": 0, "created": 0, "updated": 0, "enriched": 0,
                    "junction_inserted": 0, "errors": 0}
    if run_id:
        try:
            try:
                from .run_audit import classify_with_pre_snapshot
            except ImportError:
                from run_audit import classify_with_pre_snapshot  # type: ignore
            audit_counts = classify_with_pre_snapshot(
                client, run_id, pre_snapshot, normalized
            )
            log.info(f"Audit: {audit_counts['created']} created, "
                     f"{audit_counts['updated']} updated, "
                     f"{audit_counts['enriched']} enriched, "
                     f"{audit_counts['seen']} seen "
                     f"(junction rows inserted: {audit_counts['junction_inserted']})")
        except Exception as e:
            log.warning(f"Audit classify failed (records still saved): {e}")

    summary = {
        "seen":     audit_counts.get("seen", 0),
        "created":  audit_counts.get("created", 0),
        "updated":  audit_counts.get("updated", 0),
        "enriched": audit_counts.get("enriched", 0),
        "total":    (audit_counts.get("seen", 0) +
                     audit_counts.get("created", 0) +
                     audit_counts.get("updated", 0) +
                     audit_counts.get("enriched", 0)) or len(normalized),
    }

    # ── Live legal-description fallback ─────────────────────────────
    # For any record that came in without a prop_address but DOES have
    # a legal description, try to resolve it against the HCAD bulk via
    # the resolve_parcel_by_legal RPC. Newly-resolved records get
    # patched in-place (records table + the local normalized dict) so
    # they qualify for the auto-queue step that follows.
    try:
        from .legal_matcher import resolve_legal
    except ImportError:
        try:
            from legal_matcher import resolve_legal  # type: ignore
        except ImportError:
            resolve_legal = None

    if resolve_legal is not None:
        legal_resolved = 0
        for r in normalized:
            if r.get("prop_address") or not r.get("legal_description"):
                continue
            match = resolve_legal(client, r["legal_description"])
            if not match:
                continue
            patch = {
                "parcel_acct":      match["parcel_acct"],
                "prop_address":     match.get("situs_address") or "",
                "prop_city":        match.get("situs_city") or "Houston",
                "prop_zip":         match.get("situs_zip") or "",
                "address_source":   "hcad_legal",
                "match_confidence": "high",
            }
            try:
                client.table("records").update(patch).eq(
                    "doc_num", r["doc_num"]
                ).execute()
                # Mirror into the normalized dict so auto-queue sees it
                r.update(patch)
                legal_resolved += 1
            except Exception as e:
                log.debug(f"legal-fallback patch failed for {r.get('doc_num')}: {e}")
        if legal_resolved:
            log.info(f"Legal-fallback resolved {legal_resolved} records "
                     "(address filled from HCAD via legal description)")

    # Auto-queue: only records with both address and owner/grantor name
    # AND not yet submitted to SIFTstack (status is null or 'failed').
    qualifying = [r for r in normalized if _qualifies_for_autoqueue(r)]
    if not qualifying:
        log.info("Auto-queue: 0 qualifying records")
        return summary

    doc_nums = [r["doc_num"] for r in qualifying]

    # Look up record IDs + current siftstack_status
    id_map: dict[str, dict] = {}
    for i in range(0, len(doc_nums), ID_LOOKUP_CHUNK):
        chunk = doc_nums[i:i + ID_LOOKUP_CHUNK]
        try:
            resp = (
                client.table("records")
                .select("id, doc_num, siftstack_status")
                .in_("doc_num", chunk)
                .execute()
            )
            for row in resp.data or []:
                id_map[row["doc_num"]] = row
        except Exception as e:
            log.warning(f"ID lookup chunk failed: {e}")
            return summary

    to_queue = [
        id_map[dn] for dn in doc_nums
        if dn in id_map and id_map[dn].get("siftstack_status") in (None, "failed")
    ]

    if not to_queue:
        log.info("Auto-queue: all qualifying records already queued/completed")
        return summary

    batch_id = str(uuid.uuid4())
    queue_rows = [
        {
            "record_id": r["id"],
            "batch_id":  batch_id,
            "queued_by": None,              # scraper-originated (migration 004)
            "status":    "pending",
        }
        for r in to_queue
    ]

    try:
        client.table("siftstack_queue").insert(queue_rows).execute()
        log.info(f"Auto-queued {len(queue_rows)} records for SIFTstack "
                 f"(batch {batch_id[:8]})")
    except Exception as e:
        log.warning(f"siftstack_queue insert failed: {e}")

    return summary
