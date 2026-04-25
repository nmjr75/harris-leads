"""Shared legal-description matcher.

Parses clerk-style stored legal descriptions (the format produced by
scraper/fetch.py from the clerk results table) and resolves them
against HCAD parcels via the resolve_parcel_by_legal Postgres RPC.

Used by:
  * scraper/supabase_sync.py — fills prop_address on freshly-scraped
    records before they hit siftstack_queue, so newly-scraped records
    skip the PDF-download fallback when their legal is in HCAD.
  * scraper/refresh_records.py — historical refresh of the records
    backlog (uses an in-memory index from the local ZIP instead of
    the RPC, since the ZIP is already cached after a sync).
  * scraper/run_siftstack_worker.py — last-resort fallback when the
    queued record reaches the worker without a parcel_acct.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)


def _norm_token(s: Optional[str]) -> Optional[str]:
    """Strip + uppercase. Strip leading zeros from purely-numeric tokens
    so '01' and '1' both compare as '1'."""
    if s is None:
        return None
    s = s.strip().upper()
    if not s:
        return None
    if s.isdigit():
        return str(int(s))
    return s


def parse_clerk_legal(
    legal: Optional[str],
) -> Optional[tuple[str, Optional[str], Optional[str], Optional[str]]]:
    """Parse the clerk-side stored format produced by fetch.py:
        'SANFORD FARMS | Sec: 1 | Lot: 15 | Block: 3'
    Returns (subdivision, section, lot, block) or None if unparseable.
    Subdivision is required; the rest may be None."""
    if not legal:
        return None
    parts = [p.strip() for p in legal.split("|")]
    subdiv = (parts[0] or "").strip().upper()
    if not subdiv:
        return None
    section = lot = block = None
    for p in parts[1:]:
        if not p or ":" not in p:
            continue
        label, val = p.split(":", 1)
        label = label.strip().lower()
        val   = val.strip()
        if label.startswith("sec"):
            section = _norm_token(val)
        elif label.startswith("lot"):
            lot = _norm_token(val)
        elif label.startswith("block"):
            block = _norm_token(val)
    return (subdiv, section, lot, block)


def resolve_legal(client, legal: Optional[str]) -> Optional[dict]:
    """Resolve a clerk legal to a HCAD parcel via the
    resolve_parcel_by_legal RPC. Returns a dict with parcel_acct +
    situs fields if a unique match exists, else None.

    The RPC handles the matching tiers internally (subdivision is
    required; missing section/lot/block widen the match)."""
    parsed = parse_clerk_legal(legal)
    if not parsed:
        return None
    subdiv, section, lot, block = parsed
    try:
        rpc = client.rpc(
            "resolve_parcel_by_legal",
            {
                "p_subdiv":  subdiv,
                "p_section": section,
                "p_lot":     lot,
                "p_block":   block,
            },
        ).execute()
        acct = rpc.data
        if not acct:
            return None
    except Exception as e:
        log.debug("resolve_parcel_by_legal RPC failed: %s", e)
        return None

    # Pull situs fields from property_data
    try:
        resp = (
            client.table("property_data")
            .select("parcel_acct, situs_address, situs_city, situs_zip, "
                    "owner_name, legal_description")
            .eq("parcel_acct", acct)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.debug("property_data fetch for %s failed: %s", acct, e)
        return None
