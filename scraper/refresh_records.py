"""Refresh records.parcel_acct + prop_address from the full HCAD bulk data.

Walks every row in records and tries — in order — to:
  1. Address-based match via resolve_parcel_by_address() RPC
     (for records that have prop_address but no parcel_acct yet).
  2. Legal-description fallback — parses the clerk-side legal stored
     in records.legal_description (e.g. "SANFORD FARMS | Sec: 1 |
     Lot: 15 | Block: 3") and looks it up against the HCAD legal
     descriptions in property_data (e.g. "LT 15 BLK 3 SANFORD FARMS
     SEC 1"). Only commits when the match is unique — ambiguous
     matches are flagged but not auto-resolved.

When a record gets newly resolved, this script:
  * sets parcel_acct
  * fills prop_address / prop_city / prop_zip from HCAD's situs
    address (only if the record didn't already have one and the VA
    hasn't manually overridden the field)
  * marks match_confidence='high', address_source='hcad_legal'
    (or 'hcad_address' for tier-1 resolutions)

The legal-description index is built locally from the cached
Real_acct_owner.zip — no Supabase round-trips per record. ~10s to
build, ~1ms per record lookup. Designed to run after sync_hcad_bulk.py
in the same workflow so the ZIP is already on disk.

Environment variables:
    SUPABASE_URL
    SUPABASE_SECRET_KEY
    HCAD_WORK_DIR        — default /tmp/hcad_sync (where sync writes ZIPs)
    HCAD_DATA_YEAR       — default 2026
    REFRESH_DRY_RUN      — "1" to print what would change without writing
"""
from __future__ import annotations

import logging
import os
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

log = logging.getLogger("refresh_records")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
HCAD_YEAR    = os.environ.get("HCAD_DATA_YEAR", "2026").strip()
WORK_DIR     = Path(os.environ.get("HCAD_WORK_DIR", "/tmp/hcad_sync"))
DRY_RUN      = os.environ.get("REFRESH_DRY_RUN", "0").strip() == "1"

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed")
    sys.exit(1)

client = create_client(SUPABASE_URL, SUPABASE_KEY)

# real_acct.txt column indices used here
RA_ACCT    = 0
RA_SITE_1  = 17
RA_SITE_2  = 18
RA_SITE_3  = 19
RA_LGL_1   = 66
RA_LGL_2   = 67
RA_LGL_3   = 68
RA_LGL_4   = 69


# ═══════════════════════════════════════════════════════════════════════
#  Legal-description parsers
# ═══════════════════════════════════════════════════════════════════════
def _norm_token(s: str | None) -> str | None:
    """Strip + uppercase. Strip leading zeros for purely-numeric tokens."""
    if s is None:
        return None
    s = s.strip().upper()
    if not s:
        return None
    if s.isdigit():
        return str(int(s))
    return s


# Use the shared multi-format parser. Handles pipe ('SUBDIV | Sec: N |
# Lot: N | Block: N'), HCAD ('LT 15 BLK 3 SUBDIV SEC N'), and prose
# ('Lot Thirteen (13) in Block Three (3) of Hidden Meadow, Sec 9') —
# all four foreclosure-PDF + lead-scraper formats normalize through it.
try:
    from legal_matcher import parse_clerk_legal  # noqa: F401
except ImportError:
    from .legal_matcher import parse_clerk_legal  # type: ignore  # noqa: F401


_HCAD_LOT_RE   = re.compile(r"\bLT\s+([0-9A-Z]+)", re.IGNORECASE)
_HCAD_BLOCK_RE = re.compile(r"\bBLK\s+([0-9A-Z]+)", re.IGNORECASE)
_HCAD_SEC_RE   = re.compile(r"\bSEC\s+([0-9A-Z]+)", re.IGNORECASE)
# Tokens we want to strip from HCAD legal to recover subdivision name
_HCAD_STRIP_RE = re.compile(
    r"\b(?:LT|LTS|BLK|SEC|TR|TRS|RES|U|UNIT|ACRES?|ABST|HOMESTEAD|"
    r"OUT OF|& UNDIV|UNDIV)\s+[0-9A-Z./%-]+",
    re.IGNORECASE,
)


def parse_hcad_legal(legal: str | None) -> tuple[str, str | None, str | None, str | None] | None:
    """Parse HCAD's lgl_1..lgl_4 concatenated form:
        'LT 13 BLK 1 JASMINE HEIGHTS SEC 18'
    Returns (subdivision, section, lot, block) or None if unparseable."""
    if not legal:
        return None
    s = legal.upper().strip()
    if not s:
        return None
    lot_m = _HCAD_LOT_RE.search(s)
    block_m = _HCAD_BLOCK_RE.search(s)
    sec_m = _HCAD_SEC_RE.search(s)
    lot     = _norm_token(lot_m.group(1)) if lot_m else None
    block   = _norm_token(block_m.group(1)) if block_m else None
    section = _norm_token(sec_m.group(1)) if sec_m else None

    # Subdivision = whatever's left after pulling out the structured tokens.
    subdiv = _HCAD_STRIP_RE.sub(" ", s)
    subdiv = " ".join(subdiv.split()).strip()
    if not subdiv:
        return None
    return (subdiv, section, lot, block)


# ═══════════════════════════════════════════════════════════════════════
#  Build the in-memory legal-description index from HCAD bulk data
# ═══════════════════════════════════════════════════════════════════════
def build_legal_index() -> dict[tuple, list[dict]]:
    """Stream real_acct.txt and bucket every parcel by its parsed legal key.
    Returns {(subdiv, sec, lot, block): [parcel_dict, ...]}."""
    zip_path = WORK_DIR / "Real_acct_owner.zip"
    if not zip_path.exists():
        log.error("Real_acct_owner.zip not found at %s — run sync_hcad_bulk.py first",
                  zip_path)
        sys.exit(1)

    log.info("Building HCAD legal-description index from %s ...", zip_path)
    index: dict[tuple, list[dict]] = defaultdict(list)
    seen = parsed = 0
    with zipfile.ZipFile(zip_path) as z:
        with z.open("real_acct.txt") as f:
            _ = f.readline()
            for raw in f:
                seen += 1
                try:
                    parts = raw.decode("latin-1", errors="replace").rstrip("\n\r").split("\t")
                except Exception:
                    continue
                if len(parts) <= RA_LGL_4:
                    continue
                acct = parts[RA_ACCT].strip()
                if not acct:
                    continue
                legal = " ".join(
                    parts[i].strip()
                    for i in (RA_LGL_1, RA_LGL_2, RA_LGL_3, RA_LGL_4)
                    if parts[i].strip()
                )
                key = parse_hcad_legal(legal)
                if not key:
                    continue
                parsed += 1
                index[key].append({
                    "parcel_acct":   acct,
                    "situs_address": parts[RA_SITE_1].strip() if len(parts) > RA_SITE_1 else "",
                    "situs_city":    parts[RA_SITE_2].strip() if len(parts) > RA_SITE_2 else "",
                    "situs_zip":     parts[RA_SITE_3].strip() if len(parts) > RA_SITE_3 else "",
                    "legal_raw":     legal,
                })
    log.info("  scanned %d parcels, parsed %d, %d distinct legal keys",
             seen, parsed, len(index))
    return index


# ═══════════════════════════════════════════════════════════════════════
#  Match a clerk legal against the index — tiered fallback
# ═══════════════════════════════════════════════════════════════════════
def find_parcels(index: dict[tuple, list[dict]],
                 key: tuple[str, str | None, str | None, str | None]
                 ) -> tuple[list[dict], str]:
    """Try exact 4-tuple match, then drop section, then drop section+block.
    Returns (candidates, tier_label)."""
    subdiv, sec, lot, block = key

    # Tier A: exact 4-tuple match
    if all([subdiv, sec, lot, block]) and (subdiv, sec, lot, block) in index:
        return index[(subdiv, sec, lot, block)], "legal_exact"

    # Tier B: subdivision + lot + block (no section)
    if subdiv and lot and block:
        out = []
        for k, parcels in index.items():
            if k[0] == subdiv and k[2] == lot and k[3] == block:
                out.extend(parcels)
        if out:
            return out, "legal_no_section"

    # Tier C: subdivision + section + lot (no block — common for rural / acreage)
    if subdiv and sec and lot:
        out = []
        for k, parcels in index.items():
            if k[0] == subdiv and k[1] == sec and k[2] == lot:
                out.extend(parcels)
        if out:
            return out, "legal_no_block"

    return [], "no_match"


# ═══════════════════════════════════════════════════════════════════════
#  Queue helper — pushes a record into siftstack_queue if eligible
# ═══════════════════════════════════════════════════════════════════════
import uuid as _uuid


def queue_if_eligible(record: dict) -> bool:
    """Queue this record for SIFTstack processing if it has parcel +
    address + owner/grantor AND isn't already in the pipeline.
    Returns True if a new queue row was created."""
    if not record.get("parcel_acct"):
        return False
    if not record.get("prop_address"):
        return False
    if not (record.get("owner") or record.get("grantor_name")):
        return False
    try:
        existing = (
            client.table("siftstack_queue")
            .select("id")
            .eq("record_id", record["id"])
            .in_("status", ["pending", "processing", "completed"])
            .limit(1)
            .execute()
        )
        if existing.data:
            return False
    except Exception:
        return False
    try:
        client.table("siftstack_queue").insert({
            "record_id": record["id"],
            "batch_id":  str(_uuid.uuid4()),
            "queued_by": None,
            "status":    "pending",
        }).execute()
        return True
    except Exception as e:
        log.debug("queue insert failed for %s: %s", record["id"], e)
        return False


# ═══════════════════════════════════════════════════════════════════════
#  Pull all records from Supabase
# ═══════════════════════════════════════════════════════════════════════
def fetch_all_records() -> list[dict]:
    log.info("Loading all records from Supabase ...")
    rows: list[dict] = []
    page = 0
    cols = (
        "id, doc_num, doc_type, legal_description, prop_address, prop_city, "
        "prop_zip, parcel_acct, owner, grantor_name, match_confidence, "
        "address_source, manual_overrides"
    )
    while True:
        resp = (
            client.table("records")
            .select(cols)
            .range(page * 1000, page * 1000 + 999)
            .execute()
        )
        chunk = resp.data or []
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < 1000:
            break
        page += 1
    log.info("  loaded %d records", len(rows))
    return rows


# ═══════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════
def main() -> int:
    log.info("Refresh starting (dry_run=%s, year=%s)", DRY_RUN, HCAD_YEAR)

    index = build_legal_index()
    records = fetch_all_records()

    stats: dict[str, int] = defaultdict(int)
    # Make sure the keys we always report are present even at zero
    for k in ("already_linked", "by_address", "legal_exact",
              "legal_no_section", "legal_no_block",
              "no_legal", "no_match", "ambiguous", "queued"):
        stats[k] = 0

    for rec in records:
        rid = rec["id"]
        overrides = set(rec.get("manual_overrides") or [])

        # 1) Already linked → may still need queueing
        if rec.get("parcel_acct"):
            stats["already_linked"] += 1
            if not DRY_RUN and queue_if_eligible(rec):
                stats["queued"] += 1
            continue

        # 2) Address-based resolution (only if record has an address)
        if rec.get("prop_address"):
            try:
                rpc = client.rpc(
                    "resolve_parcel_by_address",
                    {"p_address": rec["prop_address"], "p_zip": rec.get("prop_zip")},
                ).execute()
                if rpc.data:
                    stats["by_address"] += 1
                    if not DRY_RUN:
                        client.table("records").update({
                            "parcel_acct":      rpc.data,
                            "address_source":   "hcad_address",
                            "match_confidence": "high",
                        }).eq("id", rid).execute()
                        rec["parcel_acct"] = rpc.data
                        if queue_if_eligible(rec):
                            stats["queued"] += 1
                    continue
            except Exception as e:
                log.debug("  address rpc failed for %s: %s", rid, e)

        # 3) Legal-description fallback
        legal = rec.get("legal_description")
        if not legal:
            stats["no_legal"] += 1
            continue
        parsed = parse_clerk_legal(legal)
        if not parsed:
            stats["no_legal"] += 1
            continue

        candidates, tier = find_parcels(index, parsed)
        if not candidates:
            stats["no_match"] += 1
            continue
        if len(candidates) > 1:
            # Disambiguate by zip if record has one
            if rec.get("prop_zip"):
                zip_match = [c for c in candidates if c["situs_zip"] == rec["prop_zip"]]
                if len(zip_match) == 1:
                    candidates = zip_match
            if len(candidates) > 1:
                stats["ambiguous"] += 1
                continue

        parcel = candidates[0]
        stats[tier] += 1

        update = {
            "parcel_acct":      parcel["parcel_acct"],
            "address_source":   "hcad_legal",
            "match_confidence": "high",
        }
        # Only fill missing address fields, and only if the VA hasn't locked them
        if not rec.get("prop_address") and "prop_address" not in overrides:
            update["prop_address"] = parcel["situs_address"]
        if not rec.get("prop_city") and "prop_city" not in overrides:
            update["prop_city"] = parcel["situs_city"] or "Houston"
        if not rec.get("prop_zip") and "prop_zip" not in overrides:
            update["prop_zip"] = parcel["situs_zip"]

        if not DRY_RUN:
            try:
                client.table("records").update(update).eq("id", rid).execute()
                rec.update(update)  # mirror so queue_if_eligible sees latest state
                if queue_if_eligible(rec):
                    stats["queued"] += 1
            except Exception as e:
                log.warning("  update failed for %s: %s", rid, e)

    log.info("=" * 60)
    log.info("Refresh complete (dry_run=%s):", DRY_RUN)
    for k, v in stats.items():
        log.info("  %-22s %d", k + ":", v)
    log.info("  total                  %d", len(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())
