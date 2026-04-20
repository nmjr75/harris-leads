"""HCAD bulk data sync — populates property_data from HCAD's weekly ZIP dumps.

Downloads three files from HCAD's public data portal:
  - Real_acct_owner.zip        → real_acct.txt        (owner + situs + values)
  - Real_building_land.zip     → building_res.txt     (year built + sqft)
                                 fixtures.txt         (bedrooms + bathrooms)

Instead of loading all 1.6M Harris County parcels (exceeds Supabase free-tier
storage), runs in "match mode": only upserts parcels that match an address
already in our records table. Also back-fills records.parcel_acct so the
worker has a fast join on parcel_acct.

Environment variables:
    SUPABASE_URL
    SUPABASE_SECRET_KEY
    HCAD_DATA_YEAR       — default 2026 (folder name on download.hcad.org)
    HCAD_WORK_DIR        — default /tmp/hcad_sync
    SYNC_ALL_PARCELS     — "1" to bypass match mode and load everything (Pro tier only)

Runs standalone or from GitHub Actions cron.
"""
from __future__ import annotations

import logging
import os
import re
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

log = logging.getLogger("hcad_sync")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
HCAD_YEAR    = os.environ.get("HCAD_DATA_YEAR", "2026").strip()
WORK_DIR     = Path(os.environ.get("HCAD_WORK_DIR", "/tmp/hcad_sync"))
SYNC_ALL     = os.environ.get("SYNC_ALL_PARCELS", "").strip() == "1"

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed")
    sys.exit(1)

client = create_client(SUPABASE_URL, SUPABASE_KEY)


HCAD_BASE = f"https://download.hcad.org/data/CAMA/{HCAD_YEAR}"
ZIPS = {
    "Real_acct_owner.zip":    ["real_acct.txt"],
    "Real_building_land.zip": ["building_res.txt", "fixtures.txt"],
}

BATCH_SIZE = 500
UPSERT_CHUNK = 500


# ── Column indices (0-based) from real_acct.txt header (71 cols) ──────
RA_ACCT       = 0
RA_MAILTO     = 2
RA_MAIL_ADDR  = 3
RA_MAIL_CITY  = 5
RA_MAIL_STATE = 6
RA_MAIL_ZIP   = 7
RA_SITE_1     = 17   # e.g. "6120 E OREM RD"
RA_SITE_2     = 18   # e.g. "HOUSTON"
RA_SITE_3     = 19   # e.g. "77048"
RA_STATE_CLS  = 20   # F1/A1/etc
RA_BLD_AR     = 38   # building area (sqft)
RA_LAND_AR    = 39   # land area (sqft)
RA_LAND_VAL   = 43
RA_BLD_VAL    = 44
RA_TOT_APPR   = 48
RA_TOT_MKT    = 49
RA_LGL_1      = 66
RA_LGL_2      = 67
RA_LGL_3      = 68
RA_LGL_4      = 69

# building_res.txt (col 12 = date_erected, col 20 = act_ar, col 21 = heat_ar)
BR_ACCT       = 0
BR_DATE_ERECT = 12
BR_HEAT_AR    = 21

# fixtures.txt (acct, bld_num, type, type_dscr, units)
FX_ACCT       = 0
FX_TYPE_DSCR  = 3
FX_UNITS      = 4


def _norm(addr: str) -> str:
    """Match the Postgres normalized_situs generated column."""
    return re.sub(r"[^A-Za-z0-9]+", "", addr or "").upper()


def _clean(s: str) -> str | None:
    s = (s or "").strip()
    return s if s else None


def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s or s == "0":
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _to_int(s: str) -> int | None:
    v = _to_float(s)
    return int(v) if v is not None else None


def _to_year(date_str: str) -> int | None:
    """HCAD date_erected is YYYY or MM/DD/YYYY — extract year."""
    s = (date_str or "").strip()
    if not s:
        return None
    m = re.search(r"(\d{4})", s)
    if m:
        y = int(m.group(1))
        return y if 1800 <= y <= 2100 else None
    return None


# ── Step 1: Download the ZIPs ─────────────────────────────────────────
def download_zips() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    for zip_name in ZIPS:
        dest = WORK_DIR / zip_name
        if dest.exists() and dest.stat().st_size > 1_000_000:
            log.info("Already downloaded: %s (%.1f MB)", zip_name, dest.stat().st_size / 1e6)
            continue
        url = f"{HCAD_BASE}/{zip_name}"
        log.info("Downloading %s ...", url)
        urlretrieve(url, dest)
        log.info("Saved %s (%.1f MB)", zip_name, dest.stat().st_size / 1e6)


# ── Step 2: Pull candidate addresses from records ─────────────────────
def load_candidates() -> tuple[set[str], dict[str, str]]:
    """Return (normalized_addresses, parcel_acct_set).

    Matches hcad rows against EITHER a known parcel_acct (already linked
    via scraper HCAD step) OR a normalized prop_address.
    """
    if SYNC_ALL:
        log.info("SYNC_ALL_PARCELS=1 — matching disabled, loading every parcel")
        return set(), {}

    log.info("Loading candidate addresses from records table ...")
    addrs: set[str] = set()
    parcel_to_rec: dict[str, str] = {}  # parcel_acct → one of the record IDs

    page = 0
    while True:
        resp = (
            client.table("records")
            .select("id, prop_address, parcel_acct")
            .range(page * 1000, page * 1000 + 999)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            if r.get("prop_address"):
                addrs.add(_norm(r["prop_address"]))
            if r.get("parcel_acct"):
                parcel_to_rec[r["parcel_acct"].strip()] = r["id"]
        if len(rows) < 1000:
            break
        page += 1

    log.info("Loaded %d normalized addresses + %d known parcels", len(addrs), len(parcel_to_rec))
    return addrs, parcel_to_rec


# ── Step 3: Stream real_acct.txt → collect matched rows ───────────────
def collect_real_acct(addrs: set[str], known_parcels: dict[str, str]) -> dict[str, dict]:
    log.info("Scanning real_acct.txt ...")
    out: dict[str, dict] = {}
    seen = 0
    with zipfile.ZipFile(WORK_DIR / "Real_acct_owner.zip") as z:
        with z.open("real_acct.txt") as f:
            # Skip header
            _ = f.readline()
            for raw in f:
                seen += 1
                if seen % 250_000 == 0:
                    log.info("  scanned %d rows, matched %d", seen, len(out))
                try:
                    line = raw.decode("latin-1", errors="replace").rstrip("\n\r")
                except Exception:
                    continue
                parts = line.split("\t")
                if len(parts) < 50:
                    continue

                acct = parts[RA_ACCT].strip()
                if not acct:
                    continue

                site1 = parts[RA_SITE_1].strip() if len(parts) > RA_SITE_1 else ""
                norm  = _norm(site1)

                # Match either by known parcel OR normalized address
                if not SYNC_ALL:
                    if acct not in known_parcels and norm not in addrs:
                        continue

                out[acct] = {
                    "parcel_acct":       acct,
                    "county":            "Harris",
                    "state":             "TX",
                    "situs_address":     _clean(site1),
                    "situs_city":        _clean(parts[RA_SITE_2]) if len(parts) > RA_SITE_2 else None,
                    "situs_zip":         _clean(parts[RA_SITE_3]) if len(parts) > RA_SITE_3 else None,
                    "owner_name":        _clean(parts[RA_MAILTO]) if len(parts) > RA_MAILTO else None,
                    "mail_address":      _clean(parts[RA_MAIL_ADDR]) if len(parts) > RA_MAIL_ADDR else None,
                    "mail_city":         _clean(parts[RA_MAIL_CITY]) if len(parts) > RA_MAIL_CITY else None,
                    "mail_state":        _clean(parts[RA_MAIL_STATE]) if len(parts) > RA_MAIL_STATE else None,
                    "mail_zip":          _clean(parts[RA_MAIL_ZIP]) if len(parts) > RA_MAIL_ZIP else None,
                    "structure_type":    _clean(parts[RA_STATE_CLS]) if len(parts) > RA_STATE_CLS else None,
                    "land_val":          _to_float(parts[RA_LAND_VAL]) if len(parts) > RA_LAND_VAL else None,
                    "bld_val":           _to_float(parts[RA_BLD_VAL]) if len(parts) > RA_BLD_VAL else None,
                    "tot_appr_val":      _to_float(parts[RA_TOT_APPR]) if len(parts) > RA_TOT_APPR else None,
                    "market_value":      _to_float(parts[RA_TOT_MKT]) if len(parts) > RA_TOT_MKT else None,
                    "appraised_value":   _to_float(parts[RA_TOT_APPR]) if len(parts) > RA_TOT_APPR else None,
                    "lot_size_sqft":     _to_int(parts[RA_LAND_AR]) if len(parts) > RA_LAND_AR else None,
                    "sqft":              _to_int(parts[RA_BLD_AR]) if len(parts) > RA_BLD_AR else None,
                    "legal_description": " ".join(
                        (parts[i].strip() for i in (RA_LGL_1, RA_LGL_2, RA_LGL_3, RA_LGL_4)
                         if len(parts) > i and parts[i].strip())
                    ) or None,
                }
    log.info("real_acct.txt: matched %d parcels (scanned %d)", len(out), seen)
    return out


# ── Step 4: Stream building_res.txt → year_built + heated sqft ────────
def enrich_from_building_res(parcels: dict[str, dict]) -> None:
    if not parcels:
        return
    log.info("Scanning building_res.txt for year_built + sqft ...")
    matched = 0
    with zipfile.ZipFile(WORK_DIR / "Real_building_land.zip") as z:
        with z.open("building_res.txt") as f:
            _ = f.readline()
            for raw in f:
                try:
                    parts = raw.decode("latin-1", errors="replace").rstrip("\n\r").split("\t")
                except Exception:
                    continue
                if len(parts) <= BR_HEAT_AR:
                    continue
                acct = parts[BR_ACCT].strip()
                p = parcels.get(acct)
                if p is None:
                    continue
                yr = _to_year(parts[BR_DATE_ERECT])
                if yr and not p.get("year_built"):
                    p["year_built"] = yr
                # Prefer heated area for "sqft"; falls back to building area set earlier
                heat = _to_int(parts[BR_HEAT_AR])
                if heat:
                    p["sqft"] = heat
                matched += 1
    log.info("building_res.txt: enriched %d parcels", matched)


# ── Step 5: Stream fixtures.txt → bedrooms + bathrooms ────────────────
def enrich_from_fixtures(parcels: dict[str, dict]) -> None:
    if not parcels:
        return
    log.info("Scanning fixtures.txt for bedrooms + bathrooms ...")
    # Aggregate across multiple rows per acct
    beds: dict[str, int] = {}
    baths_full: dict[str, int] = {}
    baths_half: dict[str, int] = {}

    with zipfile.ZipFile(WORK_DIR / "Real_building_land.zip") as z:
        with z.open("fixtures.txt") as f:
            _ = f.readline()
            for raw in f:
                try:
                    parts = raw.decode("latin-1", errors="replace").rstrip("\n\r").split("\t")
                except Exception:
                    continue
                if len(parts) <= FX_UNITS:
                    continue
                acct = parts[FX_ACCT].strip()
                if acct not in parcels:
                    continue
                dscr = (parts[FX_TYPE_DSCR] or "").strip().upper()
                units = _to_int(parts[FX_UNITS])
                if units is None:
                    continue
                if "BEDROOM" in dscr:
                    beds[acct] = beds.get(acct, 0) + units
                elif "BATH" in dscr and "FULL" in dscr:
                    baths_full[acct] = baths_full.get(acct, 0) + units
                elif "BATH" in dscr and ("HALF" in dscr or "1/2" in dscr):
                    baths_half[acct] = baths_half.get(acct, 0) + units

    for acct, p in parcels.items():
        if acct in beds:
            p["bedrooms"] = beds[acct]
        bf = baths_full.get(acct, 0)
        bh = baths_half.get(acct, 0)
        if bf or bh:
            p["bathrooms"] = float(bf) + 0.5 * bh

    log.info("fixtures.txt: beds on %d parcels, baths on %d parcels",
             len(beds), len(baths_full) + len(baths_half))


# ── Step 6: Upsert property_data ─────────────────────────────────────
def upsert_parcels(parcels: dict[str, dict]) -> None:
    if not parcels:
        log.warning("No parcels matched — nothing to upsert")
        return
    log.info("Upserting %d parcels to property_data ...", len(parcels))
    rows = list(parcels.values())
    done = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        batch = rows[i:i + UPSERT_CHUNK]
        try:
            client.table("property_data").upsert(batch, on_conflict="parcel_acct").execute()
            done += len(batch)
        except Exception as e:
            log.warning("upsert batch %d failed: %s", i // UPSERT_CHUNK, e)
    log.info("Upserted %d property_data rows", done)


# ── Step 7: Back-fill records.parcel_acct for matched addresses ───────
def backfill_record_parcels(addrs: set[str]) -> None:
    """For every record whose normalized address now has a matching parcel,
    set records.parcel_acct. Uses the resolve_parcel_by_address function."""
    if not addrs:
        return
    log.info("Back-filling records.parcel_acct from property_data matches ...")
    updated = 0
    page = 0
    while True:
        resp = (
            client.table("records")
            .select("id, prop_address, prop_zip, parcel_acct")
            .is_("parcel_acct", "null")
            .range(page * 500, page * 500 + 499)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            addr = r.get("prop_address") or ""
            if not addr:
                continue
            try:
                rpc = client.rpc(
                    "resolve_parcel_by_address",
                    {"p_address": addr, "p_zip": r.get("prop_zip")},
                ).execute()
                acct = rpc.data
                if acct:
                    client.table("records").update({"parcel_acct": acct}).eq("id", r["id"]).execute()
                    updated += 1
            except Exception as e:
                log.debug("resolve failed for %s: %s", addr, e)
        if len(rows) < 500:
            break
        page += 1
    log.info("Back-filled parcel_acct on %d records", updated)


# ── Main ───────────────────────────────────────────────────────────────
def main() -> int:
    download_zips()
    addrs, known_parcels = load_candidates()
    if not SYNC_ALL and not addrs and not known_parcels:
        log.warning("No candidate addresses/parcels — nothing to sync")
        return 0

    parcels = collect_real_acct(addrs, known_parcels)
    if not parcels:
        log.warning("Zero parcels matched in real_acct.txt — check normalization")
        return 0

    enrich_from_building_res(parcels)
    enrich_from_fixtures(parcels)
    upsert_parcels(parcels)
    backfill_record_parcels(addrs)

    log.info("HCAD bulk sync complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
