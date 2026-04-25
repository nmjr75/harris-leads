"""HCAD bulk data sync — loads the full Harris County property database
into Supabase. Runs weekly via GitHub Actions.

Downloads four ZIPs from HCAD's public data portal:
  - Real_acct_owner.zip        → real_acct.txt
  - Real_building_land.zip     → building_res.txt + fixtures.txt
  - Code_description_real.zip  → 26 decode tables
  - Real_jur_exempt.zip        → jur_exempt.txt (exemptions per parcel)

Loads every Harris County parcel (~1.6M) into property_data with
denormalized labels (state class, school district, market area) and
flat boolean flags for key exemptions (homestead, over-65, disability,
disabled vet). Also loads all 26 decode tables into the
hcad_code_descriptions reference table for lookup by anything else.

Environment variables:
    SUPABASE_URL
    SUPABASE_SECRET_KEY
    HCAD_DATA_YEAR       — default 2026 (folder name on download.hcad.org)
    HCAD_WORK_DIR        — default /tmp/hcad_sync
    SYNC_ALL_PARCELS     — "1" to load every parcel (default); "0" to
                           restrict to parcels matching a record address
                           (legacy free-tier behavior — no longer needed
                           now that we're on Supabase Pro).

Runs standalone or from GitHub Actions cron.
"""
from __future__ import annotations

import logging
import os
import re
import sys
import zipfile
from datetime import datetime
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
# Default to full-county load. "0" opts into the legacy match-mode filter.
SYNC_ALL     = os.environ.get("SYNC_ALL_PARCELS", "1").strip() != "0"

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
    "Real_acct_owner.zip":       ["real_acct.txt"],
    "Real_building_land.zip":    ["building_res.txt", "fixtures.txt"],
    "Code_description_real.zip": [],   # we iterate all files in this ZIP
    "Real_jur_exempt.zip":       ["jur_exempt.txt"],
}

# Batch sizes: 1,000 per upsert keeps under Supabase's payload limit and
# gives a reasonable progress cadence on 1.6M rows.
UPSERT_CHUNK = 1000


# ── real_acct.txt column indices (71 cols, verified against header) ──
RA_ACCT        = 0
RA_MAILTO      = 2
RA_MAIL_ADDR   = 3
RA_MAIL_CITY   = 5
RA_MAIL_STATE  = 6
RA_MAIL_ZIP    = 7
RA_SITE_1      = 17
RA_SITE_2      = 18
RA_SITE_3      = 19
RA_STATE_CLS   = 20
RA_SCHOOL_DIST = 21
RA_MAP_FACET   = 22
RA_KEY_MAP     = 23
RA_NGBR_CODE   = 24
RA_NGBR_GRP    = 25
RA_MKT_AREA    = 26
RA_MKT_AREA_D  = 27
RA_BLD_AR      = 38
RA_LAND_AR     = 39
RA_ACREAGE     = 40
RA_LAND_VAL    = 43
RA_BLD_VAL     = 44
RA_ASSESS_VAL  = 47
RA_TOT_APPR    = 48
RA_TOT_MKT     = 49
RA_PRIOR_APPR  = 54
RA_PRIOR_MKT   = 55
RA_NEW_OWN_DT  = 65
RA_LGL_1       = 66
RA_LGL_2       = 67
RA_LGL_3       = 68
RA_LGL_4       = 69

# building_res.txt
BR_ACCT       = 0
BR_DATE_ERECT = 12
BR_HEAT_AR    = 21

# fixtures.txt
FX_ACCT      = 0
FX_TYPE_DSCR = 3
FX_UNITS     = 4

# jur_exempt.txt
JE_ACCT       = 0
JE_EXEMPT_CAT = 2

# ── Exemption category → flat flag mapping ────────────────────────
# Sourced from Real_jur_exempt.zip/jur_exemption_dscr.txt.
#
# NOTE: Over-65 exemption codes (OVR/APO/POV/SUR) are DEFINED in
# jur_exemption_dscr.txt but do NOT appear in the bulk jur_exempt.txt
# or jur_exempt_cd.txt files that HCAD publishes publicly — likely
# withheld because over-65 status is DOB-derived PII. The has_over65
# column is kept in the schema for future sourcing but will remain
# False for every parcel populated from this bulk sync.
HOMESTEAD_CATS = {"RES", "PAR"}
OVER65_CATS    = {"OVR", "APO", "POV", "SUR"}  # not present in bulk data — see note above
DISABLED_CATS  = {"DIS", "APD", "PDS"}
DISABLED_VET_CATS = {
    "V11", "V12", "V13", "V14",
    "V21", "V22", "V23", "V24",
    "VCH", "VTX", "STX",
    "VS1", "VS2", "VS3", "VS4",
    "SSP", "SST",
}

# ── Code-description file → hcad_code_descriptions.code_type ──
# Only simple (code → description) maps are listed here. Two files
# in Code_description_real.zip — desc_r_06 and desc_r_16 — are
# context-keyed (the same code repeats with different descriptions
# per context), which can't be stored in our simple (code_type, code)
# composite key. They're skipped; we don't use them for denormalization.
DECODE_FILES = {
    "desc_r_01_state_class.txt":         "state_class",
    "desc_r_02_building_type_code.txt":  "building_type",
    "desc_r_03_building_style.txt":      "building_style",
    "desc_r_04_building_class.txt":      "building_class",
    "desc_r_05_building_data_elements.txt": "building_data_elements",
    "desc_r_07_quality_code.txt":        "quality",
    "desc_r_08_pgi_category.txt":        "pgi_category",
    "desc_r_09_subarea_type.txt":        "subarea_type",
    "desc_r_10_extra_features.txt":      "extra_features",
    "desc_r_11_extra_feature_category.txt": "extra_feature_category",
    "desc_r_12_real_jurisdictions.txt":  "jurisdiction",
    "desc_r_13_real_jurisdiction_type.txt": "jurisdiction_type",
    "desc_r_14_exemption_category.txt":  "exemption_category",
    "desc_r_15_land_usecode.txt":        "land_use",
    "desc_r_17_relationship_type.txt":   "relationship_type",
    "desc_r_18_permit_status.txt":       "permit_status",
    "desc_r_19_permit_code.txt":         "permit_code",
    "desc_r_20_school_district.txt":     "school_district",
    "desc_r_21_market_area.txt":         "market_area",
    "desc_r_22_hisd_section.txt":        "hisd_section",
    "desc_r_23_special_codes.txt":       "special_code",
    "desc_r_24_agent_roles.txt":         "agent_role",
    "desc_r_25_conclusion_code.txt":     "conclusion_code",
    "desc_r_26_neighborhood_num_adjust.txt": "neighborhood_adjust",
}


# ═══════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════
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


_INT32_MAX = 2_147_483_647


def _clamp_numeric(v: float | None, max_abs: float) -> float | None:
    """Drop values whose absolute magnitude exceeds a numeric column's
    precision. Used for HCAD fields that occasionally have garbage
    giant values (e.g. acreage > 1M)."""
    if v is None:
        return None
    if abs(v) > max_abs:
        return None
    return v


def _to_int(s: str) -> int | None:
    """Convert to int, dropping values that overflow Postgres `integer`.
    HCAD occasionally has garbage giant lot_size values (~2B sqft)."""
    v = _to_float(s)
    if v is None:
        return None
    iv = int(v)
    if iv > _INT32_MAX or iv < -_INT32_MAX - 1:
        return None
    return iv


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


def _to_iso_date(s: str) -> str | None:
    """HCAD new_own_dt is MM/DD/YYYY or empty — return ISO YYYY-MM-DD."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _split_line(raw: bytes) -> list[str] | None:
    try:
        return raw.decode("latin-1", errors="replace").rstrip("\n\r").split("\t")
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════
#  Step 1. Download all four ZIPs
# ═══════════════════════════════════════════════════════════════════════
def download_zips() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    for zip_name in ZIPS:
        dest = WORK_DIR / zip_name
        if dest.exists() and dest.stat().st_size > 10_000:
            log.info("Already downloaded: %s (%.1f MB)",
                     zip_name, dest.stat().st_size / 1e6)
            continue
        url = f"{HCAD_BASE}/{zip_name}"
        log.info("Downloading %s ...", url)
        urlretrieve(url, dest)
        log.info("Saved %s (%.1f MB)",
                 zip_name, dest.stat().st_size / 1e6)


# ═══════════════════════════════════════════════════════════════════════
#  Step 2. Load decode tables → hcad_code_descriptions + in-memory lookup
# ═══════════════════════════════════════════════════════════════════════
def load_decode_tables() -> dict[str, dict[str, str]]:
    """Load every desc_r_*.txt file from Code_description_real.zip into:
       (a) hcad_code_descriptions reference table in Supabase
       (b) in-memory dict { code_type: { code: description } } for
           label denormalization during property_data upsert.
    """
    log.info("Loading HCAD decode tables ...")
    lookups: dict[str, dict[str, str]] = {}
    # Use a dict keyed by (code_type, code) so any duplicates across or
    # within files resolve to "last seen wins" instead of being handed
    # to Postgres (which rejects ON CONFLICT batches with duplicate keys).
    keyed: dict[tuple[str, str], dict] = {}

    with zipfile.ZipFile(WORK_DIR / "Code_description_real.zip") as zf:
        for fname, code_type in DECODE_FILES.items():
            try:
                with zf.open(fname) as f:
                    _ = f.readline()  # header
                    pairs: dict[str, str] = {}
                    for raw in f:
                        parts = _split_line(raw)
                        if not parts or len(parts) < 2:
                            continue
                        code = (parts[0] or "").strip()
                        dscr = (parts[-1] or "").strip()
                        if not code or not dscr:
                            continue
                        pairs[code] = dscr
                        keyed[(code_type, code)] = {
                            "code_type":   code_type,
                            "code":        code,
                            "description": dscr,
                        }
                    lookups[code_type] = pairs
            except KeyError:
                log.warning("  missing: %s", fname)
                continue

    rows_to_upsert = list(keyed.values())
    log.info("  decoded %d types, %d unique code→description pairs",
             len(lookups), len(rows_to_upsert))

    # Upsert reference table in chunks
    done = 0
    for i in range(0, len(rows_to_upsert), UPSERT_CHUNK):
        batch = rows_to_upsert[i:i + UPSERT_CHUNK]
        try:
            client.table("hcad_code_descriptions").upsert(
                batch, on_conflict="code_type,code"
            ).execute()
            done += len(batch)
        except Exception as e:
            log.warning("  hcad_code_descriptions batch %d failed: %s",
                        i // UPSERT_CHUNK, e)
    log.info("  upserted %d hcad_code_descriptions rows", done)

    return lookups


# ═══════════════════════════════════════════════════════════════════════
#  Step 3. Load candidate set (when match-mode is enabled)
# ═══════════════════════════════════════════════════════════════════════
def load_candidates() -> tuple[set[str], set[str]]:
    """Return (normalized_addresses, parcel_acct_set) for match-mode.
    Only called when SYNC_ALL is False."""
    log.info("Match-mode: loading candidate addresses from records ...")
    addrs: set[str] = set()
    parcels: set[str] = set()

    page = 0
    while True:
        resp = (
            client.table("records")
            .select("prop_address, parcel_acct")
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
                parcels.add(r["parcel_acct"].strip())
        if len(rows) < 1000:
            break
        page += 1

    log.info("  %d addresses + %d parcels", len(addrs), len(parcels))
    return addrs, parcels


# ═══════════════════════════════════════════════════════════════════════
#  Step 4. Stream real_acct.txt → build parcels dict
# ═══════════════════════════════════════════════════════════════════════
def collect_real_acct(
    addrs: set[str],
    known_parcels: set[str],
    lookups: dict[str, dict[str, str]],
    now_iso: str,
) -> dict[str, dict]:
    log.info("Scanning real_acct.txt ...")
    state_class_map     = lookups.get("state_class", {})
    school_district_map = lookups.get("school_district", {})
    market_area_map     = lookups.get("market_area", {})

    out: dict[str, dict] = {}
    seen = 0
    with zipfile.ZipFile(WORK_DIR / "Real_acct_owner.zip") as z:
        with z.open("real_acct.txt") as f:
            _ = f.readline()
            for raw in f:
                seen += 1
                if seen % 250_000 == 0:
                    log.info("  scanned %d rows, matched %d",
                             seen, len(out))
                parts = _split_line(raw)
                if not parts or len(parts) < 50:
                    continue
                acct = parts[RA_ACCT].strip()
                if not acct:
                    continue

                site1 = parts[RA_SITE_1].strip() if len(parts) > RA_SITE_1 else ""

                # Match-mode filter
                if not SYNC_ALL:
                    if acct not in known_parcels and _norm(site1) not in addrs:
                        continue

                state_cls = _clean(parts[RA_STATE_CLS]) if len(parts) > RA_STATE_CLS else None
                school_cd = _clean(parts[RA_SCHOOL_DIST]) if len(parts) > RA_SCHOOL_DIST else None
                market_cd = _clean(parts[RA_MKT_AREA]) if len(parts) > RA_MKT_AREA else None
                market_d  = _clean(parts[RA_MKT_AREA_D]) if len(parts) > RA_MKT_AREA_D else None

                out[acct] = {
                    "parcel_acct":            acct,
                    "county":                 "Harris",
                    "state":                  "TX",
                    "situs_address":          _clean(site1),
                    "situs_city":             _clean(parts[RA_SITE_2]) if len(parts) > RA_SITE_2 else None,
                    "situs_zip":              _clean(parts[RA_SITE_3]) if len(parts) > RA_SITE_3 else None,
                    "owner_name":             _clean(parts[RA_MAILTO]) if len(parts) > RA_MAILTO else None,
                    "mail_address":           _clean(parts[RA_MAIL_ADDR]) if len(parts) > RA_MAIL_ADDR else None,
                    "mail_city":              _clean(parts[RA_MAIL_CITY]) if len(parts) > RA_MAIL_CITY else None,
                    "mail_state":             _clean(parts[RA_MAIL_STATE]) if len(parts) > RA_MAIL_STATE else None,
                    "mail_zip":               _clean(parts[RA_MAIL_ZIP]) if len(parts) > RA_MAIL_ZIP else None,
                    "structure_type":         state_cls,
                    "state_class_label":      state_class_map.get(state_cls) if state_cls else None,
                    "school_district_code":   school_cd,
                    "school_district_label":  school_district_map.get(school_cd) if school_cd else None,
                    "neighborhood_code":      _clean(parts[RA_NGBR_CODE]) if len(parts) > RA_NGBR_CODE else None,
                    "neighborhood_group":     _clean(parts[RA_NGBR_GRP]) if len(parts) > RA_NGBR_GRP else None,
                    "market_area_code":       market_cd,
                    "market_area_label":      market_d or (market_area_map.get(market_cd) if market_cd else None),
                    "map_facet":              _clean(parts[RA_MAP_FACET]) if len(parts) > RA_MAP_FACET else None,
                    "key_map":                _clean(parts[RA_KEY_MAP]) if len(parts) > RA_KEY_MAP else None,
                    "land_val":               _to_float(parts[RA_LAND_VAL]) if len(parts) > RA_LAND_VAL else None,
                    "bld_val":                _to_float(parts[RA_BLD_VAL]) if len(parts) > RA_BLD_VAL else None,
                    "tot_appr_val":           _to_float(parts[RA_TOT_APPR]) if len(parts) > RA_TOT_APPR else None,
                    "market_value":           _to_float(parts[RA_TOT_MKT]) if len(parts) > RA_TOT_MKT else None,
                    "appraised_value":        _to_float(parts[RA_TOT_APPR]) if len(parts) > RA_TOT_APPR else None,
                    "assessed_val":           _to_float(parts[RA_ASSESS_VAL]) if len(parts) > RA_ASSESS_VAL else None,
                    "prior_tot_appr_val":     _to_float(parts[RA_PRIOR_APPR]) if len(parts) > RA_PRIOR_APPR else None,
                    "prior_tot_mkt_val":      _to_float(parts[RA_PRIOR_MKT]) if len(parts) > RA_PRIOR_MKT else None,
                    "lot_size_sqft":          _to_int(parts[RA_LAND_AR]) if len(parts) > RA_LAND_AR else None,
                    "acreage":                _clamp_numeric(_to_float(parts[RA_ACREAGE]), 999_999.9999) if len(parts) > RA_ACREAGE else None,
                    "sqft":                   _to_int(parts[RA_BLD_AR]) if len(parts) > RA_BLD_AR else None,
                    "new_owner_date":         _to_iso_date(parts[RA_NEW_OWN_DT]) if len(parts) > RA_NEW_OWN_DT else None,
                    "legal_description":      " ".join(
                        parts[i].strip()
                        for i in (RA_LGL_1, RA_LGL_2, RA_LGL_3, RA_LGL_4)
                        if len(parts) > i and parts[i].strip()
                    ) or None,
                    "has_homestead":          False,
                    "has_over65":             False,
                    "has_disabled":           False,
                    "has_disabled_vet":       False,
                    "last_synced_at":         now_iso,
                }
    log.info("real_acct.txt: collected %d parcels (scanned %d total)",
             len(out), seen)
    return out


# ═══════════════════════════════════════════════════════════════════════
#  Step 5. Stream building_res.txt → year_built + heated sqft
# ═══════════════════════════════════════════════════════════════════════
def enrich_from_building_res(parcels: dict[str, dict]) -> None:
    if not parcels:
        return
    log.info("Scanning building_res.txt for year_built + sqft ...")
    matched = 0
    with zipfile.ZipFile(WORK_DIR / "Real_building_land.zip") as z:
        with z.open("building_res.txt") as f:
            _ = f.readline()
            for raw in f:
                parts = _split_line(raw)
                if not parts or len(parts) <= BR_HEAT_AR:
                    continue
                acct = parts[BR_ACCT].strip()
                p = parcels.get(acct)
                if p is None:
                    continue
                yr = _to_year(parts[BR_DATE_ERECT])
                if yr and not p.get("year_built"):
                    p["year_built"] = yr
                heat = _to_int(parts[BR_HEAT_AR])
                if heat:
                    p["sqft"] = heat
                matched += 1
    log.info("  enriched %d parcels with building data", matched)


# ═══════════════════════════════════════════════════════════════════════
#  Step 6. Stream fixtures.txt → bedrooms + bathrooms
# ═══════════════════════════════════════════════════════════════════════
def enrich_from_fixtures(parcels: dict[str, dict]) -> None:
    if not parcels:
        return
    log.info("Scanning fixtures.txt for bedrooms + bathrooms ...")
    beds: dict[str, int] = {}
    baths_full: dict[str, int] = {}
    baths_half: dict[str, int] = {}

    with zipfile.ZipFile(WORK_DIR / "Real_building_land.zip") as z:
        with z.open("fixtures.txt") as f:
            _ = f.readline()
            for raw in f:
                parts = _split_line(raw)
                if not parts or len(parts) <= FX_UNITS:
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
            # bedrooms is an int column; clamp at int32
            p["bedrooms"] = min(beds[acct], _INT32_MAX)
        bf = baths_full.get(acct, 0)
        bh = baths_half.get(acct, 0)
        if bf or bh:
            # bathrooms column is numeric(3,1) so max is 99.9. HCAD
            # fixture aggregation over multi-building parcels can
            # produce totals > 100; clamp so the upsert doesn't fail.
            total = float(bf) + 0.5 * bh
            p["bathrooms"] = min(total, 99.5)

    log.info("  beds on %d parcels, baths on %d parcels",
             len(beds), len(baths_full) + len(baths_half))


# ═══════════════════════════════════════════════════════════════════════
#  Step 7. Stream jur_exempt.txt → flat exemption flags
# ═══════════════════════════════════════════════════════════════════════
def enrich_from_exemptions(parcels: dict[str, dict]) -> None:
    if not parcels:
        return
    log.info("Scanning jur_exempt.txt for exemption flags ...")
    seen = 0
    matched = 0
    touched: set[str] = set()
    with zipfile.ZipFile(WORK_DIR / "Real_jur_exempt.zip") as z:
        with z.open("jur_exempt.txt") as f:
            _ = f.readline()
            for raw in f:
                seen += 1
                if seen % 500_000 == 0:
                    log.info("  exempt scanned %d rows, matched %d parcels",
                             seen, len(touched))
                parts = _split_line(raw)
                if not parts or len(parts) <= JE_EXEMPT_CAT:
                    continue
                acct = parts[JE_ACCT].strip()
                cat  = (parts[JE_EXEMPT_CAT] or "").strip().upper()
                p = parcels.get(acct)
                if p is None:
                    continue
                matched += 1
                if cat in HOMESTEAD_CATS:
                    p["has_homestead"] = True
                elif cat in OVER65_CATS:
                    p["has_over65"] = True
                elif cat in DISABLED_CATS:
                    p["has_disabled"] = True
                elif cat in DISABLED_VET_CATS:
                    p["has_disabled_vet"] = True
                touched.add(acct)
    log.info("  jur_exempt: scanned %d rows, matched %d (touched %d parcels)",
             seen, matched, len(touched))


# ═══════════════════════════════════════════════════════════════════════
#  Step 8. Upsert property_data in chunks
# ═══════════════════════════════════════════════════════════════════════
def upsert_parcels(parcels: dict[str, dict]) -> None:
    if not parcels:
        log.warning("No parcels to upsert")
        return
    log.info("Upserting %d parcels to property_data ...", len(parcels))
    # Pre-upsert flag tally — useful for verifying enrich_from_exemptions
    flag_sums = {
        "has_homestead":    sum(1 for p in parcels.values() if p.get("has_homestead")),
        "has_over65":       sum(1 for p in parcels.values() if p.get("has_over65")),
        "has_disabled":     sum(1 for p in parcels.values() if p.get("has_disabled")),
        "has_disabled_vet": sum(1 for p in parcels.values() if p.get("has_disabled_vet")),
    }
    log.info("  pre-upsert flag tally: %s", flag_sums)
    rows = list(parcels.values())
    done = 0
    failed = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        batch = rows[i:i + UPSERT_CHUNK]
        try:
            client.table("property_data").upsert(
                batch, on_conflict="parcel_acct"
            ).execute()
            done += len(batch)
            if (done // UPSERT_CHUNK) % 20 == 0:
                log.info("  upserted %d / %d", done, len(rows))
        except Exception as e:
            failed += len(batch)
            log.warning("  upsert chunk %d failed (%d rows): %s",
                        i // UPSERT_CHUNK, len(batch), e)
    log.info("Upsert complete: %d succeeded, %d failed", done, failed)


# ═══════════════════════════════════════════════════════════════════════
#  Step 9. Back-fill records.parcel_acct via address match
# ═══════════════════════════════════════════════════════════════════════
def backfill_record_parcels() -> None:
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
                    client.table("records").update(
                        {"parcel_acct": acct}
                    ).eq("id", r["id"]).execute()
                    updated += 1
            except Exception as e:
                log.debug("  resolve failed for %s: %s", addr, e)
        if len(rows) < 500:
            break
        page += 1
    log.info("  back-filled %d records", updated)


# ═══════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════
def main() -> int:
    now_iso = datetime.utcnow().isoformat() + "Z"
    log.info("HCAD bulk sync starting (year=%s, sync_all=%s)",
             HCAD_YEAR, SYNC_ALL)

    download_zips()
    lookups = load_decode_tables()

    if SYNC_ALL:
        addrs, known_parcels = set(), set()
    else:
        addrs, known_parcels = load_candidates()
        if not addrs and not known_parcels:
            log.warning("Match mode: no candidates — nothing to sync")
            return 0

    parcels = collect_real_acct(addrs, known_parcels, lookups, now_iso)
    if not parcels:
        log.warning("Zero parcels collected — check normalization")
        return 0

    enrich_from_building_res(parcels)
    enrich_from_fixtures(parcels)
    enrich_from_exemptions(parcels)
    upsert_parcels(parcels)
    backfill_record_parcels()

    log.info("HCAD bulk sync complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
