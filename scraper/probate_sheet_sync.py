"""Google Sheets → Supabase sync for EstateTrace probate data.

Reads the 'Harris County Probate - Miguel' workbook (sheet ID set in
PROBATE_SHEET_ID env var), discovers all tabs matching the
{County}_County_TX_{YYYY}-{MM}[-LAST_REVISION] pattern, normalizes
each row, and upserts to public.records.

READ-ONLY GUARANTEES (three independent layers):
  1. The sheet must be shared with the service account as VIEWER only.
     Google enforces this at the platform level — write attempts return
     403 Forbidden regardless of what the code does.
  2. The OAuth scope below is 'spreadsheets.readonly'. The token cannot
     authorize write API calls.
  3. This script never calls any write method. Only spreadsheets.get()
     and spreadsheets.values.batchGet().

Environment variables:
    PROBATE_SHEET_ID                        — Google Sheet ID (from URL)
    GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON      — service account key (full JSON string)
    SUPABASE_URL                            — same as other scrapers
    SUPABASE_SECRET_KEY                     — same as other scrapers (service-role)

Optional:
    PROBATE_SYNC_DRY_RUN=1                  — print what would be upserted, no DB writes
    PROBATE_SYNC_TAB_FILTER=Fortbend        — only sync tabs whose name contains this string

Tab handling:
    Tabs matching the pattern are sorted so any '-LAST_REVISION' versions
    are processed AFTER their plain counterpart. Since upsert conflicts on
    doc_num, the LAST_REVISION values overwrite the un-suffixed ones for
    the same case. Result: LAST_REVISION wins when both tabs exist.

Run via .github/workflows/probate-sync.yml (cron + workflow_dispatch) OR
locally: python scraper/probate_sheet_sync.py
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Iterable, Optional

log = logging.getLogger(__name__)

# Sheet structure constants
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
EXPECTED_HEADERS = [
    "Case Number", "Case Date", "Status",
    "Deceased First Name", "Deceased Last Name",
    "Property Street", "Property City", "Property State", "Property Zipcode",
    "App 1 First Name", "App 1 Last Name",
    "App 1 Street", "App 1 City", "App 1 State", "App 1 Zipcode",
    "App 2 First Name", "App 2 Last Name",
    "App 2 Street", "App 2 City", "App 2 State", "App 2 Zipcode",
]

# Pattern: Harris_County_TX_2026-04 OR Fortbend_County_TX_2026-04-LAST_REVISION
TAB_PATTERN = re.compile(
    r"^(?P<county>[A-Za-z]+)_County_TX_(?P<year>\d{4})-(?P<month>\d{2})"
    r"(?P<suffix>-LAST_REVISION)?$"
)

BATCH_SIZE = 500

# Spanish status → English equivalent so the dashboard can filter consistently
STATUS_TRANSLATION = {
    "SIN PROPIEDAD ENCONTRADA": "no_property_found",
    "ACTIVE":                   "active",
    "PENDING":                  "pending",
    "":                         None,
}


# ── Value cleaners ─────────────────────────────────────────────────────────────
def _clean(value) -> Optional[str]:
    """Return None for blank/whitespace-only values; otherwise stripped string."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _parse_date(value) -> Optional[str]:
    """Parse MM/DD/YYYY, M/D/YYYY, or YYYY-MM-DD → ISO date string."""
    s = _clean(value)
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%-m/%-d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except (ValueError, TypeError):
            continue
    log.debug(f"Could not parse date: {value!r}")
    return None


def _normalize_status(value) -> Optional[str]:
    s = _clean(value)
    if s is None:
        return None
    return STATUS_TRANSLATION.get(s.upper(), s.lower())


def _join_name(first, last) -> Optional[str]:
    parts = [_clean(first), _clean(last)]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


# ── Tab parsing ────────────────────────────────────────────────────────────────
def _parse_tab_name(tab_name: str) -> Optional[dict]:
    """Return {county, year, month, is_last_revision} or None if not a target tab."""
    m = TAB_PATTERN.match(tab_name)
    if not m:
        return None
    return {
        "county":           m.group("county"),
        "year":             m.group("year"),
        "month":            m.group("month"),
        "is_last_revision": bool(m.group("suffix")),
    }


def _normalize_county(raw: str) -> str:
    """'Harris' → 'Harris', 'Fortbend' → 'Fortbend'. Single source of truth for
    the county name we write to Supabase, so dashboard filters are stable."""
    raw = (raw or "").strip()
    if raw.lower() in ("fortbend", "fort_bend", "fort bend"):
        return "Fortbend"
    if raw.lower() == "harris":
        return "Harris"
    return raw  # unknown counties pass through verbatim


# ── Row normalizer ─────────────────────────────────────────────────────────────
def _row_to_record(
    headers: list[str],
    row: list,
    tab_name: str,
    county: str,
    synced_at: str,
) -> Optional[dict]:
    """Map one sheet row → one records-table dict.

    Returns None if the row has no Case Number (unique key is required).
    """
    # Pad row to header length so missing trailing cells don't IndexError
    row = list(row) + [""] * (len(headers) - len(row))
    cells = dict(zip(headers, row))

    case_number = _clean(cells.get("Case Number"))
    if not case_number:
        return None

    deceased = _join_name(
        cells.get("Deceased First Name"),
        cells.get("Deceased Last Name"),
    )
    app1 = _join_name(
        cells.get("App 1 First Name"),
        cells.get("App 1 Last Name"),
    )

    # App 2 (and beyond) — captured as structured JSONB so future App 3+
    # adds zero schema work
    additional_applicants: list[dict] = []
    app2_first = _clean(cells.get("App 2 First Name"))
    app2_last = _clean(cells.get("App 2 Last Name"))
    if app2_first or app2_last:
        additional_applicants.append({
            "first_name": app2_first,
            "last_name":  app2_last,
            "street":     _clean(cells.get("App 2 Street")),
            "city":       _clean(cells.get("App 2 City")),
            "state":      _clean(cells.get("App 2 State")),
            "zip":        _clean(cells.get("App 2 Zipcode")),
        })

    return {
        "doc_num":                case_number,
        "doc_type":               "PROB",
        "cat":                    "PROBATE",
        "cat_label":              "Probate (EstateTrace)",
        "source":                 "estatetrace",
        "county":                 county,
        "state":                  "TX",
        "filed_date":             _parse_date(cells.get("Case Date")),
        "case_status":            _normalize_status(cells.get("Status")),

        # Deceased = grantor (the property owner who died)
        "grantor_name":           deceased,

        # Property address (the deal target)
        "prop_address":           _clean(cells.get("Property Street")),
        "prop_city":              _clean(cells.get("Property City")),
        "prop_state":             _clean(cells.get("Property State")) or "TX",
        "prop_zip":               _clean(cells.get("Property Zipcode")),

        # PR / Applicant 1 = owner (the marketing target)
        "owner":                  app1,
        "mail_address":           _clean(cells.get("App 1 Street")),
        "mail_city":              _clean(cells.get("App 1 City")),
        "mail_state":             _clean(cells.get("App 1 State")) or "TX",
        "mail_zip":               _clean(cells.get("App 1 Zipcode")),

        # Applicant 2+
        "additional_applicants":  additional_applicants if additional_applicants else None,

        # Audit
        "sheet_tab_name":         tab_name,
        "sheet_synced_at":        synced_at,
    }


# ── Google Sheets client ───────────────────────────────────────────────────────
def _build_sheets_client():
    """Build a read-only Google Sheets client from a service-account JSON.

    Returns the .spreadsheets() resource, or None if creds aren't configured
    (so the script silent-no-ops in dev).
    """
    raw_key = os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_key:
        log.error("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON not set")
        return None

    try:
        info = json.loads(raw_key)
    except json.JSONDecodeError as e:
        log.error(f"GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON is not valid JSON: {e}")
        return None

    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        log.error("google-api-python-client / google-auth not installed. "
                  "Add: google-api-python-client>=2.110.0 google-auth>=2.27.0")
        return None

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    # cache_discovery=False suppresses an unrelated warning on Cloud-deploy environments
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service.spreadsheets()


# ── Sync core ──────────────────────────────────────────────────────────────────
def _list_target_tabs(sheets, sheet_id: str, name_filter: Optional[str] = None
                      ) -> list[dict]:
    """Return a list of tabs matching {County}_County_TX_{YYYY}-{MM}[-LAST_REVISION].

    Sorted so non-LAST_REVISION versions come BEFORE LAST_REVISION versions
    of the same county+month. With upsert-on-doc_num, this means LAST_REVISION
    wins when both tabs exist.
    """
    meta = sheets.get(spreadsheetId=sheet_id, fields="sheets(properties)").execute()
    targets = []
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        title = props.get("title", "")
        parsed = _parse_tab_name(title)
        if not parsed:
            continue
        if name_filter and name_filter.lower() not in title.lower():
            continue
        targets.append({"title": title, **parsed})

    # Sort key: (county, year, month, is_last_revision) — LAST_REVISION sorts last
    # within the same county+month, so it overwrites in upsert order.
    targets.sort(key=lambda t: (t["county"], t["year"], t["month"], t["is_last_revision"]))
    return targets


def _read_tab(sheets, sheet_id: str, tab_title: str) -> tuple[list[str], list[list]]:
    """Return (headers, data_rows). Empty lists if tab is empty."""
    # Use A:Z to cover the 21-column probate schema with margin for stray columns
    resp = sheets.values().batchGet(
        spreadsheetId=sheet_id,
        ranges=[f"'{tab_title}'!A:Z"],
    ).execute()
    value_ranges = resp.get("valueRanges", [])
    if not value_ranges:
        return [], []
    rows = value_ranges[0].get("values", [])
    if not rows:
        return [], []
    headers = [str(h).strip() for h in rows[0]]
    return headers, rows[1:]


def _validate_headers(headers: list[str], tab_title: str) -> bool:
    """Warn (don't fail) on missing critical headers. EstateTrace may add columns
    over time; we only require the bare minimum needed to write a row."""
    required = ["Case Number", "Deceased Last Name", "App 1 First Name", "App 1 Last Name"]
    missing = [h for h in required if h not in headers]
    if missing:
        log.warning(f"Tab '{tab_title}' missing required headers: {missing} "
                    f"— rows from this tab may not normalize correctly")
        return False
    return True


def sync(dry_run: bool = False, tab_filter: Optional[str] = None) -> dict:
    """Run the sync once. Returns counts dict.

    On any infrastructure failure (auth, missing config), logs the error and
    returns zeros. This function never raises — the workflow is configured to
    fail loudly on its own if the script exits non-zero, so we only exit
    non-zero on truly unrecoverable conditions.
    """
    counts = {"tabs_seen": 0, "rows_seen": 0, "rows_skipped": 0,
              "rows_upserted": 0, "errors": 0}

    sheet_id = (os.environ.get("PROBATE_SHEET_ID") or "").strip()
    if not sheet_id:
        log.error("PROBATE_SHEET_ID not set — cannot proceed")
        return counts

    sheets = _build_sheets_client()
    if sheets is None:
        log.error("Sheets client unavailable — cannot proceed")
        return counts

    # Discover target tabs
    try:
        targets = _list_target_tabs(sheets, sheet_id, name_filter=tab_filter)
    except Exception as e:
        log.error(f"Failed to list sheet tabs: {e}")
        counts["errors"] += 1
        return counts

    if not targets:
        log.warning("No tabs matched the {County}_County_TX_{YYYY}-{MM} pattern"
                    + (f" (filter={tab_filter})" if tab_filter else ""))
        return counts

    log.info(f"Found {len(targets)} target tabs:")
    for t in targets:
        log.info(f"  - {t['title']} ({'LAST_REVISION' if t['is_last_revision'] else 'draft'})")

    # Build Supabase client (only if not dry-running)
    client = None
    if not dry_run:
        url = (os.environ.get("SUPABASE_URL") or "").strip()
        key = (os.environ.get("SUPABASE_SECRET_KEY") or "").strip()
        if not url or not key:
            log.error("SUPABASE_URL / SUPABASE_SECRET_KEY missing — cannot upsert")
            counts["errors"] += 1
            return counts
        try:
            from supabase import create_client
            client = create_client(url, key)
        except Exception as e:
            log.error(f"Supabase client init failed: {e}")
            counts["errors"] += 1
            return counts

    synced_at_iso = datetime.now(timezone.utc).isoformat()

    # Process each tab
    all_records: list[dict] = []
    for tab in targets:
        counts["tabs_seen"] += 1
        title = tab["title"]
        county_label = _normalize_county(tab["county"])

        try:
            headers, rows = _read_tab(sheets, sheet_id, title)
        except Exception as e:
            log.error(f"Failed reading tab '{title}': {e}")
            counts["errors"] += 1
            continue

        if not headers:
            log.warning(f"Tab '{title}' is empty")
            continue

        _validate_headers(headers, title)

        log.info(f"Tab '{title}': {len(rows)} data rows")
        for row in rows:
            counts["rows_seen"] += 1
            try:
                rec = _row_to_record(headers, row, title, county_label, synced_at_iso)
            except Exception as e:
                log.warning(f"Tab '{title}' row {counts['rows_seen']}: {e}")
                counts["rows_skipped"] += 1
                continue
            if rec is None:
                counts["rows_skipped"] += 1
                continue
            all_records.append(rec)

    if not all_records:
        log.info("No valid rows to upsert")
        return counts

    log.info(f"Total normalized records ready to upsert: {len(all_records)}")

    # Dry-run preview: print first 3, return
    if dry_run:
        log.info("DRY RUN — preview (first 3 records):")
        for r in all_records[:3]:
            log.info(json.dumps(r, indent=2, default=str))
        log.info(f"DRY RUN — would upsert {len(all_records)} records "
                 "(skipping DB write)")
        return counts

    # Upsert in batches. on_conflict='doc_num' means LAST_REVISION wins on duplicate
    # (since we sorted tabs so LAST_REVISION processes last).
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i + BATCH_SIZE]
        try:
            client.table("records").upsert(batch, on_conflict="doc_num").execute()
            counts["rows_upserted"] += len(batch)
            log.info(f"Upserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows "
                     f"(running total: {counts['rows_upserted']})")
        except Exception as e:
            log.error(f"Upsert batch failed at offset {i}: {e}")
            counts["errors"] += 1

    log.info(f"Sync complete: {counts}")
    return counts


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    dry_run = os.environ.get("PROBATE_SYNC_DRY_RUN", "0") == "1"
    tab_filter = os.environ.get("PROBATE_SYNC_TAB_FILTER") or None
    if dry_run:
        log.info("Running in DRY RUN mode — no DB writes")
    if tab_filter:
        log.info(f"Tab filter active: only tabs containing '{tab_filter}'")

    counts = sync(dry_run=dry_run, tab_filter=tab_filter)

    # Exit non-zero on infrastructure failure so the workflow shows red
    if counts.get("errors", 0) > 0:
        log.error("Sync completed with errors — exiting non-zero")
        sys.exit(1)


if __name__ == "__main__":
    main()
