"""One-shot correction: fix wrong started_at values in scraper_runs.

Background
----------
The original backfill_runs.py had a bug: when parse_started_at() failed
to extract a timestamp from batch_label, it fell through to
datetime.utcnow() — meaning some runs got stamped with "the moment the
backfill ran" instead of "the moment the scraper actually ran".

Additionally, batch_labels with "AM CDT"/"PM CDT" suffixes had the TZ
stripped during parse, so the resulting timestamp was stored as UTC
when it was actually Central Time — a 5-hour drift on every parsed row.

This script corrects both errors:
  1. For runs with gh_run_number set: query GitHub Actions API for the
     accurate run_started_at (authoritative source).
  2. For older runs without gh_run_number: re-parse batch_label with
     proper CDT-to-UTC conversion (uses zoneinfo).

Idempotent: safe to re-run. Only touches rows where the correction
differs from what's already stored.

Usage:
    python supabase/fix_run_timestamps.py
    # or to preview without writing:
    DRY_RUN=1 python supabase/fix_run_timestamps.py
"""
from __future__ import annotations

import os
import re
import sys
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scraper"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "supabase" / ".env")
except ImportError:
    pass

from supabase import create_client

# zoneinfo is stdlib in Python 3.9+ but Windows doesn't ship the IANA tz db.
# Try ZoneInfo first (correct DST handling); fall back to fixed CDT offset
# (UTC-5). For our backfill range (Apr 2026) and any near-future use, fixed
# CDT is correct. The only case where this matters is parsing older labels
# from before/after DST transition - we'd need ZoneInfo (or pip install tzdata).
from datetime import timedelta
try:
    from zoneinfo import ZoneInfo
    CT = ZoneInfo("America/Chicago")
except Exception:
    print("Note: zoneinfo America/Chicago unavailable - using fixed CDT (UTC-5).")
    print("      DST transitions outside the typical Apr-Oct CDT window may be off.")
    CT = timezone(timedelta(hours=-5))

DRY_RUN = bool(os.environ.get("DRY_RUN"))

sb = create_client(
    os.environ["SUPABASE_URL"].strip(),
    os.environ["SUPABASE_SECRET_KEY"].strip(),
)

REPO = "nmjr75/harris-leads"

# Cache of gh_run_number -> ISO UTC timestamp from GH Actions
gh_cache: dict[int, str] = {}


def fetch_gh_run_started(gh_run_number: int) -> str | None:
    """Query GitHub Actions API for the actual started_at of a run.
    Uses gh CLI which is auth'd. Returns ISO UTC string or None."""
    if gh_run_number in gh_cache:
        return gh_cache[gh_run_number]

    # gh run list with filter, since the run number we tracked is
    # workflow-scoped, not repo-scoped. Try both workflows.
    for wf in ("scrape-foreclosures.yml", "scrape.yml"):
        try:
            out = subprocess.check_output([
                "gh", "run", "list",
                "--repo", REPO,
                "--workflow", wf,
                "--limit", "200",
                "--json", "number,startedAt",
            ], stderr=subprocess.DEVNULL, text=True)
            import json
            rows = json.loads(out)
            for row in rows:
                if row.get("number") == gh_run_number:
                    started = row.get("startedAt", "")
                    if started:
                        gh_cache[gh_run_number] = started
                        return started
        except Exception:
            continue
    return None


# Matches "2026-04-26 03:47 AM" or "2026-04-26 3:47 AM"
LABEL_TS_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2})\s*(AM|PM)\s*(\w+)?",
    re.IGNORECASE,
)


def parse_label_to_utc(label: str) -> str | None:
    """Parse 'Batch N - 2026-04-14 01:07 AM CDT' style label.
    If TZ suffix present (CDT/CST), use it. Otherwise assume Central Time.
    Returns ISO UTC string or None.
    """
    if not label:
        return None
    m = LABEL_TS_RE.search(label)
    if not m:
        return None
    date_part, time_part, ampm, tz_abbr = m.groups()
    raw = f"{date_part} {time_part} {ampm}"
    try:
        naive = datetime.strptime(raw, "%Y-%m-%d %I:%M %p")
    except ValueError:
        try:
            naive = datetime.strptime(raw, "%Y-%m-%d %H:%M %p")
        except ValueError:
            return None
    # Even if no TZ in label, the scraper runs from Houston so labels
    # without TZ are CT. CDT vs CST is determined by the date.
    aware_ct = naive.replace(tzinfo=CT)
    aware_utc = aware_ct.astimezone(timezone.utc)
    return aware_utc.isoformat()


# ── Main ───────────────────────────────────────────────────────────

print(f"\n=== fix_run_timestamps.py {'(DRY RUN)' if DRY_RUN else ''} ===\n")

runs = sb.table("scraper_runs").select(
    "id,run_id,gh_run_number,workflow_name,started_at,completed_at"
).order("id").execute().data

print(f"Loaded {len(runs)} runs from scraper_runs\n")

# Pull all batch_labels we have on records (one per batch_id) for fallback parsing
batch_label_lookup: dict[str, str] = {}
recs_page = sb.table("records").select("batch_id,batch_label").not_.is_("batch_id", "null").limit(1000).execute().data
for r in recs_page:
    bid = r.get("batch_id")
    if bid and bid not in batch_label_lookup:
        batch_label_lookup[bid] = r.get("batch_label") or ""

corrected = 0
already_ok = 0
unfixable = 0

for r in runs:
    run_pk = r["id"]
    run_id_text = r.get("run_id") or ""
    gh = r.get("gh_run_number")
    current = r.get("started_at") or ""

    new_ts: str | None = None

    # 1. Prefer GH Actions API when gh_run_number is set
    if gh:
        new_ts = fetch_gh_run_started(int(gh))
        if new_ts:
            source = f"GH Actions API (run #{gh})"

    # 2. Fall back to parsing batch_label with proper CT->UTC conversion
    if not new_ts and run_id_text:
        label = batch_label_lookup.get(run_id_text, "")
        new_ts = parse_label_to_utc(label)
        if new_ts:
            source = f"batch_label '{label[:50].encode('ascii','replace').decode()}'"

    if not new_ts:
        unfixable += 1
        print(f"  [SKIP] run_pk={run_pk}  gh={gh}  run_id={run_id_text}  "
              f"current={current}  (no source to derive correct ts)")
        continue

    # Normalize both to compare meaningfully
    def to_utc_iso(s: str) -> str:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except Exception:
            return s

    if to_utc_iso(current) == to_utc_iso(new_ts):
        already_ok += 1
        continue

    print(f"  [FIX]  run_pk={run_pk}  gh={gh}  run_id={run_id_text}")
    print(f"         was:  {current}")
    print(f"         new:  {new_ts}   ({source})")

    if not DRY_RUN:
        sb.table("scraper_runs").update({
            "started_at": new_ts,
            # also align completed_at if it was stamped to the same wrong moment
            "completed_at": new_ts,
        }).eq("id", run_pk).execute()
    corrected += 1

print(f"\n=== Summary ===")
print(f"  corrected:   {corrected}")
print(f"  already ok:  {already_ok}")
print(f"  unfixable:   {unfixable}")
if DRY_RUN:
    print(f"\n  DRY RUN - no changes written. Re-run without DRY_RUN=1 to apply.")
print()
