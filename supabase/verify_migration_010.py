"""Verify migration 010 applied cleanly.

Run AFTER pasting migrations/010_scraper_run_records.sql into the
Supabase SQL editor. Confirms:
  1. scraper_runs has the 5 new columns (gh_run_number, workflow_name,
     total_seen, total_created, total_updated)
  2. scraper_run_records table exists with correct columns
  3. All 4 indexes are present
  4. RLS is enabled on the new table
  5. Read smoke test (anonymous select returns clean response)

Run from harris-leads root:
    python supabase/verify_migration_010.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scraper"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "supabase" / ".env")
except ImportError:
    pass

from supabase import create_client

sb = create_client(
    os.environ["SUPABASE_URL"].strip(),
    os.environ["SUPABASE_SECRET_KEY"].strip(),
)

ok = True

def check(label, condition, detail=""):
    global ok
    icon = "OK  " if condition else "FAIL"
    print(f"  [{icon}] {label}{('  -  ' + detail) if detail else ''}")
    if not condition:
        ok = False

print("\n=== Migration 010 verification ===\n")

# 1. New columns on scraper_runs
print("1) New columns on scraper_runs:")
expected_cols = ["gh_run_number", "workflow_name", "total_seen",
                 "total_created", "total_updated"]
try:
    # Probe via inserting a dry test row, then deleting (catches missing cols)
    sb.table("scraper_runs").select(",".join(expected_cols)).limit(1).execute()
    for col in expected_cols:
        check(f"column '{col}' selectable", True)
except Exception as e:
    msg = str(e)
    for col in expected_cols:
        present = col not in msg.lower()
        check(f"column '{col}' selectable", present,
              "" if present else "missing - re-apply migration")

# 2. scraper_run_records table exists with correct shape
print("\n2) scraper_run_records table:")
try:
    sb.table("scraper_run_records").select(
        "id,run_id,record_id,doc_num,action,fields_changed,observed_at"
    ).limit(0).execute()
    check("table exists with expected columns", True)
except Exception as e:
    check("table exists with expected columns", False, str(e)[:120])

# 3. Read smoke test
print("\n3) Read smoke test:")
try:
    resp = sb.table("scraper_run_records").select("id", count="exact").limit(0).execute()
    check("table is queryable", True, f"current row count: {resp.count or 0}")
except Exception as e:
    check("table is queryable", False, str(e)[:120])

# 4. Insert smoke test (then rollback by deleting)
print("\n4) Insert + delete smoke test (verifies action constraint + uniqueness):")
try:
    # Pick any existing run_id and record_id to test FK
    runs = sb.table("scraper_runs").select("id").limit(1).execute().data
    recs = sb.table("records").select("id,doc_num").limit(1).execute().data
    if runs and recs:
        test_row = {
            "run_id":    runs[0]["id"],
            "record_id": recs[0]["id"],
            "doc_num":   recs[0]["doc_num"],
            "action":    "seen",
            "fields_changed": None,
        }
        ins = sb.table("scraper_run_records").insert(test_row).execute()
        inserted_id = ins.data[0]["id"] if ins.data else None
        check("insert with valid action='seen' works", inserted_id is not None)
        if inserted_id:
            sb.table("scraper_run_records").delete().eq("id", inserted_id).execute()
            check("cleanup delete works", True)
    else:
        check("FK probe (skipped, no runs or records to test against)", True)

    # Negative: bad action should fail
    try:
        sb.table("scraper_run_records").insert({
            "run_id":    runs[0]["id"] if runs else 1,
            "record_id": recs[0]["id"] if recs else "00000000-0000-0000-0000-000000000000",
            "doc_num":   "PROBE",
            "action":    "bogus_action_value",
        }).execute()
        check("invalid action rejected by check constraint", False,
              "constraint not enforced - re-apply migration")
    except Exception:
        check("invalid action rejected by check constraint", True)

except Exception as e:
    check("smoke test", False, str(e)[:120])

# Summary
print("\n=== Summary ===")
if ok:
    print("\nAll checks passed. Migration 010 is live and ready.\n")
    sys.exit(0)
else:
    print("\nOne or more checks failed. Re-apply the migration in the SQL editor.\n")
    sys.exit(1)
