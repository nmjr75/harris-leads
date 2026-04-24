"""One-shot runner for migration 006. Prints pre/post counts and exits.

Uses supabase-py's table API (DELETE + UPDATE). No raw SQL needed because
both operations are trivial filter-based changes.

Safe to re-run: DELETE matches zero rows on second pass, UPDATE is idempotent.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(Path(__file__).parent / ".env")

url = os.environ["SUPABASE_URL"].strip()
key = os.environ["SUPABASE_SECRET_KEY"].strip()
client = create_client(url, key)

DROP_TYPES = ["REL", "BNKRCY", "JUDGE"]


def count_where(column: str, op: str, value):
    q = client.table("records").select("id", count="exact")
    if op == "in":
        q = q.in_(column, value)
    elif op == "eq":
        q = q.eq(column, value)
    resp = q.limit(1).execute()
    return resp.count or 0


def count_aj_legacy_cat() -> int:
    resp = (
        client.table("records")
        .select("id", count="exact")
        .eq("doc_type", "A/J")
        .neq("cat", "AOJ")
        .limit(1)
        .execute()
    )
    return resp.count or 0


print("-- Pre-migration counts --")
pre_drop = count_where("doc_type", "in", DROP_TYPES)
pre_recat = count_aj_legacy_cat()
print(f"  records in REL/BNKRCY/JUDGE (to delete): {pre_drop}")
print(f"  A/J records not yet on cat=AOJ (to update): {pre_recat}")

if pre_drop == 0 and pre_recat == 0:
    print("Nothing to do.")
    sys.exit(0)

print("\n-- Applying --")
if pre_drop:
    client.table("records").delete().in_("doc_type", DROP_TYPES).execute()
    print(f"  deleted {pre_drop} low-intent rows")

if pre_recat:
    client.table("records").update({"cat": "AOJ"}).eq("doc_type", "A/J").execute()
    print(f"  recategorized {pre_recat} A/J rows to cat=AOJ")

print("\n-- Post-migration counts --")
post_drop = count_where("doc_type", "in", DROP_TYPES)
post_recat = count_aj_legacy_cat()
print(f"  REL/BNKRCY/JUDGE remaining: {post_drop}")
print(f"  A/J rows not on cat=AOJ:    {post_recat}")

if post_drop == 0 and post_recat == 0:
    print("\n[OK] Migration 006 applied cleanly.")
else:
    print("\n[WARN] Residual rows remain - inspect manually.")
    sys.exit(1)
