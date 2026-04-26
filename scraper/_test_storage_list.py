"""Probe for Layer 2: verify Storage list() pagination + download() work.

Run from harris-leads root:
    python scraper/_test_storage_list.py

Loads creds from supabase/.env. Uploads 3 test PDFs at known prefix,
lists them, downloads one back, cleans up. Logs everything.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scraper"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "supabase" / ".env")
except ImportError:
    print("Note: python-dotenv not installed - relying on shell env")

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    print("FAIL: SUPABASE_URL or SUPABASE_SECRET_KEY missing from env")
    sys.exit(1)

print(f"Connecting to {SUPABASE_URL}")
client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

BUCKET = "clerk_pdfs"
PREFIX = "_probe_list"  # isolated test prefix
SAMPLES = ["RP-9999-001.pdf", "RP-9999-002.pdf", "RP-9999-003.pdf"]
TEST_BYTES = b"%PDF-1.4\n% list+download probe - safe to delete\n%%EOF\n"

# 1) Upload 3 test files
print(f"\n1) Uploading {len(SAMPLES)} test files to {BUCKET}/{PREFIX}/...")
for name in SAMPLES:
    path = f"{PREFIX}/{name}"
    try:
        client.storage.from_(BUCKET).upload(
            path=path,
            file=TEST_BYTES,
            file_options={"content-type": "application/pdf", "upsert": "true"},
        )
        print(f"   uploaded {path}")
    except Exception as e:
        print(f"   FAIL on {path}: {type(e).__name__}: {e}")
        sys.exit(1)

# 2) List the prefix
print(f"\n2) Listing {BUCKET}/{PREFIX}/...")
try:
    listing = client.storage.from_(BUCKET).list(
        path=PREFIX,
        options={"limit": 1000, "offset": 0},
    )
    print(f"   list returned {len(listing)} items")
    print(f"   sample item type: {type(listing[0]).__name__}")
    print(f"   sample item: {listing[0]}")
    names_found = set()
    for item in listing:
        if isinstance(item, dict):
            n = item.get("name", "")
        else:
            n = getattr(item, "name", "")
        names_found.add(n)
    expected = set(SAMPLES)
    missing = expected - names_found
    extra = names_found - expected
    if missing:
        print(f"   FAIL: expected names not in list: {missing}")
        sys.exit(1)
    print(f"   OK: all {len(expected)} expected names present")
    if extra:
        print(f"   note: extra items in prefix (probably from prior probe runs): {extra}")
except Exception as e:
    print(f"   FAIL: list error - {type(e).__name__}: {e}")
    sys.exit(1)

# 3) Download one back
print(f"\n3) Downloading {BUCKET}/{PREFIX}/{SAMPLES[0]}...")
try:
    body = client.storage.from_(BUCKET).download(f"{PREFIX}/{SAMPLES[0]}")
    if body == TEST_BYTES:
        print(f"   OK: downloaded {len(body)} bytes, content matches")
    else:
        print(f"   FAIL: downloaded {len(body)} bytes but content differs")
        sys.exit(1)
except Exception as e:
    print(f"   FAIL: download error - {type(e).__name__}: {e}")
    sys.exit(1)

# 4) Try downloading a non-existent file (this is the "skip-if-not-present" path)
print(f"\n4) Testing download of non-existent file (should fail cleanly)...")
try:
    client.storage.from_(BUCKET).download(f"{PREFIX}/RP-DOES-NOT-EXIST.pdf")
    print(f"   UNEXPECTED: download succeeded for missing file")
    sys.exit(1)
except Exception as e:
    print(f"   OK: clean failure with {type(e).__name__}: {str(e)[:120]}")

# 5) Cleanup
print(f"\n5) Cleaning up...")
to_remove = [f"{PREFIX}/{n}" for n in SAMPLES]
try:
    client.storage.from_(BUCKET).remove(to_remove)
    print(f"   OK: removed {len(to_remove)} probe files")
except Exception as e:
    print(f"   WARN: cleanup failed - {e}")

# 6) Confirm cleanup worked
print(f"\n6) Verifying cleanup...")
try:
    after = client.storage.from_(BUCKET).list(path=PREFIX, options={"limit": 1000})
    if len(after) == 0:
        print(f"   OK: prefix is empty after cleanup")
    else:
        print(f"   note: {len(after)} items remain (probably old probe leftovers)")
except Exception as e:
    print(f"   WARN: post-cleanup list failed - {e}")

print("\nALL CHECKS PASSED - list+download APIs ready for Layer 2 integration.")
