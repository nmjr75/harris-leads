"""One-shot probe: verify clerk_pdfs bucket exists and we can write to it.

Run from harris-leads root:
    python scraper/_test_storage_write.py

Loads creds from supabase/.env. Writes a small test PDF, reads it back,
deletes it. Logs everything. Safe to run repeatedly.
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
    print("Note: python-dotenv not installed — relying on shell env")

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    print("FAIL: SUPABASE_URL or SUPABASE_SECRET_KEY missing from env")
    sys.exit(1)

print(f"Connecting to {SUPABASE_URL}")
client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

BUCKET = "clerk_pdfs"
TEST_PATH = "_probe/_storage_write_test.pdf"
TEST_BYTES = b"%PDF-1.4\n% storage write probe - safe to delete\n%%EOF\n"

# 1) List buckets to confirm clerk_pdfs exists
print("\n1) Listing buckets...")
try:
    buckets = client.storage.list_buckets()
    bucket_names = [b.name if hasattr(b, "name") else b.get("name") for b in buckets]
    print(f"   Buckets visible to service-role: {bucket_names}")
    if BUCKET not in bucket_names:
        print(f"   FAIL: '{BUCKET}' not found. Need to create it in Supabase Console.")
        sys.exit(1)
    print(f"   OK: '{BUCKET}' exists")
except Exception as e:
    print(f"   FAIL: list_buckets error — {e}")
    sys.exit(1)

# 2) Try upload with upsert
print(f"\n2) Uploading test bytes to {BUCKET}/{TEST_PATH}...")
try:
    result = client.storage.from_(BUCKET).upload(
        path=TEST_PATH,
        file=TEST_BYTES,
        file_options={"content-type": "application/pdf", "upsert": "true"},
    )
    print(f"   OK: upload returned {result}")
except Exception as e:
    print(f"   FAIL: upload error — {type(e).__name__}: {e}")
    sys.exit(1)

# 3) Read it back
print(f"\n3) Reading back {BUCKET}/{TEST_PATH}...")
try:
    body = client.storage.from_(BUCKET).download(TEST_PATH)
    if body == TEST_BYTES:
        print(f"   OK: downloaded {len(body)} bytes, content matches")
    else:
        print(f"   WARN: downloaded {len(body)} bytes but content differs from upload")
except Exception as e:
    print(f"   FAIL: download error — {type(e).__name__}: {e}")
    sys.exit(1)

# 4) Try uploading again (verifies upsert works on second write)
print(f"\n4) Re-uploading same path (idempotency test)...")
try:
    client.storage.from_(BUCKET).upload(
        path=TEST_PATH,
        file=TEST_BYTES + b"\n% v2",
        file_options={"content-type": "application/pdf", "upsert": "true"},
    )
    print(f"   OK: upsert succeeded (no conflict error)")
except Exception as e:
    print(f"   FAIL: upsert error — {type(e).__name__}: {e}")
    print(f"          → may need to use 'update' instead of 'upload' for idempotency")
    sys.exit(1)

# 5) Cleanup
print(f"\n5) Cleaning up...")
try:
    client.storage.from_(BUCKET).remove([TEST_PATH])
    print(f"   OK: removed probe file")
except Exception as e:
    print(f"   WARN: cleanup failed — {e} (manual cleanup may be needed)")

print("\nALL CHECKS PASSED — Storage layer is ready for Layer 1 integration.")
