"""Probate -> DataSift queue worker.

Drains probate_campaign_queue (populated by VA "Submit to SIFTstack"
button on /probate.html). For each pending row:

  1. Pull queue row joined with records (probate-shaped)
  2. Pull contact_phones (priority + is_best -> NoticeData phone slots)
  3. Pull contact_emails -> NoticeData email slots
  4. Build NoticeData (probate-aware: grantor=decedent, owner=PR)
  5. SKIP run_enrichment_pipeline entirely — VA already did the work
     (verification + skip-trace). Re-enriching would risk overwriting
     VA-confirmed PR data with stale obituary matches.
  6. Generate DataSift 41-column CSV via write_datasift_csv
  7. Upload CSV to Dropbox at /Harris Leads/Probate/<ts>/
  8. Upload CSV to DataSift (Playwright, Enrich Property OFF, Skip Trace OFF)
  9. Slack notification with CSV link
 10. Mark queue row completed/failed (DB trigger mirrors onto records.probate_campaign_status)

Different from run_siftstack_worker.py:
  - Reads from probate_campaign_queue (not siftstack_queue)
  - Pulls phones/emails from contact_phones/contact_emails (siftstack worker doesn't)
  - Skips enrichment_pipeline (siftstack worker runs it)
  - Skips PDF generation (probate doesn't need deep-prospecting reports)
  - Uploads with Enrich Property Information OFF (preserves VA-edited values)

Required env vars (must be set in workflow secrets):
    SUPABASE_URL, SUPABASE_SECRET_KEY  -- harris-leads project
    SIFTSTACK_SRC                       -- path to SiftStack/src
    PROBATE_BATCH_SIZE                  -- max queue rows per run (default 25)
    DATASIFT_EMAIL, DATASIFT_PASSWORD
    DROPBOX_ACCESS_TOKEN OR DROPBOX_REFRESH_TOKEN + APP_KEY/SECRET
    SLACK_WEBHOOK_URL                   -- optional
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger("probate_datasift_worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ── Env + path setup ───────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
SIFTSTACK_SRC = os.environ.get("SIFTSTACK_SRC", "./SiftStack/src").strip()
BATCH_SIZE = int(os.environ.get("PROBATE_BATCH_SIZE", "25"))
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL / SUPABASE_SECRET_KEY")
    sys.exit(1)

siftstack_path = Path(SIFTSTACK_SRC).resolve()
if not siftstack_path.is_dir():
    log.error("SIFTSTACK_SRC does not exist: %s", siftstack_path)
    sys.exit(1)
sys.path.insert(0, str(siftstack_path))


# ── Imports (after PYTHONPATH fixup) ──────────────────────────────────
try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed")
    sys.exit(1)

try:
    from notice_parser import NoticeData                          # type: ignore
    from datasift_formatter import write_datasift_csv             # type: ignore
    from datasift_uploader import upload_to_datasift              # type: ignore
    from dropbox_uploader import upload_batch                     # type: ignore
    from slack_notifier import send_slack_notification            # type: ignore
except ImportError as e:
    log.error("SiftStack imports failed: %s", e)
    sys.exit(1)


client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Queue helpers ─────────────────────────────────────────────────────
def fetch_pending(n: int) -> list[dict]:
    """Pending probate-queue rows joined with the underlying record."""
    resp = (
        client.table("probate_campaign_queue")
        .select(
            "id, record_id, status, "
            "records!inner(id, doc_num, county, state, filed_date, "
            "owner, grantor_name, "
            "prop_address, prop_city, prop_state, prop_zip, "
            "mail_address, mail_city, mail_state, mail_zip, "
            "parcel_acct, sheet_tab_name, additional_applicants, "
            "verification_status, source)"
        )
        .eq("status", "pending")
        .order("queued_at", desc=False)
        .limit(n)
        .execute()
    )
    return resp.data or []


def mark_processing(queue_id: int) -> None:
    client.table("probate_campaign_queue").update({"status": "processing"}).eq("id", queue_id).execute()


def mark_completed(queue_id: int) -> None:
    client.table("probate_campaign_queue").update(
        {"status": "completed", "error": None, "completed_at": datetime.utcnow().isoformat()}
    ).eq("id", queue_id).execute()


def mark_failed(queue_id: int, error: str) -> None:
    client.table("probate_campaign_queue").update(
        {"status": "failed", "error": error[:500], "completed_at": datetime.utcnow().isoformat()}
    ).eq("id", queue_id).execute()


# ── Contact data plumbing ─────────────────────────────────────────────
def fetch_phones(record_id: str) -> list[dict]:
    """All phones for a record. Filtered to non-DNC, non-litigator only."""
    resp = (
        client.table("contact_phones")
        .select("phone_e164, phone_display, phone_type, role, priority, is_best, is_dnc, is_litigator")
        .eq("record_id", record_id)
        .eq("is_dnc", False)
        .eq("is_litigator", False)
        .execute()
    )
    return resp.data or []


def fetch_emails(record_id: str) -> list[dict]:
    """All emails for a record. Filtered to non-bounced only."""
    resp = (
        client.table("contact_emails")
        .select("email, role, is_bounced")
        .eq("record_id", record_id)
        .eq("is_bounced", False)
        .execute()
    )
    return resp.data or []


def assign_phones_to_notice(notice: NoticeData, phones: list[dict]) -> int:
    """Map contact_phones rows to NoticeData phone slots.

    DataSift Phone 1-9 maps to:
        primary_phone (Phone 1) <- is_best=true (highest VA confidence)
        mobile_1..5   (Phones 2-6) <- wireless phones in priority order
        landline_1..3 (Phones 7-9) <- landline phones in priority order

    Role priority for phone selection: pr > heir > other. Skip deceased
    and attorney (not direct contact targets).

    Returns count of phones assigned (0-9).
    """
    if not phones:
        return 0

    # Filter and rank by role
    role_rank = {"pr": 0, "heir": 1, "other": 2}
    relevant = [p for p in phones if p.get("role") in role_rank]
    if not relevant:
        return 0

    # Sort: is_best first, then by role rank, then by priority (None last)
    def sort_key(p):
        return (
            0 if p.get("is_best") else 1,
            role_rank.get(p.get("role"), 99),
            p.get("priority") if p.get("priority") is not None else 999,
        )
    sorted_phones = sorted(relevant, key=sort_key)

    # Dedupe by phone_e164
    seen_e164 = set()
    deduped = []
    for p in sorted_phones:
        e164 = p.get("phone_e164")
        if not e164 or e164 in seen_e164:
            continue
        seen_e164.add(e164)
        deduped.append(p)

    # Assign to slots
    mobile_slots = ["mobile_1", "mobile_2", "mobile_3", "mobile_4", "mobile_5"]
    landline_slots = ["landline_1", "landline_2", "landline_3"]
    mobile_idx = 0
    landline_idx = 0
    assigned = 0

    # First pass: assign best phone to primary_phone
    if deduped:
        notice.primary_phone = deduped[0].get("phone_display") or deduped[0].get("phone_e164", "")
        assigned += 1

    # Second pass: fill remaining slots based on phone_type
    for p in deduped[1:]:
        phone_str = p.get("phone_display") or p.get("phone_e164", "")
        if not phone_str:
            continue
        ptype = p.get("phone_type", "unknown")
        if ptype in ("wireless", "voip", "unknown") and mobile_idx < len(mobile_slots):
            setattr(notice, mobile_slots[mobile_idx], phone_str)
            mobile_idx += 1
            assigned += 1
        elif ptype == "landline" and landline_idx < len(landline_slots):
            setattr(notice, landline_slots[landline_idx], phone_str)
            landline_idx += 1
            assigned += 1
        else:
            # Slots full for this type — try the other bucket as overflow
            if mobile_idx < len(mobile_slots):
                setattr(notice, mobile_slots[mobile_idx], phone_str)
                mobile_idx += 1
                assigned += 1
            elif landline_idx < len(landline_slots):
                setattr(notice, landline_slots[landline_idx], phone_str)
                landline_idx += 1
                assigned += 1
            else:
                break  # all 9 slots full
    return assigned


def assign_emails_to_notice(notice: NoticeData, emails: list[dict]) -> int:
    """Map contact_emails rows to NoticeData email_1..5 slots.

    Role priority same as phones: pr > heir > other.
    """
    if not emails:
        return 0
    role_rank = {"pr": 0, "heir": 1, "other": 2}
    relevant = [e for e in emails if e.get("role") in role_rank]
    if not relevant:
        return 0

    sorted_emails = sorted(relevant, key=lambda e: role_rank.get(e.get("role"), 99))
    seen = set()
    slots = ["email_1", "email_2", "email_3", "email_4", "email_5"]
    idx = 0
    for e in sorted_emails:
        addr = e.get("email", "").strip().lower()
        if not addr or addr in seen:
            continue
        seen.add(addr)
        if idx >= len(slots):
            break
        setattr(notice, slots[idx], addr)
        idx += 1
    return idx


# ── records + contact_data -> NoticeData ──────────────────────────────
def fetch_property_data(parcel_acct: str | None) -> dict | None:
    if not parcel_acct:
        return None
    try:
        resp = (
            client.table("property_data")
            .select("*")
            .eq("parcel_acct", parcel_acct)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.debug("property_data fetch failed for %s: %s", parcel_acct, e)
        return None


def build_notice(queue_row: dict) -> tuple[NoticeData | None, int, str | None, str | None]:
    """Build a probate-shaped NoticeData with phones + emails attached.

    Returns (notice, queue_id, record_id, error_msg).
    notice is None when build failed; error_msg explains why.
    """
    qid = queue_row["id"]
    rec = queue_row.get("records") or {}
    rec_id = rec.get("id")

    if not rec_id:
        return None, qid, None, "queue row has no joined record"

    # Required fields for a meaningful upload
    if not rec.get("prop_address"):
        return None, qid, rec_id, "record has no property address"
    if not rec.get("owner"):
        return None, qid, rec_id, "record has no PR (owner) name"

    n = NoticeData()
    n.notice_type = "probate"
    n.county      = rec.get("county") or "Harris"
    n.state       = rec.get("prop_state") or rec.get("state") or "TX"
    n.date_added  = rec.get("filed_date") or ""

    # Property address (target of marketing)
    n.address = rec.get("prop_address") or ""
    n.city    = rec.get("prop_city") or ""
    n.zip     = rec.get("prop_zip") or ""

    # Probate-specific: grantor = decedent, owner = PR
    n.owner_name    = rec.get("owner") or ""
    n.decedent_name = rec.get("grantor_name") or ""

    # Mailing address = PR's mailing (where the letter goes)
    n.owner_street = rec.get("mail_address") or ""
    n.owner_city   = rec.get("mail_city") or ""
    n.owner_state  = rec.get("mail_state") or ""
    n.owner_zip    = rec.get("mail_zip") or ""

    # Pre-populated HCAD data (from property_data, set by Saturday bulk sync)
    parcel = rec.get("parcel_acct")
    if parcel:
        n.parcel_id = parcel
    prop = fetch_property_data(parcel)
    if prop:
        if prop.get("sqft"):           n.sqft = str(prop["sqft"])
        if prop.get("bedrooms"):       n.bedrooms = str(prop["bedrooms"])
        if prop.get("bathrooms"):      n.bathrooms = str(prop["bathrooms"])
        if prop.get("year_built"):     n.year_built = str(prop["year_built"])
        if prop.get("lot_size_sqft"):  n.lot_size = str(prop["lot_size_sqft"])
        if prop.get("market_value"):   n.estimated_value = str(int(prop["market_value"]))

    # Phones + emails from contact_intelligence tables
    phones = fetch_phones(rec_id)
    emails = fetch_emails(rec_id)
    phone_count = assign_phones_to_notice(n, phones)
    assign_emails_to_notice(n, emails)

    # Hard requirement: must have at least one phone to be useful for SMS campaign
    if phone_count == 0:
        return None, qid, rec_id, (
            "no usable phones (need at least one non-DNC, non-litigator phone "
            "from a PR/heir/other role)"
        )

    return n, qid, rec_id, None


# ── Dropbox helpers ───────────────────────────────────────────────────
DROPBOX_ROOT = os.environ.get("DROPBOX_HARRIS_OUTPUT_FOLDER", "/Harris Leads").rstrip("/")


def build_dropbox_path(filename: str) -> str:
    subfolder = "Probate/" + datetime.now().strftime("%Y-%m-%d_%H-%M")
    return f"{DROPBOX_ROOT}/{subfolder}/{filename}"


# ── Main run ──────────────────────────────────────────────────────────
async def run_once() -> int:
    pending = fetch_pending(BATCH_SIZE)
    log.info("Pending probate-queue rows: %d (batch size %d)", len(pending), BATCH_SIZE)
    if not pending:
        return 0

    notices: list[NoticeData] = []
    queue_ids: list[int] = []
    skipped: list[tuple[int, str]] = []

    for q in pending:
        try:
            mark_processing(q["id"])
            notice, qid, _rec_id, err = build_notice(q)
            if err or notice is None:
                skipped.append((qid, err or "unknown build error"))
                mark_failed(qid, err or "unknown build error")
                continue
            notices.append(notice)
            queue_ids.append(qid)
        except Exception as e:
            log.exception("build_notice crashed for queue %s: %s", q.get("id"), e)
            skipped.append((q.get("id"), str(e)))
            if q.get("id"):
                try: mark_failed(q["id"], f"build_notice: {e}")
                except Exception: pass

    if not notices:
        log.warning("No valid notices to upload (skipped: %d)", len(skipped))
        return 0

    log.info("Built %d notices (skipped %d). Generating CSV...", len(notices), len(skipped))

    # ── Generate CSV ─────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    csv_path = write_datasift_csv(notices, filename=f"probate_datasift_{ts}.csv")
    log.info("DataSift CSV: %s (%d records)", csv_path, len(notices))

    # ── Upload CSV to Dropbox ────────────────────────────────────────
    csv_link = None
    try:
        uploads = [(Path(csv_path), build_dropbox_path(Path(csv_path).name))]
        results = upload_batch(uploads)
        for local, url in results:
            if url is None:
                log.warning("Dropbox upload failed for %s", local.name)
            else:
                csv_link = url
                log.info("Dropbox: %s", url)
    except Exception as e:
        log.warning("Dropbox upload crashed: %s", e)

    # ── Upload to DataSift ───────────────────────────────────────────
    upload_result: dict = {}
    try:
        log.info("Uploading CSV to DataSift (Enrich Property OFF, Skip Trace OFF)...")
        # NOTE: enrich=False is the deliberate probate setting — VA-edited
        # HCAD values stay as-is in DataSift instead of being overlaid by
        # SiftMap's data on collisions.
        upload_result = await upload_to_datasift(
            Path(csv_path),
            headless=True,
            enrich=False,
            skip_trace=False,
        )
        log.info("DataSift result: %s", upload_result.get("message") or upload_result)
    except Exception as e:
        log.exception("DataSift upload crashed: %s", e)
        upload_result = {"success": False, "message": str(e)}

    # ── Slack notification ───────────────────────────────────────────
    if SLACK_WEBHOOK:
        try:
            send_slack_notification(
                notices,
                webhook_url=SLACK_WEBHOOK,
                upload_result=upload_result,
                elapsed_min=0.0,
                csv_link=csv_link,
                pdf_links=[],  # probate worker skips PDFs
            )
        except Exception as e:
            log.warning("Slack notification failed: %s", e)

    # ── Mark queue rows ──────────────────────────────────────────────
    uploaded_ok = bool(upload_result.get("success"))
    for qid in queue_ids:
        try:
            if uploaded_ok:
                mark_completed(qid)
            else:
                mark_failed(qid, upload_result.get("message", "DataSift upload failed"))
        except Exception as e:
            log.warning("mark status failed for %s: %s", qid, e)

    log.info("Probate run complete: %d uploaded, %d skipped, success=%s",
             len(notices), len(skipped), uploaded_ok)
    return 0


def main() -> int:
    try:
        return asyncio.run(run_once())
    except Exception as e:
        log.exception("Probate worker crashed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
