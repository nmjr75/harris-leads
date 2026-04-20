"""SIFTstack queue worker — full pipeline.

End-to-end flow for every pending siftstack_queue row:
  1. Pull batch from siftstack_queue joined with records
  2. Resolve records.parcel_acct → property_data via generated-column lookup
  3. Build NoticeData objects from records + property_data
  4. Run SiftStack's canonical enrichment pipeline (Smarty + obituary/heir
     + entity research). Live HCAD/Zillow/Tax lookups are SKIPPED — the
     HCAD bulk sync populates property_data; we pre-load from there.
     Skip trace and phone validation are OFF.
  5. Generate per-record deep-prospecting PDFs
  6. Upload every PDF to Google Drive in a timestamped Harris subfolder
  7. Generate DataSift 41-column CSV and upload via Playwright
     (enrich property data = ON, skip trace = OFF)
  8. Send Slack notification with links
  9. Mark each queue row completed / failed

One Slack message per DataSift upload. If the queue is empty, the worker
logs and exits — no PDFs, no CSV, no Slack.

Environment variables required:
    SUPABASE_URL                      — harris-leads Supabase
    SUPABASE_SECRET_KEY               — service-role key
    SIFTSTACK_SRC                     — path to SiftStack/src/ (worker adds to PYTHONPATH)
    SIFTSTACK_BATCH_SIZE              — rows per run (default 25)
    ANTHROPIC_API_KEY                 — LLM for entity research + DM detection
    SMARTY_AUTH_ID / SMARTY_AUTH_TOKEN— address standardization
    SERPER_API_KEY                    — Google search for obituary/heir research
    FIRECRAWL_API_KEY                 — JS-rendered page scrape for obituaries
    DATASIFT_EMAIL / DATASIFT_PASSWORD— DataSift upload auth
    SLACK_WEBHOOK_URL                 — Slack/Discord webhook
    GOOGLE_SERVICE_ACCOUNT_KEY        — base64-encoded service-account JSON
    GOOGLE_DRIVE_FOLDER_ID_HARRIS     — Drive folder for PDF + CSV uploads
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

log = logging.getLogger("siftstack_worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ── Env + path setup ───────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
SIFTSTACK_SRC = os.environ.get("SIFTSTACK_SRC", "./SiftStack/src").strip()
BATCH_SIZE = int(os.environ.get("SIFTSTACK_BATCH_SIZE", "25"))
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL / SUPABASE_SECRET_KEY — cannot run")
    sys.exit(1)

siftstack_path = Path(SIFTSTACK_SRC).resolve()
if not siftstack_path.is_dir():
    log.error("SIFTSTACK_SRC does not exist: %s", siftstack_path)
    sys.exit(1)
sys.path.insert(0, str(siftstack_path))
log.info("SiftStack src on PYTHONPATH: %s", siftstack_path)


# ── Imports (after PYTHONPATH fixup) ──────────────────────────────────
try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed")
    sys.exit(1)

try:
    from notice_parser import NoticeData                          # type: ignore
    from enrichment_pipeline import (                             # type: ignore
        PipelineOptions, run_enrichment_pipeline,
    )
    from report_generator import generate_record_pdf              # type: ignore
    from datasift_formatter import write_datasift_csv             # type: ignore
    from datasift_uploader import upload_to_datasift              # type: ignore
    from dropbox_uploader import upload_and_share, upload_batch   # type: ignore
    from slack_notifier import send_slack_notification            # type: ignore
except Exception as e:
    log.error("Failed to import SiftStack modules: %s", e)
    log.error(traceback.format_exc())
    sys.exit(1)


client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Doc type → notice type mapping ────────────────────────────────────
# Clerk doc types we recognize. Everything else defaults to "foreclosure"
# so it still flows through the DataSift "Foreclosure" list rather than
# getting dropped.
DOC_TYPE_MAP = {
    "FRCL":   "foreclosure",
    "TRSALE": "foreclosure",
    "NOTICE": "foreclosure",
    "PROB":   "probate",
    "WILL":   "probate",
    "AFFT":   "probate",
    "LIEN":   "tax_delinquent",
    "T/L":    "tax_delinquent",
    "LEVY":   "tax_delinquent",
    "JUDGE":  "foreclosure",
    "A/J":    "foreclosure",
    "L/P":    "foreclosure",
    "BNKRCY": "foreclosure",
    "REL":    "foreclosure",
}


def _map_notice_type(doc_type: str | None) -> str:
    return DOC_TYPE_MAP.get((doc_type or "").strip().upper(), "foreclosure")


# ── Supabase helpers ──────────────────────────────────────────────────
def fetch_pending(n: int) -> list[dict]:
    """Pending queue rows + joined record + joined property_data."""
    resp = (
        client.table("siftstack_queue")
        .select(
            "id, record_id, status, "
            "records!inner(id, doc_num, doc_type, filed_date, sale_date, "
            "owner, grantor_name, co_borrower, "
            "prop_address, prop_city, prop_state, prop_zip, "
            "mail_address, mail_city, mail_state, mail_zip, "
            "parcel_acct, clerk_url, hcad_url, legal_description, amount)"
        )
        .eq("status", "pending")
        .order("queued_at", desc=False)
        .limit(n)
        .execute()
    )
    return resp.data or []


def resolve_parcel(record: dict) -> str | None:
    """If record.parcel_acct is unset, try to resolve via property_data."""
    if record.get("parcel_acct"):
        return record["parcel_acct"]
    addr = record.get("prop_address")
    if not addr:
        return None
    try:
        rpc = client.rpc(
            "resolve_parcel_by_address",
            {"p_address": addr, "p_zip": record.get("prop_zip")},
        ).execute()
        return rpc.data
    except Exception as e:
        log.debug("parcel resolve failed for %s: %s", addr, e)
        return None


def fetch_property_data(parcel_acct: str) -> dict | None:
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


def mark_processing(queue_id: int) -> None:
    client.table("siftstack_queue").update({"status": "processing"}).eq("id", queue_id).execute()


def mark_completed(queue_id: int) -> None:
    client.table("siftstack_queue").update({"status": "completed", "error": None}).eq("id", queue_id).execute()


def mark_failed(queue_id: int, error: str) -> None:
    client.table("siftstack_queue").update(
        {"status": "failed", "error": error[:500]}
    ).eq("id", queue_id).execute()


# ── records + property_data → NoticeData ──────────────────────────────
def build_notice(queue_row: dict) -> tuple[NoticeData, int, str]:
    """Build a NoticeData, back-filling from property_data when possible.

    Returns (notice, queue_id, record_id) so the caller can mark status.
    """
    qid = queue_row["id"]
    rec = queue_row.get("records") or {}
    rec_id = rec.get("id")

    # Ensure we have a parcel_acct (may need to resolve from property_data)
    parcel = resolve_parcel(rec)
    if parcel and parcel != rec.get("parcel_acct"):
        try:
            client.table("records").update({"parcel_acct": parcel}).eq("id", rec_id).execute()
        except Exception:
            pass

    prop = fetch_property_data(parcel) if parcel else None

    notice_type = _map_notice_type(rec.get("doc_type"))
    n = NoticeData()
    n.notice_type    = notice_type
    n.county         = rec.get("county") or "Harris"
    n.state          = rec.get("prop_state") or rec.get("state") or "TX"
    n.date_added     = rec.get("filed_date") or ""
    n.auction_date   = rec.get("sale_date") or ""
    n.source_url     = rec.get("clerk_url") or rec.get("hcad_url") or ""

    # Property address
    n.address = rec.get("prop_address") or ""
    n.city    = rec.get("prop_city") or ""
    n.zip     = rec.get("prop_zip") or ""

    # Owner — for probate the grantor is the decedent; owner is the PR
    owner = rec.get("owner") or ""
    grantor = rec.get("grantor_name") or ""
    if notice_type == "probate":
        n.owner_name    = (owner or grantor) or ""
        n.decedent_name = grantor or owner
    else:
        n.owner_name = owner or grantor

    # Mailing
    n.owner_street = rec.get("mail_address") or ""
    n.owner_city   = rec.get("mail_city") or ""
    n.owner_state  = rec.get("mail_state") or ""
    n.owner_zip    = rec.get("mail_zip") or ""

    # Parcel + HCAD pre-enrichment from property_data
    if parcel:
        n.parcel_id = parcel
    if prop:
        if prop.get("sqft"):           n.sqft = str(prop["sqft"])
        if prop.get("bedrooms"):       n.bedrooms = str(prop["bedrooms"])
        if prop.get("bathrooms"):      n.bathrooms = str(prop["bathrooms"])
        if prop.get("year_built"):     n.year_built = str(prop["year_built"])
        if prop.get("lot_size_sqft"):  n.lot_size = str(prop["lot_size_sqft"])
        if prop.get("market_value"):   n.estimated_value = str(int(prop["market_value"]))
        if prop.get("owner_name"):     n.tax_owner_name = prop["owner_name"]
        if prop.get("situs_address") and not n.address:
            n.address = prop["situs_address"]
        if prop.get("situs_city") and not n.city:
            n.city = prop["situs_city"]
        if prop.get("situs_zip") and not n.zip:
            n.zip = prop["situs_zip"]

    return n, qid, rec_id


# ── Dropbox upload helpers ────────────────────────────────────────────
DROPBOX_OUTPUT_ROOT = os.environ.get("DROPBOX_HARRIS_OUTPUT_FOLDER", "/Harris Leads").rstrip("/")


def build_dropbox_path(subfolder: str, filename: str) -> str:
    return f"{DROPBOX_OUTPUT_ROOT}/{subfolder}/{filename}"


# ── Main run ───────────────────────────────────────────────────────────
async def run_once() -> int:
    pending = fetch_pending(BATCH_SIZE)
    log.info("Pending queue rows: %d (batch size %d)", len(pending), BATCH_SIZE)
    if not pending:
        return 0

    # Build NoticeData list + keep queue id pairing
    notices: list[NoticeData] = []
    queue_ids: list[int] = []
    skipped: list[tuple[int, str]] = []

    for q in pending:
        try:
            mark_processing(q["id"])
            n, qid, _ = build_notice(q)
            if not n.address or not n.owner_name:
                skipped.append((qid, "missing address or owner"))
                mark_failed(qid, "missing address or owner after property_data lookup")
                continue
            notices.append(n)
            queue_ids.append(qid)
        except Exception as e:
            log.warning("build_notice failed for queue id %s: %s", q.get("id"), e)
            skipped.append((q.get("id"), str(e)))
            if q.get("id"):
                try:
                    mark_failed(q["id"], f"build_notice: {e}")
                except Exception:
                    pass

    if not notices:
        log.warning("No valid notices to enrich (skipped: %d)", len(skipped))
        return 0

    log.info("Enriching %d notices ...", len(notices))
    start = datetime.now()
    opts = PipelineOptions(
        skip_filter_sold     = True,   # Harris leads aren't sold-filtered here
        skip_vacant_filter   = True,   # don't drop vacant land from the queue
        skip_entity_filter   = True,   # don't remove LLC-owned records (still upload)
        skip_entity_research = False,  # DO research entities to enrich data
        skip_commercial_filter = True,
        skip_parcel_lookup   = True,   # HCAD comes from property_data, not live
        skip_tax             = True,   # same
        skip_smarty          = False,  # standardize addresses
        skip_geocode         = True,   # not needed for Harris pipeline
        skip_zillow          = True,   # Harris uses HCAD, not Zillow
        skip_obituary        = False,  # confirm deceased + find heirs
        skip_ancestry        = True,   # we are not using Ancestry
        skip_heir_verification = False,
        max_heir_depth       = 2,
        skip_dm_address      = False,
        source_label         = "harris-siftstack-worker",
    )
    enriched = run_enrichment_pipeline(notices, opts)
    elapsed_min = (datetime.now() - start).total_seconds() / 60.0
    log.info("Enrichment complete: %d notices in %.1f min", len(enriched), elapsed_min)

    # ── Generate PDFs ─────────────────────────────────────────────────
    pdf_dir = Path("output/reports") / datetime.now().strftime("%Y-%m-%d_%H-%M")
    pdf_dir.mkdir(parents=True, exist_ok=True)
    pdf_paths: list[tuple[NoticeData, Path]] = []
    for n in enriched:
        try:
            pdf_path = generate_record_pdf(n, output_dir=pdf_dir)
            pdf_paths.append((n, pdf_path))
        except Exception as e:
            log.warning("PDF generation failed for %s: %s", n.address, e)

    log.info("Generated %d PDFs in %s", len(pdf_paths), pdf_dir)

    # ── Generate DataSift CSV ─────────────────────────────────────────
    # write_datasift_csv writes into SiftStack's config.OUTPUT_DIR
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    csv_path = write_datasift_csv(enriched, filename=f"harris_siftstack_{ts}.csv")
    log.info("DataSift CSV: %s (%d records)", csv_path, len(enriched))

    # ── Upload PDFs + CSV to Dropbox + collect share links ────────────
    drive_links: list[tuple[str, str]] = []   # (name, url) — reused for Slack
    csv_link = None
    subfolder_name = datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        uploads: list[tuple[Path, str]] = []
        for _n, p in pdf_paths:
            uploads.append((p, build_dropbox_path(subfolder_name, p.name)))
        uploads.append((Path(csv_path), build_dropbox_path(subfolder_name, Path(csv_path).name)))
        log.info("Uploading %d files to Dropbox under %s/%s ...",
                 len(uploads), DROPBOX_OUTPUT_ROOT, subfolder_name)
        results = upload_batch(uploads)
        for local, url in results:
            if url is None:
                log.warning("Dropbox upload failed for %s", local.name)
                continue
            if local.suffix.lower() == ".csv":
                csv_link = url
            else:
                drive_links.append((local.name, url))
    except Exception as e:
        log.warning("Dropbox batch upload crashed: %s", e)

    # ── Upload to DataSift (Playwright) ───────────────────────────────
    upload_result: dict = {}
    try:
        log.info("Uploading CSV to DataSift (headless Playwright) ...")
        upload_result = await upload_to_datasift(
            Path(csv_path),
            headless=True,
            enrich=True,          # enrich property information ON
            skip_trace=False,     # skip trace OFF (param is deprecated/ignored upstream)
        )
        log.info("DataSift upload result: %s", upload_result.get("message") or upload_result)
    except Exception as e:
        log.exception("DataSift upload crashed: %s", e)
        upload_result = {"success": False, "message": str(e)}

    # ── Slack notification ────────────────────────────────────────────
    if SLACK_WEBHOOK:
        pdf_links_for_slack = [(name, url) for name, url in drive_links[:20]]
        try:
            send_slack_notification(
                enriched,
                webhook_url=SLACK_WEBHOOK,
                upload_result=upload_result,
                elapsed_min=elapsed_min,
                csv_link=csv_link,
                pdf_links=pdf_links_for_slack,
            )
        except Exception as e:
            log.warning("Slack notification failed: %s", e)
    else:
        log.info("SLACK_WEBHOOK_URL not set — skipping notification")

    # ── Mark queue rows ───────────────────────────────────────────────
    uploaded_ok = bool(upload_result.get("success"))
    for qid in queue_ids:
        try:
            if uploaded_ok:
                mark_completed(qid)
            else:
                mark_failed(qid, upload_result.get("message", "DataSift upload failed"))
        except Exception as e:
            log.warning("mark status failed for %s: %s", qid, e)

    log.info("Run complete: %d enriched, %d skipped, uploaded=%s",
             len(enriched), len(skipped), uploaded_ok)
    return 0


def main() -> int:
    try:
        return asyncio.run(run_once())
    except Exception as e:
        log.exception("Worker crashed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
