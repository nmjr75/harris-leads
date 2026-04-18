"""
Gemini Vision API extraction for Harris County clerk PDFs.

Uses Google Gemini 2.5 Flash to extract structured data from PDF images.
Falls back to None (triggering pdfplumber/OCR pipeline) if Gemini is unavailable.

Contains two functions:
  - parse_pdf_with_gemini(): For foreclosure posting PDFs
  - parse_clerk_pdf_with_gemini(): For clerk filing PDFs (LP, liens, judgments, probate, etc.)

PDF→image conversion uses pypdfium2 (pure Python, no poppler dependency).
"""

import json
import logging
import os
import re
import time

log = logging.getLogger(__name__)

# --- PDF to image conversion (pypdfium2 — no system dependency) ---
HAS_PDFIUM = False
try:
    import pypdfium2 as pdfium
    HAS_PDFIUM = True
except ImportError:
    log.info("pypdfium2 not installed — PDF-to-image conversion disabled.")

# --- Gemini setup (prefer google.genai, fall back to deprecated google.generativeai) ---
HAS_GEMINI = False
_USE_NEW_SDK = False
_client = None

try:
    from google import genai
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if api_key:
        _client = genai.Client(api_key=api_key)
        HAS_GEMINI = True
        _USE_NEW_SDK = True
        log.info("Gemini Vision API configured (google.genai SDK).")
    else:
        log.info("GEMINI_API_KEY not set – Gemini extraction disabled.")
except ImportError:
    try:
        import google.generativeai as genai
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if api_key:
            genai.configure(api_key=api_key)
            HAS_GEMINI = True
            log.info("Gemini Vision API configured (legacy SDK).")
        else:
            log.info("GEMINI_API_KEY not set – Gemini extraction disabled.")
    except ImportError:
        log.info("google-genai not installed – Gemini extraction disabled.")

# Rate limiting for free tier
GEMINI_DELAY = 4.5  # seconds between API calls


def _pdf_to_images(pdf_bytes: bytes, max_pages: int = 3, scale: float = 2.0) -> list:
    """Convert PDF bytes to PIL Image objects using pypdfium2.

    No poppler or system dependency required.
    """
    if not HAS_PDFIUM:
        return []
    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        page_count = min(len(pdf), max_pages)
        images = []
        for i in range(page_count):
            page = pdf[i]
            bitmap = page.render(scale=scale)
            pil_image = bitmap.to_pil()
            images.append(pil_image)
        pdf.close()
        return images
    except Exception as e:
        log.warning(f"PDF to image conversion failed: {e}")
        return []


def _call_gemini(prompt: str, image) -> str:
    """Call Gemini Vision API with prompt + image, abstracting SDK version.

    Returns the raw response text.
    """
    from io import BytesIO

    if _USE_NEW_SDK:
        # google.genai SDK accepts PIL images directly
        response = _client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt, image],
        )
        return response.text
    else:
        # Legacy SDK needs image as bytes dict
        img_buf = BytesIO()
        image.save(img_buf, format="PNG")
        img_buf.seek(0)
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content([
            prompt,
            {"mime_type": "image/png", "data": img_buf.read()}
        ])
        return response.text


def _parse_json_response(text: str) -> dict | None:
    """Parse Gemini response text into a dict, stripping code fences."""
    try:
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)
        text = text.strip()
        return json.loads(text)
    except (json.JSONDecodeError, AttributeError) as e:
        log.warning(f"Failed to parse Gemini JSON response: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Foreclosure Posting PDFs
# ═══════════════════════════════════════════════════════════════════════════════

def parse_pdf_with_gemini(pdf_bytes, doc_id):
    """Extract fields from foreclosure PDF using Gemini Vision API.

    Returns parsed dict on success, None on failure (triggers OCR fallback).
    """
    if not HAS_GEMINI or not HAS_PDFIUM:
        return None

    try:
        images = _pdf_to_images(pdf_bytes, max_pages=2)
        if not images:
            return None

        prompt = (
            "Extract these fields from this Harris County foreclosure posting document.\n"
            "Return ONLY a valid JSON object with these keys (use empty string if not found):\n\n"
            "{\n"
            '  "grantor": "Full name of the borrower/property owner",\n'
            '  "co_borrower": "Co-borrower name if any",\n'
            '  "property_address": "Street address of the SUBJECT PROPERTY being foreclosed",\n'
            '  "property_city": "City of the subject property",\n'
            '  "property_zip": "ZIP code of the subject property",\n'
            '  "legal_description": "Lot, block, subdivision, and section. Example: LOT 15 BLOCK 21 INWOOD TERRACE SEC 2. Keep concise, no recording references.",\n'
            '  "amount": "Loan/debt amount with dollar sign",\n'
            '  "sale_date": "Foreclosure sale date in MM/DD/YYYY format",\n'
            '  "mortgagee": "Name of the lender/bank/mortgage company"\n'
            "}\n\n"
            "IMPORTANT:\n"
            "- property_address must be the SUBJECT PROPERTY street address, NOT a mailing address or trustee address\n"
            "- Look for phrases like 'Property Address', 'Subject Property', 'commonly known as'\n"
            "- Return ONLY the JSON object, no other text"
        )

        # Try page 1
        text = _call_gemini(prompt, images[0])
        data = _parse_json_response(text)
        if not data:
            return None

        result = {
            "grantor": data.get("grantor", ""),
            "co_borrower": data.get("co_borrower", ""),
            "property_address": data.get("property_address", ""),
            "property_city": data.get("property_city", ""),
            "property_state": "TX",
            "property_zip": data.get("property_zip", ""),
            "legal_description": data.get("legal_description", ""),
            "amount": data.get("amount", ""),
            "sale_date": data.get("sale_date", ""),
            "mortgagee": data.get("mortgagee", ""),
            "needs_ocr": False,
        }

        # If page 1 had no address, try page 2
        if not result["property_address"] and len(images) > 1:
            time.sleep(GEMINI_DELAY)
            try:
                text2 = _call_gemini(prompt, images[1])
                d2 = _parse_json_response(text2)
                if d2:
                    for key in ["property_address", "property_city", "property_zip",
                                "legal_description", "grantor", "amount", "sale_date", "mortgagee"]:
                        if not result.get(key) and d2.get(key):
                            result[key] = d2[key]
            except Exception:
                pass

        log.info(f"  Gemini: addr={result['property_address'][:40]}, legal={result['legal_description'][:40]}")
        time.sleep(GEMINI_DELAY)
        return result

    except Exception as e:
        log.warning(f"  Gemini failed for {doc_id}: {e}")
        time.sleep(GEMINI_DELAY)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Clerk Document Extraction (LP, Liens, Judgments, Probate, etc.)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_clerk_pdf_with_gemini(pdf_bytes, doc_id, doc_type=""):
    """Extract property address and legal description from a Harris County
    clerk filing PDF (lis pendens, liens, judgments, probate, etc.).

    These documents are very different from foreclosure postings — they're
    typically multi-page legal filings with property info buried in the text.

    Returns dict with extracted fields on success, None on failure.
    """
    if not HAS_GEMINI or not HAS_PDFIUM:
        return None

    try:
        images = _pdf_to_images(pdf_bytes, max_pages=3)
        if not images:
            return None

        prompt = (
            f"Extract property and legal information from this Harris County clerk filing (type: {doc_type}).\n"
            "This is a legal document filed with the Harris County Clerk's office. "
            "It may be a lis pendens, tax lien, judgment, probate filing, levy, or similar.\n\n"
            "Return ONLY a valid JSON object with these keys (use empty string if not found):\n\n"
            "{\n"
            '  "property_address": "Street address of the SUBJECT PROPERTY referenced in the filing",\n'
            '  "property_city": "City of the subject property",\n'
            '  "property_zip": "ZIP code of the subject property",\n'
            '  "legal_description": "Full legal description — lot, block, subdivision, section. Example: LOT 5 BLOCK 3 WILDHEATHER SEC 1. Keep concise.",\n'
            '  "amount": "Dollar amount if any (judgment amount, lien amount, loan balance, etc.)",\n'
            '  "grantor_name": "Name of the grantor / filing party / plaintiff / petitioner",\n'
            '  "grantee_name": "Name of the grantee / respondent / defendant / property owner"\n'
            "}\n\n"
            "IMPORTANT:\n"
            "- property_address must be the SUBJECT PROPERTY street address, NOT a court address, attorney address, or mailing address\n"
            "- Look for phrases like 'property address', 'subject property', 'commonly known as', "
            "'property described as', 'real property located at', 'situated in Harris County'\n"
            "- The legal description often contains subdivision name, lot number, block number, and section\n"
            "- Look for the legal description near phrases like 'described as', 'legally described', "
            "'being more particularly described'\n"
            "- For amounts, look for judgment amounts, lien amounts, principal balance, or debt owed\n"
            "- Return ONLY the JSON object, no other text"
        )

        # Process up to first 3 pages (legal docs often have property info on page 2-3)
        combined_result = {
            "property_address": "",
            "property_city": "",
            "property_zip": "",
            "legal_description": "",
            "amount": "",
            "grantor_name": "",
            "grantee_name": "",
        }

        for page_num, img in enumerate(images):
            try:
                text = _call_gemini(prompt, img)
                data = _parse_json_response(text)

                if data:
                    # Merge: fill in any fields that are still empty
                    for key in combined_result:
                        if not combined_result[key] and data.get(key):
                            combined_result[key] = data[key]

                    # If we have the property address + legal, stop early
                    if combined_result["property_address"] and combined_result["legal_description"]:
                        break

            except Exception as page_err:
                log.debug(f"  Gemini page {page_num+1} parse error for {doc_id}: {page_err}")
                continue

            time.sleep(GEMINI_DELAY)

        if combined_result["property_address"] or combined_result["legal_description"]:
            log.info(f"  Gemini clerk: addr={combined_result['property_address'][:40]}, "
                     f"legal={combined_result['legal_description'][:40]}")
        else:
            log.info(f"  Gemini clerk: no property info found in {doc_id}")

        return combined_result

    except Exception as e:
        log.warning(f"  Gemini clerk failed for {doc_id}: {e}")
        time.sleep(GEMINI_DELAY)
        return None
