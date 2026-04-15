"""
Gemini Vision API extraction for Harris County clerk PDFs.

Uses Google Gemini 2.5 Flash to extract structured data from PDF images.
Falls back to None (triggering OCR/pdfplumber pipeline) if Gemini is unavailable or fails.

Contains two functions:
  - parse_pdf_with_gemini(): For foreclosure posting PDFs (original)
  - parse_clerk_pdf_with_gemini(): For clerk filing PDFs (LP, liens, judgments, probate, etc.)
"""

import json
import logging
import os
import re
from io import BytesIO

log = logging.getLogger(__name__)

# --- Gemini setup (soft dependency) ---
HAS_GEMINI = False
try:
    import google.generativeai as genai
    from pdf2image import convert_from_bytes

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if api_key:
        genai.configure(api_key=api_key)
        HAS_GEMINI = True
        log.info("Gemini Vision API configured successfully.")
    else:
        log.info("GEMINI_API_KEY not set – Gemini extraction disabled.")
except ImportError:
    log.info("google-generativeai not installed – Gemini extraction disabled.")


def parse_pdf_with_gemini(pdf_bytes, doc_id):
    """Extract fields from foreclosure PDF using Gemini Vision API.

    Returns parsed dict on success, None on failure (triggers OCR fallback).
    """
    if not HAS_GEMINI:
        return None

    try:
        images = convert_from_bytes(pdf_bytes, dpi=200)
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

        model = genai.GenerativeModel("gemini-2.5-flash")

        # Convert first page to PNG bytes for Gemini
        img_buf = BytesIO()
        images[0].save(img_buf, format="PNG")
        img_buf.seek(0)

        response = model.generate_content([
            prompt,
            {"mime_type": "image/png", "data": img_buf.read()}
        ])

        text = response.text.strip()
        # Remove markdown code fences if present
        if text.startswith("```"):
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)

        data = json.loads(text)

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
            img2 = BytesIO()
            images[1].save(img2, format="PNG")
            img2.seek(0)
            try:
                resp2 = model.generate_content([
                    prompt, {"mime_type": "image/png", "data": img2.read()}
                ])
                t2 = resp2.text.strip()
                if t2.startswith("```"):
                    t2 = re.sub(r'^```(?:json)?\s*', '', t2)
                    t2 = re.sub(r'\s*```$', '', t2)
                d2 = json.loads(t2)
                for key in ["property_address", "property_city", "property_zip",
                            "legal_description", "grantor", "amount", "sale_date", "mortgagee"]:
                    if not result.get(key) and d2.get(key):
                        result[key] = d2[key]
            except Exception:
                pass

        log.info(f"  Gemini: addr={result['property_address'][:40]}, legal={result['legal_description'][:40]}")
        return result

    except Exception as e:
        log.warning(f"  Gemini failed for {doc_id}: {e}")
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
    if not HAS_GEMINI:
        return None

    try:
        images = convert_from_bytes(pdf_bytes, dpi=200)
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

        model = genai.GenerativeModel("gemini-2.5-flash")

        # Process up to first 3 pages (legal docs often have property info on page 2-3)
        max_pages = min(len(images), 3)
        combined_result = {
            "property_address": "",
            "property_city": "",
            "property_zip": "",
            "legal_description": "",
            "amount": "",
            "grantor_name": "",
            "grantee_name": "",
        }

        for page_num in range(max_pages):
            img_buf = BytesIO()
            images[page_num].save(img_buf, format="PNG")
            img_buf.seek(0)

            try:
                response = model.generate_content([
                    prompt,
                    {"mime_type": "image/png", "data": img_buf.read()}
                ])

                text = response.text.strip()
                # Remove markdown code fences if present
                if text.startswith("```"):
                    text = re.sub(r'^```(?:json)?\s*', '', text)
                    text = re.sub(r'\s*```$', '', text)

                data = json.loads(text)

                # Merge: fill in any fields that are still empty
                for key in combined_result:
                    if not combined_result[key] and data.get(key):
                        combined_result[key] = data[key]

                # If we have the property address, we can stop early
                if combined_result["property_address"] and combined_result["legal_description"]:
                    break

            except Exception as page_err:
                log.debug(f"  Gemini page {page_num+1} parse error for {doc_id}: {page_err}")
                continue

        if combined_result["property_address"] or combined_result["legal_description"]:
            log.info(f"  Gemini clerk: addr={combined_result['property_address'][:40]}, "
                     f"legal={combined_result['legal_description'][:40]}")
        else:
            log.info(f"  Gemini clerk: no property info found in {doc_id}")

        return combined_result

    except Exception as e:
        log.warning(f"  Gemini clerk failed for {doc_id}: {e}")
        return None
