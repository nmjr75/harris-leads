"""
Gemini Vision API extraction for Harris County foreclosure PDFs.

Uses Google Gemini 2.5 Flash to extract structured data from PDF images.
Falls back to None (triggering OCR pipeline) if Gemini is unavailable or fails.
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
        log.info("GEMINI_API_KEY not set \u2013 Gemini extraction disabled.")
except ImportError:
    log.info("google-generativeai not installed \u2013 Gemini extraction disabled.")


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
            '  "legal_description": "Lot, block, and subdivision ONLY. Keep concise.",\n'
            '  "amount": "Loan/debt amount with dollar sign",\n'
            '  "sale_date": "Foreclosure sale date in MM/DD/YYYY format",\n'
            '  "mortgagee": "Current mortgagee or loan servicer name"\n'
            "}\n\n"
            "IMPORTANT:\n"
            "- The property address is the SUBJECT PROPERTY, NOT the law firm or servicer office.\n"
            "- Law firm addresses (McCarthy and Holthus, Barrett Daffin, Mackie Wolf) are NOT property addresses.\n"
            "- For legal description, extract ONLY lot/block/subdivision, not lengthy paragraphs.\n"
            "- Return ONLY the JSON object, no markdown fences, no other text."
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
