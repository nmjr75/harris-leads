"""SiftMap property enrichment worker (self-contained — no SiftStack dep).

Pulls dashboard contact addresses lacking a property_enrichment row,
navigates to DataSift's SiftMap with each address as a URL query param,
scrapes the Property Details side panel, and upserts the structured
fields into Supabase. Cache forever — each address looked up exactly
once. Daily cron picks up new addresses as scored leads accumulate.

Architecture:
  1. Query addresses_needing_enrichment(limit) RPC → list of addresses
  2. Login to DataSift (one session reused for the whole run)
  3. For each address:
        - Navigate to /siftmap?location=<encoded JSON>
        - Wait for the right-side Property Details panel to render
        - Capture panel inner_text + parse with defensive regex
        - Upsert into property_enrichment (normalized_address PK)
        - Sleep 3-5s between addresses (polite + rate-limit safety)
  4. Exit. Daily cron picks the next batch.

Env vars:
    SUPABASE_URL                  harris-leads project URL
    SUPABASE_SECRET_KEY           service-role key
    DATASIFT_EMAIL                login email
    DATASIFT_PASSWORD             login password
    SIFTMAP_PER_RUN_LIMIT         max lookups per run (default 100)
    SIFTMAP_HEADED                "1" to show browser (debug only)
    SIFTMAP_DELAY_MIN/MAX         seconds between lookups (default 3-5)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

log = logging.getLogger("siftmap_enricher")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)


# ── Env ──────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
DS_EMAIL = os.environ.get("DATASIFT_EMAIL", "").strip()
DS_PASSWORD = os.environ.get("DATASIFT_PASSWORD", "").strip()
PER_RUN_LIMIT = int(os.environ.get("SIFTMAP_PER_RUN_LIMIT", "100"))
HEADED = os.environ.get("SIFTMAP_HEADED", "0") == "1"
DELAY_SEC_MIN = float(os.environ.get("SIFTMAP_DELAY_MIN", "3.0"))
DELAY_SEC_MAX = float(os.environ.get("SIFTMAP_DELAY_MAX", "5.0"))

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing SUPABASE_URL / SUPABASE_SECRET_KEY")
    sys.exit(1)
if not DS_EMAIL or not DS_PASSWORD:
    log.error("Missing DATASIFT_EMAIL / DATASIFT_PASSWORD")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    log.error("supabase-py not installed; pip install supabase>=2.10.0")
    sys.exit(1)

try:
    from playwright.async_api import async_playwright, Page, TimeoutError as PwTimeout
except ImportError:
    log.error("playwright not installed")
    sys.exit(1)


# ── Constants ────────────────────────────────────────────────────────
SIFTMAP_BASE = "https://app.reisift.io/siftmap"
LOGIN_URL = "https://app.reisift.io/login"
DASHBOARD_URL = "https://app.reisift.io/dashboard/general"

PANEL_SELECTOR_CANDIDATES = [
    '[class*="PropertyDetails" i]',
    '[class*="property-details" i]',
    'aside:has-text("Property Details")',
    'div:has-text("Property Details")',
    'section:has-text("Property Details")',
]


# ── DataSift login (inlined; mirrors SiftStack/datasift_core.py) ─────
async def datasift_login(page: Page) -> bool:
    """Fresh DataSift login. Returns True on success."""
    log.info("Navigating to DataSift login page")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    try:
        await page.get_by_role("textbox", name="Email").fill(DS_EMAIL)
        await page.get_by_role("textbox", name="Password").fill(DS_PASSWORD)
    except Exception as e:
        log.error("Could not fill credentials: %s", e)
        return False

    # Hidden checkboxes — click labels, not inputs.
    for label_text in ["Remember me", "I've read and agree"]:
        loc = page.locator(f'label:has-text("{label_text}")')
        try:
            if await loc.count() > 0:
                await loc.first.click()
        except Exception:
            pass

    try:
        await page.get_by_role("button", name="Sign In").click()
    except Exception as e:
        log.error("Sign In click failed: %s", e)
        return False

    try:
        await page.wait_for_url("**/dashboard/general**", timeout=15000)
    except PwTimeout:
        if "/login" in page.url:
            log.error("Login failed — still on /login")
            return False
    log.info("DataSift login successful")
    return True


async def dismiss_popups(page: Page) -> None:
    """Remove DataSift's Beamer NPS overlay + notification popups, both of
    which intercept clicks. Idempotent + safe to re-run."""
    try:
        for text in ["NO, THANKS", "No, thanks", "No Thanks", "Not Now", "Dismiss"]:
            el = page.get_by_text(text, exact=True)
            if await el.count() > 0:
                try:
                    await el.first.click(force=True)
                    await page.wait_for_timeout(500)
                    return
                except Exception:
                    continue

        await page.evaluate("""() => {
            const nps = document.getElementById('npsIframeContainer');
            if (nps) nps.remove();
            document.querySelectorAll('[class*="nps-iframe"], [class*="beamer"]').forEach(el => el.remove());
            const overlays = document.querySelectorAll('[class*="notification"], [class*="Notification"]');
            for (const o of overlays) {
                if (o.textContent && o.textContent.includes('notifications')) o.remove();
            }
        }""")
    except Exception:
        pass


# ── Helpers ──────────────────────────────────────────────────────────
def normalize_address(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", s or "").upper()


def build_full_address(raw: str, city: str | None, state: str | None, zipc: str | None) -> str:
    parts = [raw or ""]
    tail_bits = []
    if city: tail_bits.append(city)
    if state: tail_bits.append(state)
    tail_line = ", ".join(tail_bits) if tail_bits else ""
    if tail_line and zipc:
        tail_line = f"{tail_line} {zipc}"
    elif zipc and not tail_line:
        tail_line = zipc
    if tail_line:
        parts.append(tail_line)
    return ", ".join(p.strip() for p in parts if p and p.strip())


def siftmap_url(full_address: str) -> str:
    payload = {"searchType": "address", "title": full_address, "value": full_address}
    return f"{SIFTMAP_BASE}?location={quote(json.dumps(payload, separators=(',', ':')))}"


def to_int(s: str | None) -> int | None:
    if s is None: return None
    m = re.search(r"-?\d+", s.replace(",", ""))
    return int(m.group()) if m else None


def to_float(s: str | None) -> float | None:
    if s is None: return None
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    return float(m.group()) if m else None


def parse_money(s: str | None) -> float | None:
    if not s: return None
    s = s.strip().upper().replace("$", "").replace(",", "")
    m = re.match(r"(-?\d+(?:\.\d+)?)\s*([KM]?)", s)
    if not m: return None
    n = float(m.group(1))
    suf = m.group(2)
    if suf == "K": n *= 1_000
    elif suf == "M": n *= 1_000_000
    return n


# ── Panel scrape + parse ─────────────────────────────────────────────
async def find_property_panel_text(page: Page) -> str | None:
    """Return the inner_text of whatever container holds the SiftMap
    Property Details. Strategy: find candidates by class, pick the
    LARGEST text blob (not first/smallest), and fall back to the full
    body text — the regex parser anchors on unique labels ('EST. VALUE',
    'EQUITY', 'BDS', '% EQUITY'), so a wider blob is safe.
    """
    # Strategy 1: every plausible panel container; keep the largest.
    selectors = [
        '[class*="PropertyDetails"]',
        '[class*="property-details"]',
        '[class*="DrawerPanel"]',
        '[class*="SidePanel"]',
        '[class*="DetailPanel"]',
        'aside',
    ]
    best_text = ""
    for sel in selectors:
        try:
            locs = page.locator(sel)
            n = await locs.count()
            for i in range(min(n, 12)):
                try:
                    t = await locs.nth(i).inner_text(timeout=2000)
                    if t and len(t) > len(best_text):
                        best_text = t
                except Exception:
                    continue
        except Exception:
            continue

    if best_text and len(best_text) > 200 and \
       any(k in best_text.upper() for k in ("EQUITY", "EST", "BDS", "SQFT")):
        return best_text

    # Strategy 2: anchor on 'EST. VALUE' text and walk up 6 levels.
    try:
        loc = page.locator('text=/EST\\.?\\s*VALUE/i').first
        if await loc.count() > 0:
            t = await loc.evaluate(
                "el => { let n=el; for(let i=0;i<8;i++){ if(!n.parentElement)break; n=n.parentElement;} return n.innerText||''; }"
            )
            if t and len(t) > 200:
                return t
    except Exception:
        pass

    # Strategy 3: dump entire body. Regex labels are unique enough.
    try:
        t = await page.locator("body").inner_text(timeout=5000)
        if t and any(k in t.upper() for k in ("EQUITY", "EST. VALUE", "EST VALUE")):
            return t
    except Exception:
        pass

    return None


def parse_panel(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    upper = text.upper()

    m = re.search(r"\$([\d,]+(?:\.\d+)?[KM]?)\s*\n?\s*EST\.?\s*VALUE", upper) \
        or re.search(r"EST\.?\s*VALUE\s*\n?\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper)
    if m: out["estimated_value"] = parse_money("$" + m.group(1))

    m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*\n?\s*EQUITY", upper) \
        or re.search(r"EQUITY\s*\n?\s*(\d+(?:\.\d+)?)\s*%", upper)
    if m: out["equity_pct"] = to_float(m.group(1))

    m = re.search(r"(\d+)\s*BDS?", upper)
    if m: out["bedrooms"] = to_int(m.group(1))
    m = re.search(r"(\d+(?:\.\d+)?)\s*BA(?:THS?)?", upper)
    if m: out["bathrooms"] = to_float(m.group(1))
    m = re.search(r"([\d,]+)\s*SQFT", upper)
    if m: out["sqft"] = to_int(m.group(1))
    m = re.search(r"([\d.]+)\s*ACRES?", upper)
    if m: out["acres"] = to_float(m.group(1))

    for kw in ["SINGLE FAMILY RESIDENTIAL", "MULTI FAMILY", "TOWNHOUSE",
               "CONDO", "MOBILE HOME", "VACANT LAND", "DUPLEX"]:
        if kw in upper:
            out["structure_type"] = kw.title()
            break
    for kw in ["INDIVIDUAL OWNED", "LLC OWNED", "TRUST OWNED",
               "CORPORATE OWNED", "BANK OWNED"]:
        if kw in upper:
            out["ownership_type"] = kw.title()
            break

    out["off_market"] = "OFF MARKET" in upper
    out["owner_occupied"] = "OWNER OCCUPIED" in upper

    m = re.search(r"MLS\s+STATUS\s*\n+\s*([A-Z][A-Za-z ]+?)(?:\n|$)", text)
    if m:
        out["mls_status"] = m.group(1).strip()
    else:
        if "OFF MARKET" in upper: out["mls_status"] = "Off Market"
        elif "ON MARKET" in upper: out["mls_status"] = "On Market"

    m = re.search(r"(\d+)\s*DAYS?\s+ON\s+MARKET", upper)
    if m: out["days_on_market"] = to_int(m.group(1))

    m = re.search(r"YEAR\s+BUILT\s*[:\n\s]+\s*(\d{4})", upper)
    if m: out["year_built"] = to_int(m.group(1))

    for key, label in [
        ("ai_off_market_investor", "OFF-MARKET INVESTOR"),
        ("ai_on_market_investor",  "ON-MARKET INVESTOR"),
        ("ai_realtor",             "REALTOR"),
    ]:
        pat = re.compile(re.escape(label) + r"\s*[:\n]+\s*(\d+(?:\.\d+)?)")
        m = pat.search(upper)
        if m: out[key] = to_float(m.group(1))

    # Distressor signal flags (visible in the DISTRESSORS list).
    out["high_equity"]    = "HIGH EQUITY"     in upper
    out["low_equity"]     = "LOW EQUITY"      in upper
    out["absentee_owner"] = "ABSENTEE"        in upper
    out["vacant"]         = "VACANT"          in upper and "VACANT LAND" not in upper

    # Collect distressor labels into an array. DataSift typically lists them
    # under a "DISTRESSORS" header on individual lines.
    known_distressors = [
        "High Equity", "Low Equity", "Free & Clear", "Owner Occupied",
        "Absentee Owners", "Vacant", "Low Income", "Bad Credit",
        "Senior Homeowners", "Tax Delinquent", "Pre-Foreclosure",
        "Bankruptcy", "Divorce", "Probate", "Tired Landlord",
    ]
    signals = [d for d in known_distressors if d.upper() in upper]
    if signals:
        out["distressor_signals"] = signals

    # County assessed value — SiftMap sometimes shows it as "County Value"
    # or "Assessed Value". Try both, take the dollar amount.
    m = re.search(r"(?:COUNTY\s+VALUE|ASSESSED\s+VALUE)\s*[:\n]+\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper)
    if m: out["county_assessed_value"] = parse_money("$" + m.group(1))

    # Mortgage balance (Owner tab). Labels vary: "Mortgage Balance",
    # "Outstanding Mortgage", "Loan Balance".
    for label in ["MORTGAGE BALANCE", "OUTSTANDING MORTGAGE", "LOAN BALANCE", "MORTGAGE"]:
        m = re.search(re.escape(label) + r"\s*[:\n]+\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper)
        if m:
            v = parse_money("$" + m.group(1))
            if v and v > 1000:  # reject "0" or single-digit garbage
                out["mortgage_balance"] = v
                break

    # Last sale (price + date). Labels: "Last Sale Date", "Last Sale Price",
    # "Sale Date", "Sale Price".
    m = re.search(r"LAST\s+SALE\s+PRICE\s*[:\n]+\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper) \
        or re.search(r"SALE\s+PRICE\s*[:\n]+\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper)
    if m: out["last_sale_price"] = parse_money("$" + m.group(1))

    m = re.search(r"LAST\s+SALE\s+DATE\s*[:\n]+\s*([\d/\-]{6,10})", upper) \
        or re.search(r"SALE\s+DATE\s*[:\n]+\s*([\d/\-]{6,10})", upper)
    if m:
        # Accept "MM/DD/YYYY" or "YYYY-MM-DD" — pass through as ISO if possible.
        ds = m.group(1)
        try:
            from datetime import datetime as _dt
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    out["last_sale_date"] = _dt.strptime(ds, fmt).date().isoformat()
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # Tax delinquent value + year.
    m = re.search(r"TAX\s+DELINQUENT\s+VALUE\s*[:\n]+\s*\$?([\d,]+(?:\.\d+)?[KM]?)", upper)
    if m: out["tax_delinquent_value"] = parse_money("$" + m.group(1))
    m = re.search(r"TAX\s+DELINQUENT\s+YEAR\s*[:\n]+\s*(\d{4})", upper) \
        or re.search(r"DELINQUENT\s+SINCE\s*[:\n]+\s*(\d{4})", upper)
    if m: out["tax_delinquent_year"] = to_int(m.group(1))

    return out


# ── DB ───────────────────────────────────────────────────────────────
def fetch_pending_addresses(sb, limit: int) -> list[dict[str, Any]]:
    try:
        res = sb.rpc("addresses_needing_enrichment", {"p_limit": limit}).execute()
        return res.data or []
    except Exception as e:
        log.error("addresses_needing_enrichment RPC failed: %s", e)
        return []


def upsert_enrichment(sb, row: dict[str, Any]) -> bool:
    try:
        sb.table("property_enrichment").upsert(row, on_conflict="normalized_address").execute()
        return True
    except Exception as e:
        log.error("upsert failed (%s): %s", row.get("normalized_address"), e)
        return False


# ── Per-address worker ───────────────────────────────────────────────
async def search_via_input(page: Page, full_address: str) -> bool:
    """Drive the SiftMap search box like a human: click the address input,
    type the address, wait for autocomplete, click the first suggestion.

    The URL-query approach (location={searchType,title,value}) doesn't
    trigger the React SPA to fetch the property — confirmed by capturing
    completely wrong properties on 5 different addresses. The search-box
    flow IS the workflow Nelson uses manually, so it's the reliable path.
    Returns True if the SPA appears to have navigated to a per-property
    detail view; False otherwise.
    """
    # Land on bare SiftMap.
    try:
        await page.goto(SIFTMAP_BASE, wait_until="domcontentloaded", timeout=45_000)
    except Exception as e:
        log.warning("siftmap nav failed: %s", e)
        return False
    await page.wait_for_timeout(5000)
    await dismiss_popups(page)

    # Find the search input — placeholder is "Address, city, county or zip".
    placeholders = [
        "Address, city, county or zip",
        "Address, city, county or",
        "Address",
    ]
    search_input = None
    for ph in placeholders:
        loc = page.locator(f'input[placeholder*="{ph}" i]').first
        try:
            if await loc.count() > 0:
                search_input = loc
                break
        except Exception:
            continue
    if not search_input:
        log.warning("Could not find SiftMap search input")
        return False

    try:
        await search_input.click()
        await page.wait_for_timeout(500)
        # Clear any prior value, then type slowly so autocomplete kicks in.
        await search_input.fill("")
        await page.wait_for_timeout(300)
        await search_input.type(full_address, delay=40)
    except Exception as e:
        log.warning("Failed to type into search box: %s", e)
        return False

    # Wait for the autocomplete dropdown to populate.
    await page.wait_for_timeout(2500)

    # Click the first autocomplete result. DataSift typically renders these
    # as styled options under the input — try a few selector patterns.
    clicked = False
    for sel in [
        '[class*="autocomplete"] [class*="Item"]',
        '[class*="Autocomplete"] [class*="Item"]',
        '[class*="SuggestionItem"]',
        '[class*="result-item"]',
        '[role="option"]',
        '[class*="DropdownItem"]',
    ]:
        try:
            opt = page.locator(sel).first
            if await opt.count() > 0 and await opt.is_visible():
                await opt.click()
                clicked = True
                break
        except Exception:
            continue
    if not clicked:
        # Fallback: just press Enter to submit the typed value.
        try:
            await search_input.press("Enter")
        except Exception:
            return False

    # Give the SPA time to fetch + render the detail panel.
    await page.wait_for_timeout(10_000)
    await dismiss_popups(page)
    try:
        await page.wait_for_selector(
            'text=/EST\\.?\\s*VALUE|EQUITY|BDS/i', timeout=10_000
        )
    except Exception:
        pass
    return True


async def click_owner_tab(page: Page) -> bool:
    """Click the Owner tab on the SiftMap detail panel so its content
    (mortgage balance + owner details) becomes part of the panel text.
    Returns True if the click landed."""
    for sel in [
        'button:has-text("Owner")',
        '[role="tab"]:has-text("Owner")',
        'div:has-text("Owner") >> visible=true',
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                await loc.click()
                await page.wait_for_timeout(2500)
                return True
        except Exception:
            continue
    return False


async def run_one_address(page: Page, addr: dict[str, Any]) -> dict[str, Any] | None:
    raw = (addr.get("raw_address") or "").strip()
    city = (addr.get("city") or "").strip() or None
    state = (addr.get("state") or "").strip() or None
    zipc = (addr.get("postal_code") or "").strip() or None
    if not raw:
        return None

    full = build_full_address(raw, city, state, zipc)
    norm = normalize_address(raw)

    log.info("[%s] %s", norm[:18], full[:80])
    ok = await search_via_input(page, full)
    if not ok:
        log.warning("search-via-input bailed for %s", full[:60])

    # Capture Property tab (default view) first.
    property_text = await find_property_panel_text(page) or ""

    # Then click Owner tab so its content joins the panel text. Mortgage
    # balance + sale history typically live behind that tab.
    owner_text = ""
    try:
        if await click_owner_tab(page):
            owner_text = await find_property_panel_text(page) or ""
    except Exception as e:
        log.debug("click_owner_tab failed: %s", e)

    combined = (property_text + "\n\n=== OWNER TAB ===\n\n" + owner_text).strip() \
        if owner_text else property_text

    now_iso = datetime.now(timezone.utc).isoformat()
    if not combined or len(combined) < 50:
        log.warning("no Property Details panel for %s", full[:60])
        # Cache a "no panel" row so we don't re-attempt every cron tick.
        return {
            "normalized_address": norm,
            "raw_address": raw,
            "city": city, "state": state, "postal_code": zipc,
            "source": "siftmap_no_panel",
            "raw_panel_text": None,
            "looked_up_at": now_iso,
            "last_refreshed_at": now_iso,
        }

    fields = parse_panel(combined)
    # Cross-check: refuse to save a row whose panel text doesn't contain at
    # least part of the address we searched for. Prevents us from caching
    # whatever generic page DataSift falls back to when a search fails.
    addr_tokens = [t for t in re.split(r"\W+", raw) if len(t) >= 3]
    if addr_tokens:
        hits = sum(1 for t in addr_tokens if t.upper() in combined.upper())
        if hits < max(1, len(addr_tokens) // 3):
            log.warning(
                "panel text doesn't reference '%s' (%d/%d tokens) — caching as miss",
                raw, hits, len(addr_tokens),
            )
            return {
                "normalized_address": norm,
                "raw_address": raw,
                "city": city, "state": state, "postal_code": zipc,
                "source": "siftmap_address_mismatch",
                "raw_panel_text": combined[:8000],
                "looked_up_at": now_iso,
                "last_refreshed_at": now_iso,
            }
    fields.update({
        "normalized_address": norm,
        "raw_address": raw,
        "city": city, "state": state, "postal_code": zipc,
        "source": "siftmap",
        "raw_panel_text": combined[:8000],
        "looked_up_at": now_iso,
        "last_refreshed_at": now_iso,
    })
    return fields


# ── Main ─────────────────────────────────────────────────────────────
async def main() -> int:
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    pending = fetch_pending_addresses(sb, PER_RUN_LIMIT)
    if not pending:
        log.info("Nothing to enrich — exiting clean.")
        return 0

    log.info("Will enrich %d addresses (cap=%d)", len(pending), PER_RUN_LIMIT)

    successes = 0
    failures = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not HEADED)
        ctx = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            accept_downloads=False,
        )
        page = await ctx.new_page()

        if not await datasift_login(page):
            await browser.close()
            return 2
        await dismiss_popups(page)

        for i, addr in enumerate(pending, 1):
            try:
                row = await run_one_address(page, addr)
                if row and upsert_enrichment(sb, row):
                    successes += 1
                else:
                    failures += 1
            except Exception:
                failures += 1
                log.error("address %d/%d unhandled error:\n%s", i, len(pending), traceback.format_exc())

            if i < len(pending):
                await page.wait_for_timeout(int(random.uniform(DELAY_SEC_MIN, DELAY_SEC_MAX) * 1000))

        await browser.close()

    log.info("Done. successes=%d failures=%d total=%d", successes, failures, len(pending))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
