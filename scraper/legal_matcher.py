"""Shared legal-description matcher.

Parses legal descriptions in MULTIPLE formats and resolves them against
HCAD parcels via the resolve_parcel_by_legal Postgres RPC.

Supported input formats:
  1. Pipe format (clerk lead scraper, fetch.py output):
       'SANFORD FARMS | Sec: 1 | Lot: 15 | Block: 3'
  2. Prose format (foreclosure Gemini extraction, courthouse PDFs):
       'Lot Thirteen (13) in Block Three (3) of Hidden Meadow, Sec 9'
  3. HCAD format (sometimes appears in source records):
       'LT 15 BLK 3 SANFORD FARMS SEC 1'
  4. Inline labelled (variant prose):
       'Hidden Meadow Sec 9, Lot 13, Block 3'

All formats normalize to the same (subdivision, section, lot, block)
tuple before lookup. Word-to-number conversion handles spelled-out
numbers ('Thirteen' -> 13) when digits aren't parenthesized.

Used by:
  * scraper/supabase_sync.py — fills prop_address on freshly-scraped
    records before they hit siftstack_queue, so newly-scraped records
    skip the PDF-download fallback when their legal is in HCAD.
  * scraper/refresh_records.py — historical refresh of the records
    backlog (uses an in-memory index from the local ZIP instead of
    the RPC, since the ZIP is already cached after a sync).
  * scraper/run_siftstack_worker.py — last-resort fallback when the
    queued record reaches the worker without a parcel_acct.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

log = logging.getLogger(__name__)


# ── Word -> number conversion (for prose like 'Lot Thirteen' -> 13) ──
_WORD_NUMS = {
    "zero":0, "one":1, "two":2, "three":3, "four":4, "five":5, "six":6,
    "seven":7, "eight":8, "nine":9, "ten":10, "eleven":11, "twelve":12,
    "thirteen":13, "fourteen":14, "fifteen":15, "sixteen":16,
    "seventeen":17, "eighteen":18, "nineteen":19, "twenty":20,
    "thirty":30, "forty":40, "fifty":50, "sixty":60, "seventy":70,
    "eighty":80, "ninety":90, "hundred":100,
}

def _word_to_int(text: str) -> Optional[int]:
    """Convert spelled-out number (incl. compounds like 'twenty-three')
    to int. Returns None if not parseable."""
    if not text:
        return None
    text = text.lower().strip().replace("-", " ")
    parts = [p for p in text.split() if p in _WORD_NUMS]
    if not parts:
        return None
    total = 0
    for p in parts:
        v = _WORD_NUMS[p]
        if v == 100:
            total = max(total, 1) * 100
        elif v >= 20:
            total += v
        else:
            total += v
    return total


def _norm_token(s: Optional[str]) -> Optional[str]:
    """Strip + uppercase. Strip leading zeros from purely-numeric tokens
    so '01' and '1' both compare as '1'."""
    if s is None:
        return None
    s = s.strip().upper()
    if not s:
        return None
    if s.isdigit():
        return str(int(s))
    return s


# ── Format detectors and parsers ──────────────────────────────────────

# Pipe format: "SUBDIV | Sec: 1 | Lot: 15 | Block: 3"
def _parse_pipe(legal: str):
    if "|" not in legal:
        return None
    parts = [p.strip() for p in legal.split("|")]
    subdiv = (parts[0] or "").strip().upper()
    if not subdiv:
        return None
    section = lot = block = None
    for p in parts[1:]:
        if not p or ":" not in p:
            continue
        label, val = p.split(":", 1)
        label = label.strip().lower()
        val   = val.strip()
        if label.startswith("sec"):
            section = _norm_token(val)
        elif label.startswith("lot"):
            lot = _norm_token(val)
        elif label.startswith("block"):
            block = _norm_token(val)
    return (subdiv, section, lot, block)


# HCAD format: "LT 15 BLK 3 SANFORD FARMS SEC 1"
_HCAD_RE = re.compile(
    r"^\s*LT\s+(?P<lot>\d+)\s+BLK\s+(?P<block>\d+)\s+(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)
_HCAD_SEC_TAIL = re.compile(r"\s+SEC\s+(\d+)\s*$", re.IGNORECASE)

def _parse_hcad(legal: str):
    m = _HCAD_RE.match(legal)
    if not m:
        return None
    lot = _norm_token(m.group("lot"))
    block = _norm_token(m.group("block"))
    rest = m.group("rest").strip()
    section = None
    sm = _HCAD_SEC_TAIL.search(rest)
    if sm:
        section = _norm_token(sm.group(1))
        rest = _HCAD_SEC_TAIL.sub("", rest).strip()
    subdiv = rest.upper()
    if not subdiv:
        return None
    return (subdiv, section, lot, block)


# Prose Lot/Block extraction in priority order:
#   1. Parens digit ANYWHERE between the keyword and ')' — handles
#      compound number words like "Block Two Hundred Twenty-Eight (228)".
#   2. Bare digit immediately after — "Lot 13", "Block 5".
#   3. Single word number — "Lot Thirteen" with no parens digit.
# Wrong number is worse than no number, so the parens form must win.
_LOT_PAREN_RE   = re.compile(r"\bLot\b[^()\n]{0,80}?\((\d+)\)", re.IGNORECASE)
_BLOCK_PAREN_RE = re.compile(r"\bBlock\b[^()\n]{0,80}?\((\d+)\)", re.IGNORECASE)
_LOT_PLAIN_RE   = re.compile(r"\bLot(?:\s+(?:Number|No\.?))?\s+(\d+)\b", re.IGNORECASE)
_BLOCK_PLAIN_RE = re.compile(r"\bBlock(?:\s+(?:Number|No\.?))?\s+(\d+)\b", re.IGNORECASE)
_LOT_WORD_RE    = re.compile(r"\bLot\s+([A-Za-z\-]{3,30})\b", re.IGNORECASE)
_BLOCK_WORD_RE  = re.compile(r"\bBlock\s+([A-Za-z\-]{3,30})\b", re.IGNORECASE)
_SEC_RE   = re.compile(
    r"\b(?:Sec(?:tion)?\.?)\s+(\d+)",  # tolerate 'Sec.', 'Sec', 'Section'
    re.IGNORECASE,
)

# Subdivision after 'of' before Sec/comma: "...of Hidden Meadow, Sec 9..."
# Allow optional 'THE REPLAT OF' / 'THE PARTIAL REPLAT OF' chain — they are
# not part of the subdivision name itself; we want what follows them.
_SUBDIV_OF_RE = re.compile(
    r"\bOF(?:\s+THE)?(?:\s+(?:PARTIAL\s+)?REPLAT\s+OF)?\s+"
    r"([A-Z][A-Za-z0-9'&\.\- ]{2,80}?)"
    r"\s*(?=,|\s+Sec\b|\s+Section\b|\s+Subdivision|\s+Subd\b|$)",
    re.IGNORECASE,
)
_SUBDIV_IN_RE = re.compile(
    r"\bin(?:\s+the)?\s+([A-Z][A-Za-z0-9'&\.\- ]{2,80}?)"
    r"\s+(?=Subdivision|Subd\b)",
)
# Leading subdivision is a LAST RESORT — only fires if the line doesn't
# start with a Lot/Block declaration. Otherwise we'd grab "LOT 26" as
# the subdivision in "LOT 26, BLOCK 1, OF MAPLE RIDGE PLACE...".
_SUBDIV_LEADING_RE = re.compile(
    r"^([A-Z][A-Za-z0-9'&\.\- ]{2,80}?)"
    r"\s*(?=,|\s+Sec\b|\s+Section\b)",
)
# Words that never make sense as subdivision suffixes — strip from end.
_SUBDIV_TAIL_NOISE = re.compile(
    r"\s+(?:AN\s+ADDITION|A\s+SUBDIVISION|ADDITION|SUBDIVISION|SUBD)\s*$",
    re.IGNORECASE,
)


def _extract_number(legal: str, paren_re, plain_re, word_re) -> Optional[str]:
    """Resolve a Lot/Block number. Priority:
      1. Digit in parens (handles compound number words).
      2. Bare digit right after the keyword.
      3. Single word number.
    Returns digit string or None.
    """
    m = paren_re.search(legal)
    if m:
        return _norm_token(m.group(1))
    m = plain_re.search(legal)
    if m:
        return _norm_token(m.group(1))
    m = word_re.search(legal)
    if m:
        n = _word_to_int(m.group(1))
        if n is not None:
            return str(n)
    return None


def _parse_prose(legal: str):
    lot   = _extract_number(legal, _LOT_PAREN_RE,   _LOT_PLAIN_RE,   _LOT_WORD_RE)
    block = _extract_number(legal, _BLOCK_PAREN_RE, _BLOCK_PLAIN_RE, _BLOCK_WORD_RE)
    sec_m = _SEC_RE.search(legal)
    section = _norm_token(sec_m.group(1)) if sec_m else None

    # Skip the leading-subdiv pattern entirely if the line starts with a
    # Lot or Block declaration — otherwise we'd grab "LOT 26" as the
    # subdivision in "LOT 26, BLOCK 1, OF MAPLE RIDGE PLACE..."
    starts_with_lot_or_block = bool(re.match(
        r"^\s*(?:LOT|BLOCK|BLK|LT)\b", legal, re.IGNORECASE
    ))
    patterns = [_SUBDIV_OF_RE, _SUBDIV_IN_RE]
    if not starts_with_lot_or_block:
        patterns.append(_SUBDIV_LEADING_RE)

    subdiv = None
    for pat in patterns:
        m = pat.search(legal)
        if m:
            cand = m.group(1).strip().rstrip(",").strip()
            # Strip trailing noise like "AN ADDITION", "SUBDIVISION", etc.
            cand = _SUBDIV_TAIL_NOISE.sub("", cand).strip()
            cand = re.sub(
                r"\s+(?:Subdivision|Subd|Section|Sec)\s*$",
                "", cand, flags=re.IGNORECASE,
            ).strip()
            if cand and len(cand) >= 3:
                subdiv = cand.upper()
                break
    if not subdiv:
        return None
    return (subdiv, section, lot, block)


def parse_clerk_legal(
    legal: Optional[str],
) -> Optional[tuple[str, Optional[str], Optional[str], Optional[str]]]:
    """Parse a legal description in any supported format.
    Returns (subdivision, section, lot, block) or None if unparseable.
    Subdivision is required; the rest may be None.

    Tries formats in this priority order:
      1. Pipe format (most reliable, structured)
      2. HCAD 'LT N BLK N SUBDIV SEC N' (also structured)
      3. Prose extraction (regex-based, lossier)
    First parser to return a tuple with a non-empty subdivision wins.
    """
    if not legal:
        return None
    legal = legal.strip()
    if not legal:
        return None
    for parser in (_parse_pipe, _parse_hcad, _parse_prose):
        try:
            result = parser(legal)
        except Exception:
            continue
        if result and result[0]:
            return result
    return None


def resolve_legal(client, legal: Optional[str]) -> Optional[dict]:
    """Resolve a clerk legal to a HCAD parcel via the
    resolve_parcel_by_legal RPC. Returns a dict with parcel_acct +
    situs fields if a unique match exists, else None.

    The RPC handles the matching tiers internally (subdivision is
    required; missing section/lot/block widen the match)."""
    parsed = parse_clerk_legal(legal)
    if not parsed:
        return None
    subdiv, section, lot, block = parsed
    try:
        rpc = client.rpc(
            "resolve_parcel_by_legal",
            {
                "p_subdiv":  subdiv,
                "p_section": section,
                "p_lot":     lot,
                "p_block":   block,
            },
        ).execute()
        acct = rpc.data
        if not acct:
            return None
    except Exception as e:
        log.debug("resolve_parcel_by_legal RPC failed: %s", e)
        return None

    # Pull situs fields from property_data
    try:
        resp = (
            client.table("property_data")
            .select("parcel_acct, situs_address, situs_city, situs_zip, "
                    "owner_name, legal_description")
            .eq("parcel_acct", acct)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.debug("property_data fetch for %s failed: %s", acct, e)
        return None
