// Supabase Edge Function: enrich-hitlist-contacts
//
// One-shot backfill that turns Hit List "slim" contacts into fully-enriched
// rows on the dashboard. Iterates every contact currently at the
// SELLER DISPOSITION → HIT LIST pipeline stage and:
//   1. Pulls fresh contact data from GHL's Contacts API (address fields).
//   2. Upserts the address into public.ghl_contact_address_overrides keyed
//      by contact_id, so the dashboard can render an address line + the
//      "Open in REI AI Leads" link for these rows.
//   3. Dispatches the existing siftmap-enricher-on-demand GitHub workflow
//      so property data (beds, baths, sqft, year built, est value, equity,
//      market status) lands in public.property_enrichment under the same
//      normalized_address scheme scored leads use.
//
// Auth model identical to sync-ghl-contact-tags: gateway anon-key required,
// JWT verification OFF on the deployed function, app-level auth via
// X-Webhook-Secret header (same TAGS_WEBHOOK_SECRET secret is reused).
//
// Invocation:
//   POST {} — backfills every Hit List contact
//   POST {"contact_id": "..."} — backfills one specific contact (idempotent)
//
// Env vars (project-level secrets):
//   TAGS_WEBHOOK_SECRET        shared secret (reused)
//   GHL_API_TOKEN              REI Reply Private Integration Token
//   GITHUB_PAT                 fine-grained PAT with Contents+Actions:write
//   GITHUB_REPO                nmjr75/harris-leads
//   SUPABASE_URL               auto-injected
//   SUPABASE_SERVICE_ROLE_KEY  auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WEBHOOK_SECRET = Deno.env.get("TAGS_WEBHOOK_SECRET") ?? "";
const GHL_API_TOKEN = Deno.env.get("GHL_API_TOKEN") ?? "";
const GITHUB_PAT = Deno.env.get("GITHUB_PAT") ?? "";
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") ?? "nmjr75/harris-leads";

// Same pipeline stage ID the dashboard uses for the Hit List pill.
const HIT_LIST_STAGE_ID = "8c02dadc-7d76-42f9-9ee0-17e13659a4fa";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-webhook-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function jsonResponse(status: number, body: Record<string, unknown>) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

type GhlContact = {
  address?: string | null;
  address1?: string | null;
  city?: string | null;
  state?: string | null;
  postalCode?: string | null;
  postal_code?: string | null;
};

async function fetchGhlContact(contactId: string): Promise<GhlContact | null> {
  const resp = await fetch(
    `https://services.leadconnectorhq.com/contacts/${contactId}`,
    {
      headers: {
        "Authorization": `Bearer ${GHL_API_TOKEN}`,
        "Version": "2021-07-28",
        "Accept": "application/json",
      },
    },
  );
  if (!resp.ok) {
    const body = await resp.text();
    console.warn(`GHL_FETCH_FAIL contact=${contactId} status=${resp.status} body=${body.slice(0, 150)}`);
    return null;
  }
  const data = await resp.json();
  return (data?.contact ?? data) as GhlContact;
}

async function dispatchSiftmapEnrichment(contactId: string): Promise<boolean> {
  if (!GITHUB_PAT) {
    console.warn(`DISPATCH_SKIP contact=${contactId} — GITHUB_PAT not configured`);
    return false;
  }
  const ghResp = await fetch(
    `https://api.github.com/repos/${GITHUB_REPO}/dispatches`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_PAT}`,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "harris-leads-hitlist-enrich",
      },
      body: JSON.stringify({
        event_type: "new-contact-enrich",
        client_payload: { contact_id: contactId },
      }),
    },
  );
  if (!ghResp.ok) {
    const errText = await ghResp.text();
    console.error(`DISPATCH_FAIL contact=${contactId} status=${ghResp.status} body=${errText.slice(0, 200)}`);
    return false;
  }
  return true;
}

async function enrichOne(sb: any, contactId: string): Promise<{ ok: boolean; reason?: string; address?: string | null }> {
  const c = await fetchGhlContact(contactId);
  if (!c) return { ok: false, reason: "GHL fetch failed" };
  const address = c.address1 ?? c.address ?? null;
  const city = c.city ?? null;
  const state = c.state ?? null;
  const postalCode = c.postalCode ?? c.postal_code ?? null;

  if (!address && !city && !state && !postalCode) {
    // No address fields at all — skip the upsert, just dispatch enrichment
    // (which may have its own internal address discovery path).
    await dispatchSiftmapEnrichment(contactId);
    return { ok: true, address: null };
  }

  const { error: upErr } = await sb.from("ghl_contact_address_overrides").upsert({
    contact_id: contactId,
    address,
    city,
    state,
    postal_code: postalCode,
    source: "ghl_api",
    updated_at: new Date().toISOString(),
  }, { onConflict: "contact_id" });
  if (upErr) return { ok: false, reason: `upsert: ${upErr.message}` };

  await dispatchSiftmapEnrichment(contactId);
  return { ok: true, address };
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
  if (req.method !== "POST") return jsonResponse(405, { error: "method not allowed" });

  if (WEBHOOK_SECRET) {
    const provided = req.headers.get("x-webhook-secret") ?? "";
    if (provided !== WEBHOOK_SECRET) {
      return jsonResponse(401, { error: "invalid webhook secret" });
    }
  }

  if (!GHL_API_TOKEN) return jsonResponse(500, { error: "GHL_API_TOKEN not configured" });

  let payload: any = {};
  try { payload = await req.json(); } catch { /* empty body is OK */ }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Single-contact mode — useful for manual retries.
  if (payload?.contact_id) {
    const result = await enrichOne(sb, String(payload.contact_id));
    return jsonResponse(200, { mode: "single", contact_id: payload.contact_id, ...result });
  }

  // Backfill mode — iterate every Hit List contact_id from the
  // ghl_current_opportunity table (mirrors what the dashboard pulls).
  const { data, error } = await sb
    .from("ghl_current_opportunity")
    .select("contact_id, contact_name")
    .eq("pipeline_stage_id", HIT_LIST_STAGE_ID);

  if (error) {
    console.error(`HIT_LIST_QUERY_ERR ${error.message}`);
    return jsonResponse(500, { error: "hit-list query failed", detail: error.message });
  }

  const seen = new Set<string>();
  const targets: { contact_id: string; contact_name: string | null }[] = [];
  for (const row of (data || [])) {
    const cid = row?.contact_id;
    if (!cid || seen.has(cid)) continue;
    seen.add(cid);
    targets.push({ contact_id: cid, contact_name: row.contact_name ?? null });
  }

  console.log(`HITLIST_ENRICH_START total=${targets.length}`);

  const results: any[] = [];
  let ok = 0, fail = 0;
  for (const t of targets) {
    const r = await enrichOne(sb, t.contact_id);
    if (r.ok) ok++; else fail++;
    results.push({ contact_id: t.contact_id, contact_name: t.contact_name, ...r });
  }

  console.log(`HITLIST_ENRICH_DONE total=${targets.length} ok=${ok} fail=${fail}`);
  return jsonResponse(200, {
    mode: "backfill",
    total: targets.length,
    ok,
    fail,
    results,
  });
});
