// Supabase Edge Function: enrich-on-snapshot (v2)
//
// Receives Database Webhook events from ghl_conversation_snapshots INSERTs
// and dispatches the SiftMap on-demand GitHub workflow for contacts that
// aren't yet in property_enrichment.
//
// v2 change: addresses are pulled from the ghl_latest_contact_address view
// (keyed by contact_id), NOT from the snapshot row's contact_address column,
// because that column is frequently NULL on raw snapshot records — the
// dashboard's address data lives in the view, not the underlying table.
// v1 was silently exiting on every invocation because of this.
//
// JWT verification: should be OFF for this function (Settings → Verify JWT off).
// Gateway anon-key auth still applies. App-level auth: WEBHOOK_SECRET header.
//
// Env vars (Functions → enrich-on-snapshot → Secrets):
//   GITHUB_PAT           Fine-grained PAT with Contents+Actions: write on the repo
//   GITHUB_REPO          nmjr75/harris-leads
//   WEBHOOK_SECRET       shared secret with Database Webhook (optional)
//   SUPABASE_URL         auto-injected
//   SUPABASE_SERVICE_ROLE_KEY  auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GITHUB_PAT = Deno.env.get("GITHUB_PAT") ?? "";
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") ?? "nmjr75/harris-leads";
const WEBHOOK_SECRET = Deno.env.get("WEBHOOK_SECRET") ?? "";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-webhook-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function normalizeAddress(s: string | null | undefined): string {
  return (s ?? "").replace(/[^A-Za-z0-9]+/g, "").toUpperCase();
}

function jsonResponse(status: number, body: Record<string, unknown>) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
  if (req.method !== "POST") return jsonResponse(405, { error: "method not allowed" });

  if (WEBHOOK_SECRET) {
    const provided = req.headers.get("x-webhook-secret") ?? "";
    if (provided !== WEBHOOK_SECRET) {
      console.log("DENY: invalid webhook secret");
      return jsonResponse(401, { error: "invalid webhook secret" });
    }
  }

  if (!GITHUB_PAT) {
    console.error("GITHUB_PAT not set");
    return jsonResponse(500, { error: "GITHUB_PAT not configured" });
  }

  let payload: any;
  try {
    payload = await req.json();
  } catch {
    return jsonResponse(400, { error: "invalid JSON body" });
  }

  const record = payload?.record ?? payload?.new ?? payload;
  const contactId: string | undefined = record?.contact_id;

  console.log(`RECEIVED: contact_id=${contactId}`);

  if (!contactId) {
    console.log("SKIP: no contact_id in payload");
    return jsonResponse(200, { skipped: "no contact_id" });
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Look up the address from the view, NOT from the snapshot row.
  // ghl_conversation_snapshots.contact_address is often NULL; the view
  // joins the contact's address from wherever GHL persists it.
  const { data: addrRows, error: addrErr } = await sb
    .from("ghl_latest_contact_address")
    .select("contact_address")
    .eq("contact_id", contactId)
    .limit(1);

  if (addrErr) {
    console.error(`ADDR_LOOKUP_ERR for ${contactId}: ${addrErr.message}`);
    return jsonResponse(500, { error: "address lookup failed", detail: addrErr.message });
  }

  const rawAddress = addrRows?.[0]?.contact_address;
  if (!rawAddress) {
    console.log(`SKIP: no address in view for ${contactId}`);
    return jsonResponse(200, { skipped: "no address in view", contact_id: contactId });
  }

  const norm = normalizeAddress(rawAddress);
  if (!norm) {
    console.log(`SKIP: empty normalized for ${contactId}`);
    return jsonResponse(200, { skipped: "empty normalized address", contact_id: contactId });
  }

  // Don't re-burn DataSift quota on cached addresses.
  const { data: existing, error: lookupErr } = await sb
    .from("property_enrichment")
    .select("normalized_address, source")
    .eq("normalized_address", norm)
    .limit(1);

  if (lookupErr) {
    console.error(`PE_LOOKUP_ERR for ${contactId}: ${lookupErr.message}`);
    return jsonResponse(500, { error: "enrichment lookup failed", detail: lookupErr.message });
  }

  if (existing && existing.length > 0) {
    console.log(`SKIP_ENRICHED: ${contactId} norm=${norm} source=${existing[0].source}`);
    return jsonResponse(200, {
      skipped: "already enriched",
      contact_id: contactId,
      normalized_address: norm,
      source: existing[0].source,
    });
  }

  console.log(`DISPATCH: firing GitHub for ${contactId} norm=${norm}`);

  const ghResp = await fetch(
    `https://api.github.com/repos/${GITHUB_REPO}/dispatches`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_PAT}`,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "harris-leads-enrich-trigger",
      },
      body: JSON.stringify({
        event_type: "new-contact-enrich",
        client_payload: { contact_id: contactId },
      }),
    },
  );

  if (!ghResp.ok) {
    const errText = await ghResp.text();
    console.error(`DISPATCH_FAIL: ${contactId} status=${ghResp.status} body=${errText.slice(0, 200)}`);
    return jsonResponse(502, {
      error: "github dispatch failed",
      status: ghResp.status,
      detail: errText.slice(0, 500),
    });
  }

  console.log(`DISPATCH_OK: ${contactId}`);
  return jsonResponse(200, {
    triggered: true,
    contact_id: contactId,
    normalized_address: norm,
  });
});
