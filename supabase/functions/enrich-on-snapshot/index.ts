// Supabase Edge Function: enrich-on-snapshot
//
// Receives Database Webhook events from ghl_conversation_snapshots INSERTs
// (fired ~30/hour as the scorer worker pulls conversations). Filters to
// just contacts whose address isn't yet in property_enrichment, then
// triggers the SiftMap on-demand workflow on GitHub via repository_dispatch.
//
// Why this filter exists: snapshots fire on every refresh (~6h per active
// convo), not just new contacts. Without this filter we'd burn ~720
// GitHub Actions cold-starts per day on contacts we've already enriched.
// With this filter we only fire the workflow when there's real work.
//
// JWT verification: should be OFF for this function (Functions →
// enrich-on-snapshot → Settings → "Verify JWT" off). The Supabase gateway
// still requires Authorization: Bearer <anon key> on the call — Database
// Webhooks add that header automatically when "Use Supabase auth" is on.
// App-level auth here is WEBHOOK_SECRET (extra defense vs. anon key leaks).
//
// Env vars (Functions → enrich-on-snapshot → secrets):
//   GITHUB_PAT           Personal access token with Actions: write on
//                         nmjr75/harris-leads (fine-grained PAT preferred)
//   GITHUB_REPO          nmjr75/harris-leads (or override for forks)
//   WEBHOOK_SECRET       shared secret with Database Webhook (optional;
//                         skip if not set)
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
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }
  if (req.method !== "POST") {
    return jsonResponse(405, { error: "method not allowed" });
  }

  // Optional shared-secret defense beyond the anon key.
  if (WEBHOOK_SECRET) {
    const provided = req.headers.get("x-webhook-secret") ?? "";
    if (provided !== WEBHOOK_SECRET) {
      return jsonResponse(401, { error: "invalid webhook secret" });
    }
  }

  if (!GITHUB_PAT) {
    console.error("GITHUB_PAT not set — refusing to dispatch");
    return jsonResponse(500, { error: "GITHUB_PAT not configured" });
  }

  let payload: any;
  try {
    payload = await req.json();
  } catch {
    return jsonResponse(400, { error: "invalid JSON body" });
  }

  // Supabase webhook shape: { type: 'INSERT', table, record, old_record }
  const record = payload?.record ?? payload?.new ?? payload;
  const contactId: string | undefined = record?.contact_id;
  const rawAddress: string | undefined = record?.contact_address;

  if (!contactId) {
    return jsonResponse(200, { skipped: "no contact_id in payload" });
  }
  if (!rawAddress) {
    return jsonResponse(200, { skipped: "no contact_address in payload", contact_id: contactId });
  }

  const norm = normalizeAddress(rawAddress);
  if (!norm) {
    return jsonResponse(200, { skipped: "empty normalized address", contact_id: contactId });
  }

  // Skip if already enriched. This is the key filter — without it every
  // snapshot refresh would trigger a workflow run for already-cached
  // addresses (most of them, since snapshots fire ~30x/hour).
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const { data: existing, error: lookupErr } = await sb
    .from("property_enrichment")
    .select("normalized_address, source")
    .eq("normalized_address", norm)
    .limit(1);

  if (lookupErr) {
    console.error("property_enrichment lookup failed:", lookupErr);
    return jsonResponse(500, { error: "lookup failed", detail: lookupErr.message });
  }

  if (existing && existing.length > 0) {
    return jsonResponse(200, {
      skipped: "already enriched",
      contact_id: contactId,
      normalized_address: norm,
      source: existing[0].source,
    });
  }

  // Fire repository_dispatch on GitHub to trigger the on-demand workflow.
  const ghBody = {
    event_type: "new-contact-enrich",
    client_payload: { contact_id: contactId },
  };
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
      body: JSON.stringify(ghBody),
    },
  );

  if (!ghResp.ok) {
    const errText = await ghResp.text();
    console.error(`GitHub dispatch failed: ${ghResp.status} ${errText}`);
    return jsonResponse(502, {
      error: "github dispatch failed",
      status: ghResp.status,
      detail: errText.slice(0, 500),
    });
  }

  return jsonResponse(200, {
    triggered: true,
    contact_id: contactId,
    normalized_address: norm,
  });
});
