// Supabase Edge Function: force-enrich-contact
//
// One-click re-enrichment for the dashboard. Triggered by any
// authenticated user clicking the "Re-enrich" button on a lead row.
// Does the full flow server-side so VAs never touch SQL or curl:
//
//   1. Verify caller's session (no anon access)
//   2. Pull the latest contact data from GHL API (REI Reply) — picks
//      up address corrections that haven't propagated to Supabase yet
//   3. Update ghl_conversation_snapshots so the view reflects the
//      fresh data immediately
//   4. Delete any existing property_enrichment row for that address
//      (including cached misses) so the worker can't skip
//   5. Fire repository_dispatch on GitHub to run the on-demand enricher
//
// JWT verification: should be ON for this function (Settings → Verify
// JWT on). We want only authenticated dashboard users to trigger this.
//
// Env vars (Functions → force-enrich-contact → Secrets):
//   GHL_API_TOKEN        Private Integration Token for REI Reply
//   GITHUB_PAT           fine-grained PAT with Contents+Actions: write
//   GITHUB_REPO          nmjr75/harris-leads (default)
//   SUPABASE_URL         auto-injected
//   SUPABASE_SERVICE_ROLE_KEY  auto-injected
//   SUPABASE_ANON_KEY    auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY") ?? "";
const GHL_API_TOKEN = Deno.env.get("GHL_API_TOKEN") ?? "";
const GITHUB_PAT = Deno.env.get("GITHUB_PAT") ?? "";
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") ?? "nmjr75/harris-leads";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
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

  // 1. Verify caller has a real session.
  const authHeader = req.headers.get("Authorization") ?? "";
  if (!authHeader.startsWith("Bearer ")) {
    return jsonResponse(401, { error: "missing Authorization header" });
  }
  const userSb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    global: { headers: { Authorization: authHeader } },
  });
  const { data: { user }, error: authErr } = await userSb.auth.getUser();
  if (authErr || !user) {
    return jsonResponse(401, { error: "invalid session", detail: authErr?.message });
  }

  if (!GHL_API_TOKEN) return jsonResponse(500, { error: "GHL_API_TOKEN not configured" });
  if (!GITHUB_PAT) return jsonResponse(500, { error: "GITHUB_PAT not configured" });

  let payload: any;
  try {
    payload = await req.json();
  } catch {
    return jsonResponse(400, { error: "invalid JSON body" });
  }
  const contactId: string = payload?.contact_id;
  if (!contactId) return jsonResponse(400, { error: "contact_id required" });

  console.log(`FORCE_ENRICH: user=${user.email} contact_id=${contactId}`);

  const adminSb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // 2. Pull fresh contact data from GHL API. If this fails (network,
  // token expired, contact deleted), we fall back to whatever is
  // currently in ghl_latest_contact_address — the enrichment may use
  // stale data but the trigger still fires.
  let freshAddress: string | null = null;
  let freshCity: string | null = null;
  let freshState: string | null = null;
  let freshPostal: string | null = null;
  let ghlFetchOk = false;

  try {
    const ghlResp = await fetch(
      `https://services.leadconnectorhq.com/contacts/${contactId}`,
      {
        headers: {
          "Authorization": `Bearer ${GHL_API_TOKEN}`,
          "Version": "2021-07-28",
          "Accept": "application/json",
        },
      },
    );
    if (ghlResp.ok) {
      const ghlData = await ghlResp.json();
      const c = ghlData?.contact ?? ghlData;
      freshAddress = c?.address1 ?? c?.address ?? null;
      freshCity = c?.city ?? null;
      freshState = c?.state ?? null;
      freshPostal = c?.postalCode ?? c?.postal_code ?? null;
      ghlFetchOk = true;
      console.log(`GHL_FETCH_OK: address="${freshAddress}" city="${freshCity}" state="${freshState}"`);

      // 3. Push fresh address into ghl_conversation_snapshots so the
      // view reflects the corrected data RIGHT NOW. Update ALL snapshot
      // rows for this contact_id (multiple may exist over time).
      if (freshAddress) {
        const { error: upErr } = await adminSb
          .from("ghl_conversation_snapshots")
          .update({
            contact_address: freshAddress,
            contact_city: freshCity,
            contact_state: freshState,
            contact_postal_code: freshPostal,
          })
          .eq("contact_id", contactId);
        if (upErr) console.error(`SNAPSHOT_UPDATE_ERR: ${upErr.message}`);
      }
    } else {
      console.error(`GHL_FETCH_FAIL: status=${ghlResp.status} body=${(await ghlResp.text()).slice(0, 200)}`);
    }
  } catch (e) {
    console.error(`GHL_FETCH_EXCEPTION: ${e}`);
  }

  // 4. Read whatever address is now in the view (fresh if GHL succeeded,
  // stale if it failed) so we know which normalized key to wipe.
  const { data: addrRows } = await adminSb
    .from("ghl_latest_contact_address")
    .select("contact_address")
    .eq("contact_id", contactId)
    .limit(1);
  const address = addrRows?.[0]?.contact_address || freshAddress || null;
  if (!address) {
    return jsonResponse(404, { error: "no address found for contact", contact_id: contactId });
  }
  const norm = normalizeAddress(address);

  // 5. Wipe any cached property_enrichment row (including
  // siftmap_address_mismatch / no_panel) so the worker can't skip.
  const { error: delErr } = await adminSb
    .from("property_enrichment")
    .delete()
    .eq("normalized_address", norm);
  if (delErr) {
    console.error(`PE_DELETE_ERR: ${delErr.message}`);
    return jsonResponse(500, { error: "failed to clear cached enrichment", detail: delErr.message });
  }

  // 6. Trigger GitHub workflow.
  const ghResp = await fetch(
    `https://api.github.com/repos/${GITHUB_REPO}/dispatches`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_PAT}`,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "force-enrich-contact",
      },
      body: JSON.stringify({
        event_type: "new-contact-enrich",
        client_payload: { contact_id: contactId },
      }),
    },
  );
  if (!ghResp.ok) {
    const errText = await ghResp.text();
    console.error(`DISPATCH_FAIL: status=${ghResp.status} body=${errText.slice(0, 200)}`);
    return jsonResponse(502, {
      error: "github dispatch failed",
      status: ghResp.status,
      detail: errText.slice(0, 500),
    });
  }

  console.log(`DISPATCH_OK: ${contactId} address="${address}" ghl_fetch=${ghlFetchOk}`);

  return jsonResponse(200, {
    success: true,
    contact_id: contactId,
    address,
    normalized_address: norm,
    ghl_fetch_ok: ghlFetchOk,
    note: "Enrichment workflow triggered. Refresh the dashboard in ~3-5 minutes.",
  });
});
