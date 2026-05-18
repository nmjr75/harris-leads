// Supabase Edge Function: sync-ghl-contact-tags
//
// Mirrors a contact's GHL tag set into public.ghl_contact_tags whenever REI
// Reply fires a tag-update workflow webhook. Replaces the contact's tag rows
// atomically so REMOVALS propagate naturally — REI Reply sends the FULL
// current tag list, we delete-then-insert.
//
// Wiring in REI Reply (Settings → Workflows → new workflow):
//   1. Trigger: "Contact Tag" → fires on Added / Removed
//   2. Action: "Custom Webhook" or "Webhook"
//        URL: https://<project>.supabase.co/functions/v1/sync-ghl-contact-tags
//        Method: POST
//        Headers:
//          Content-Type:        application/json
//          Authorization:       Bearer <SUPABASE_ANON_KEY>   (gateway auth)
//          X-Webhook-Secret:    <shared secret>              (app-level auth)
//        Body (JSON):
//          {
//            "contact_id": "{{contact.id}}",
//            "tags":       "{{contact.tags}}"
//          }
//   GHL's {{contact.tags}} resolves to a comma-separated string of every
//   tag the contact currently has, which is exactly the snapshot we need.
//
// JWT verification: OFF (Settings → Verify JWT off). Gateway anon-key auth
// still applies. App-level auth comes from X-Webhook-Secret.
//
// Env vars (Functions → sync-ghl-contact-tags → Secrets):
//   WEBHOOK_SECRET             shared secret with the GHL workflow
//   SUPABASE_URL               auto-injected
//   SUPABASE_SERVICE_ROLE_KEY  auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WEBHOOK_SECRET = Deno.env.get("WEBHOOK_SECRET") ?? "";

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

// GHL ships {{contact.tags}} as a comma-separated string. Accept either that
// or an explicit JSON array — both are valid webhook body shapes if someone
// rewires the workflow later.
function parseTags(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.map((t) => String(t).trim()).filter((t) => t.length > 0);
  }
  if (typeof raw === "string") {
    return raw.split(",").map((t) => t.trim()).filter((t) => t.length > 0);
  }
  return [];
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

  let payload: any;
  try {
    payload = await req.json();
  } catch {
    return jsonResponse(400, { error: "invalid JSON body" });
  }

  const contactId: string | undefined = payload?.contact_id ?? payload?.contactId;
  const tags = parseTags(payload?.tags);

  console.log(`RECEIVED: contact_id=${contactId} tag_count=${tags.length}`);

  if (!contactId) {
    console.log("REJECT: no contact_id in payload");
    return jsonResponse(400, { error: "no contact_id in payload" });
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Atomically replace this contact's tag set. Delete current rows, insert
  // the new tag set. Empty incoming tags = full removal (correct behavior
  // when all tags get stripped in REI Reply).
  const { error: delErr } = await sb
    .from("ghl_contact_tags")
    .delete()
    .eq("contact_id", contactId);

  if (delErr) {
    console.error(`DELETE_ERR for ${contactId}: ${delErr.message}`);
    return jsonResponse(500, { error: "delete failed", detail: delErr.message });
  }

  if (tags.length === 0) {
    console.log(`CLEARED: ${contactId} (no tags in payload)`);
    return jsonResponse(200, { cleared: true, contact_id: contactId, tag_count: 0 });
  }

  const nowIso = new Date().toISOString();
  const rows = tags.map((tag) => ({
    contact_id: contactId,
    tag,
    applied_at: nowIso,
  }));

  const { error: insErr } = await sb.from("ghl_contact_tags").insert(rows);

  if (insErr) {
    console.error(`INSERT_ERR for ${contactId}: ${insErr.message}`);
    return jsonResponse(500, { error: "insert failed", detail: insErr.message });
  }

  console.log(`UPSERT_OK: ${contactId} ${tags.length} tags`);
  return jsonResponse(200, {
    ok: true,
    contact_id: contactId,
    tag_count: tags.length,
    tags,
  });
});
