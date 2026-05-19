// Supabase Edge Function: sync-ghl-call
//
// Receives the "Sync Calls to Lead Audit Dashboard" workflow webhook from
// REI Reply (GHL Call Details trigger). Writes one row into public.ghl_calls
// per call. Idempotent — re-fires upsert into the same row via a
// deterministic external_id derived from started_at + numbers + agent.
//
// JWT verification: OFF (Functions → sync-ghl-call → Settings →
// "Verify JWT" off). The Supabase gateway still requires
// Authorization: Bearer <anon/publishable key> on every request — that
// header is set in the GHL workflow alongside X-Webhook-Secret. App-level
// auth here is GHL_WEBHOOK_SECRET.
//
// Env vars (Functions → secrets):
//   GHL_WEBHOOK_SECRET           shared secret with workflow
//   GHL_API_TOKEN                v3 PIT for /users/{id} agent-email lookup
//   SUPABASE_URL                 auto-injected
//   SUPABASE_SERVICE_ROLE_KEY    auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GHL_WEBHOOK_SECRET = Deno.env.get("GHL_WEBHOOK_SECRET") ?? "";
const GHL_API_TOKEN = Deno.env.get("GHL_API_TOKEN") ?? "";

// Some agents log into REI Reply with a different email than their canonical
// dashboard identity. Without normalization, their per-VA cards show 0 calls
// because the dashboard looks up by the canonical email but rows are keyed
// by the REI Reply login email returned from /users/{id}.
const AGENT_EMAIL_ALIASES: Record<string, string> = {
  "tcdept.gamc@gmail.com": "mark.realdealsource@gmail.com",  // Walter
};

function normalizeAgentEmail(email: string | null): string | null {
  if (!email) return null;
  return AGENT_EMAIL_ALIASES[email.toLowerCase()] ?? email;
}

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-webhook-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

// GHL emits naive timestamps in Central Time. Deno would otherwise parse
// them as UTC and shift call times by 5-6 hours. This walks the wall-clock
// string back to true UTC, DST-aware via Intl.
function naiveCentralTimeToUTC(s: string | null | undefined): string | null {
  if (!s) return null;
  const t = String(s).replace(" ", "T");
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?/.exec(t);
  if (!m) return null;
  const [, Y, Mo, D, H, Mi, S] = m;
  const asIfUTC = new Date(Date.UTC(+Y, +Mo - 1, +D, +H, +Mi, +(S ?? 0)));
  const ctParts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  }).formatToParts(asIfUTC);
  const get = (k: string) => +(ctParts.find(p => p.type === k)?.value ?? 0);
  let ctH = get("hour"); if (ctH === 24) ctH = 0;
  const ctMs = Date.UTC(
    get("year"), get("month") - 1, get("day"),
    ctH, get("minute"), get("second"),
  );
  const offsetMs = asIfUTC.getTime() - ctMs;
  return new Date(asIfUTC.getTime() + offsetMs).toISOString();
}

async function sha256Hex(input: string): Promise<string> {
  const buf = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0")).join("");
}

// Fetch the GHL agent's email + display name. Call Details trigger exposes
// user.id + user.name but not user.email; this fills the gap.
async function fetchAgent(
  userId: string | null,
): Promise<{ email: string | null; name: string | null }> {
  if (!userId || !GHL_API_TOKEN) return { email: null, name: null };
  try {
    const res = await fetch(
      `https://services.leadconnectorhq.com/users/${userId}`,
      {
        headers: {
          Authorization: `Bearer ${GHL_API_TOKEN}`,
          Version: "2021-07-28",
          Accept: "application/json",
        },
      },
    );
    if (!res.ok) return { email: null, name: null };
    const u = await res.json();
    const composed = `${u?.firstName ?? ""} ${u?.lastName ?? ""}`.trim();
    const name = u?.name ?? (composed || null);
    return { email: u?.email ?? null, name };
  } catch {
    return { email: null, name: null };
  }
}

serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405, headers: CORS });
  }

  // App-level auth.
  const incoming = req.headers.get("x-webhook-secret") ??
    req.headers.get("X-Webhook-Secret");
  if (!GHL_WEBHOOK_SECRET || incoming !== GHL_WEBHOOK_SECRET) {
    return new Response(
      JSON.stringify({ ok: false, error: "unauthorized" }),
      { status: 401, headers: { ...CORS, "Content-Type": "application/json" } },
    );
  }

  let body: any;
  try { body = await req.json(); } catch {
    return new Response(
      JSON.stringify({ ok: false, error: "invalid json" }),
      { status: 400, headers: { ...CORS, "Content-Type": "application/json" } },
    );
  }

  // Field names match the workflow body keys (camelCase).
  const contactId       = (body?.contactId       ?? "").toString().trim() || null;
  const conversationId  = (body?.conversationId  ?? "").toString().trim() || null;
  const direction       = (body?.direction       ?? "").toString().toLowerCase().trim() || null;
  const status          = (body?.status          ?? "").toString().toLowerCase().trim() || null;
  const fromNumber      = (body?.from            ?? "").toString().trim() || null;
  const toNumber        = (body?.to              ?? "").toString().trim() || null;
  const userId          = (body?.userId          ?? "").toString().trim() || null;
  const userName        = (body?.userName        ?? "").toString().trim() || null;
  const startedAtRaw    = (body?.startedAt       ?? "").toString().trim();
  const endedAtRaw      = (body?.endedAt         ?? "").toString().trim();
  const contactFullName = (body?.contactFullName ?? "").toString().trim() || null;

  const durRaw = body?.durationSeconds;
  const durationSec = (durRaw === null || durRaw === undefined || durRaw === "")
    ? null
    : Number(durRaw);

  const startedAt = naiveCentralTimeToUTC(startedAtRaw);
  const endedAt   = naiveCentralTimeToUTC(endedAtRaw);

  if (!startedAt) {
    return new Response(
      JSON.stringify({ ok: false, error: "missing or unparseable startedAt" }),
      { status: 400, headers: { ...CORS, "Content-Type": "application/json" } },
    );
  }

  // Stable dedup key — re-fires of the same workflow on the same call
  // hash to the same external_id and upsert into the same row.
  const externalId = await sha256Hex(
    [startedAt, fromNumber ?? "", toNumber ?? "", userId ?? ""].join("|"),
  );

  const agent = await fetchAgent(userId);

  const row = {
    external_id:      externalId,
    contact_id:       contactId,
    conversation_id:  conversationId,
    direction,
    status,
    duration_seconds: Number.isFinite(durationSec) ? durationSec : null,
    from_number:      fromNumber,
    to_number:        toNumber,
    agent_user_id:    userId,
    agent_email:      normalizeAgentEmail(agent.email),
    agent_name:       agent.name ?? userName,
    started_at:       startedAt,
    ended_at:         endedAt,
    contact_name:     contactFullName,
    last_synced_at:   new Date().toISOString(),
  };

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const { error } = await sb
    .from("ghl_calls")
    .upsert(row, { onConflict: "external_id", ignoreDuplicates: false });

  if (error) {
    console.error("ghl_calls upsert failed:", error);
    return new Response(
      JSON.stringify({ ok: false, error: error.message }),
      { status: 500, headers: { ...CORS, "Content-Type": "application/json" } },
    );
  }

  return new Response(
    JSON.stringify({ ok: true, external_id: externalId }),
    { status: 200, headers: { ...CORS, "Content-Type": "application/json" } },
  );
});
