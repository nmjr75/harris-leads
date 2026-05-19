// Supabase Edge Function: sync-ghl-contact-tags
//
// Mirrors a contact's GHL/REI Reply tag set into public.ghl_contact_tags.
// REI Reply's {{contact.tags}} merge field is unreliable — it intermittently
// resolves to the string "null" instead of the comma-separated tag list. So
// this function takes the contact_id from the webhook and goes BACK to the
// GHL API to fetch the authoritative tag list, then replaces the rows
// atomically (delete + insert) so tag removals propagate without a separate
// event type.
//
// Wiring in REI Reply (Automation → new workflow):
//   1. Trigger: "Contact Tag" → fires on Added / Removed
//   2. Action: "Custom Webhook"
//        URL: https://<project>.supabase.co/functions/v1/sync-ghl-contact-tags
//        Method: POST
//        Headers:
//          Content-Type:        application/json
//          Authorization:       Bearer <SUPABASE_ANON_KEY>   (gateway auth)
//          X-Webhook-Secret:    <shared secret>              (app-level auth)
//        Body (JSON):
//          { "contact_id": "{{contact.id}}" }
//   Only contact_id is needed — we fetch tags from the GHL API ourselves.
//
// JWT verification: OFF. Gateway anon-key auth still applies. App-level auth
// comes from X-Webhook-Secret.
//
// Env vars (project-level secrets):
//   TAGS_WEBHOOK_SECRET        shared secret with the GHL workflow
//   GHL_API_TOKEN              REI Reply Private Integration Token — same
//                              secret used by force-enrich-contact
//   SUPABASE_URL               auto-injected
//   SUPABASE_SERVICE_ROLE_KEY  auto-injected

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WEBHOOK_SECRET = Deno.env.get("TAGS_WEBHOOK_SECRET") ?? "";
const GHL_API_TOKEN = Deno.env.get("GHL_API_TOKEN") ?? "";

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

// Accept either a JSON array of tags or a comma-separated string. Used as a
// fallback when the webhook body happens to carry the tag list directly; the
// primary source is now the GHL API fetch below.
function parseTagsFromPayload(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.map((t) => String(t).trim()).filter((t) => t.length > 0 && t.toLowerCase() !== "null");
  }
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (!trimmed || trimmed.toLowerCase() === "null") return [];
    return trimmed.split(",").map((t) => t.trim()).filter((t) => t.length > 0 && t.toLowerCase() !== "null");
  }
  return [];
}

async function fetchTagsFromGHL(contactId: string): Promise<string[]> {
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
    throw new Error(`GHL API status=${resp.status} body=${body.slice(0, 200)}`);
  }
  const data = await resp.json();
  const c = data?.contact ?? data;
  const rawTags = c?.tags;
  if (Array.isArray(rawTags)) {
    return rawTags
      .map((t) => String(t).trim())
      .filter((t) => t.length > 0 && t.toLowerCase() !== "null");
  }
  return [];
}

type AppointmentInfo = {
  at: string;
  title: string | null;
  status: string | null;
  raw: any;
} | null;

// GHL's Calendars/Appointments API returns appointment times with a `Z` (or
// +0000) suffix that *claims* UTC, but the actual hour/minute represent the
// calendar's local timezone — Central for Houston ops. JavaScript's Date
// constructor honors the Z and parses as UTC, producing the 5-hour shift bug
// (10 AM CT becomes 5 AM CT on the dashboard).
//
// Fix mirrors the pattern from sync-ghl-call (SiftStack commit 17debe9): pull
// out the wall-clock parts via regex regardless of any Z/offset suffix, treat
// them as America/Chicago, then compute the equivalent UTC instant using the
// Intl API (handles DST automatically).
function ghlAppointmentTimeToUtcIso(rawStr: unknown): string | null {
  if (!rawStr) return null;
  const s = String(rawStr).trim();
  if (!s) return null;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?/);
  if (!m) {
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }
  const [, year, month, day, hour, minute, second = "0"] = m;
  // Step 1: treat the wall-clock parts AS IF they were UTC (first guess).
  const guessUtcMs = Date.UTC(+year, +month - 1, +day, +hour, +minute, +second);
  // Step 2: render that UTC instant in Central; the components we get back
  // tell us what time it "would be" if our guess were correct. The delta
  // between the original wall-clock and that rendering is the CT offset
  // (handles CDT vs CST automatically via the Intl API).
  const ctParts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  }).formatToParts(new Date(guessUtcMs));
  const get = (t: string): number => {
    const raw = ctParts.find((p) => p.type === t)?.value || "0";
    return parseInt(raw === "24" ? "0" : raw, 10);
  };
  const ctEpochMs = Date.UTC(
    get("year"), get("month") - 1, get("day"),
    get("hour"), get("minute"), get("second"),
  );
  const offsetMs = guessUtcMs - ctEpochMs;
  return new Date(guessUtcMs + offsetMs).toISOString();
}

// Fetches the contact's appointments and picks the most actionable one:
// the soonest upcoming event if any are in the future, otherwise the most
// recent past event. Returns null if the contact has no appointments OR if
// the API call fails (we don't want to block tag sync on a calendar issue).
async function fetchUpcomingAppointment(contactId: string): Promise<AppointmentInfo> {
  try {
    const resp = await fetch(
      `https://services.leadconnectorhq.com/contacts/${contactId}/appointments`,
      {
        headers: {
          "Authorization": `Bearer ${GHL_API_TOKEN}`,
          "Version": "2021-04-15",
          "Accept": "application/json",
        },
      },
    );
    if (!resp.ok) {
      const body = await resp.text();
      console.warn(`APPT_FETCH_FAIL contact=${contactId} status=${resp.status} body=${body.slice(0, 150)}`);
      return null;
    }
    const data = await resp.json();
    // GHL responses vary by API version: events / appointments / array.
    const events: any[] = Array.isArray(data) ? data : (data?.events ?? data?.appointments ?? []);
    if (!Array.isArray(events) || events.length === 0) {
      console.log(`APPT_NONE contact=${contactId}`);
      return null;
    }
    const now = Date.now();
    const enriched = events
      .map((e) => {
        const rawStart = e.startTime ?? e.start_time ?? e.startsAt;
        const utcIso = ghlAppointmentTimeToUtcIso(rawStart);
        // Capture both the raw GHL field and our normalized value so future
        // debugging can verify whether GHL ever changes its return format.
        return {
          ...e,
          _rawStart: rawStart,
          _utcIso: utcIso,
          _startMs: utcIso ? new Date(utcIso).getTime() : 0,
        };
      })
      .filter((e) => !!e._startMs);
    if (enriched.length === 0) return null;
    enriched.sort((a, b) => a._startMs - b._startMs);
    const upcoming = enriched.find((e) => e._startMs >= now);
    const chosen = upcoming ?? enriched[enriched.length - 1];
    console.log(
      `APPT_PARSE contact=${contactId} raw="${chosen._rawStart}" → utc=${chosen._utcIso}`,
    );
    return {
      at: chosen._utcIso!,
      title: chosen.title ?? null,
      status: chosen.appointmentStatus ?? chosen.status ?? null,
      raw: chosen,
    };
  } catch (e) {
    console.warn(`APPT_FETCH_ERR contact=${contactId} ${(e as Error).message}`);
    return null;
  }
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
  if (!contactId) {
    console.log("REJECT: no contact_id in payload");
    return jsonResponse(400, { error: "no contact_id in payload" });
  }

  // Try the payload first (cheap), fall back to GHL API. If the payload tags
  // looked like the broken "null" merge field, treat the payload as empty and
  // go to the API.
  let tags = parseTagsFromPayload(payload?.tags);
  let source: "payload" | "ghl_api" = "payload";

  if (tags.length === 0) {
    if (!GHL_API_TOKEN) {
      console.error("GHL_API_TOKEN not set — cannot fetch authoritative tags");
      return jsonResponse(500, { error: "GHL_API_TOKEN not configured" });
    }
    try {
      tags = await fetchTagsFromGHL(contactId);
      source = "ghl_api";
    } catch (e) {
      console.error(`GHL_FETCH_ERR for ${contactId}: ${(e as Error).message}`);
      return jsonResponse(502, { error: "GHL API fetch failed", detail: (e as Error).message });
    }
  }

  console.log(`RECEIVED: contact_id=${contactId} tag_count=${tags.length} source=${source}`);

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
    console.log(`CLEARED: ${contactId} (no tags from any source)`);
    return jsonResponse(200, { cleared: true, contact_id: contactId, tag_count: 0, source });
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

  // Fetch + persist the upcoming appointment (if any). Don't block the
  // response on this; if it fails the tag chip still renders, it just won't
  // show a scheduled time.
  let appointmentAt: string | null = null;
  const appt = await fetchUpcomingAppointment(contactId);
  if (appt && appt.at) {
    const { error: apptErr } = await sb.from("ghl_contact_appointments").upsert({
      contact_id: contactId,
      appointment_at: appt.at,
      appointment_title: appt.title,
      appointment_status: appt.status,
      raw: appt.raw,
      updated_at: nowIso,
    }, { onConflict: "contact_id" });
    if (apptErr) {
      console.error(`APPT_UPSERT_ERR contact=${contactId} ${apptErr.message}`);
    } else {
      appointmentAt = appt.at;
      console.log(`APPT_UPSERT_OK contact=${contactId} at=${appt.at}`);
    }
  }

  console.log(`UPSERT_OK: ${contactId} ${tags.length} tags appt=${appointmentAt ?? "(none)"} (source=${source})`);
  return jsonResponse(200, {
    ok: true,
    contact_id: contactId,
    tag_count: tags.length,
    tags,
    appointment_at: appointmentAt,
    source,
  });
});
