// Supabase Edge Function: notify-offer-slack
//
// Triggered by a Supabase Database Webhook on INSERT and UPDATE to
// public.offers_made. Posts a formatted message to the #rei-reply-leads
// Slack channel:
//
//   - INSERT  → "📨 New offer logged by <agent> on <contact> — $<amount>"
//   - UPDATE  → only if outcome transitioned to 'accepted' (the big one),
//               then "✅ Offer ACCEPTED — <agent>, <contact>, $<amount>".
//               outcome → 'rejected' / 'counter' / 'no_response' fires a
//               quieter mention so the timeline is complete.
//
// JWT verification: OFF (Settings → Verify JWT off). Gateway anon-key auth
// applies via the Authorization header. App-level auth: GHL_WEBHOOK_SECRET
// header — reused from the existing webhook receivers since this is the
// same trust boundary.
//
// Env vars (Functions → notify-offer-slack → Secrets):
//   GHL_WEBHOOK_SECRET            shared secret with the DB webhook config
//   SLACK_WEBHOOK_NELSON_PRIVATE  Slack incoming webhook URL for #rei-reply-leads
//   DASHBOARD_BASE_URL            (optional) e.g. https://app.distressradarflow.com

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

const GHL_WEBHOOK_SECRET           = Deno.env.get("GHL_WEBHOOK_SECRET") ?? "";
const SLACK_WEBHOOK_NELSON_PRIVATE = Deno.env.get("SLACK_WEBHOOK_NELSON_PRIVATE") ?? "";
const DASHBOARD_BASE_URL           = Deno.env.get("DASHBOARD_BASE_URL") ?? "https://app.distressradarflow.com";

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

function fmtMoney(n: unknown): string {
  const num = Number(n);
  if (!Number.isFinite(num)) return "$—";
  return "$" + Math.round(num).toLocaleString("en-US");
}

function fmtAgent(email: string | null | undefined, name: string | null | undefined): string {
  const n = (name ?? "").trim();
  if (n) return n;
  if (!email) return "—";
  return email.split("@")[0];
}

async function postSlack(text: string, blocks?: unknown[]) {
  if (!SLACK_WEBHOOK_NELSON_PRIVATE) {
    console.error("SLACK_WEBHOOK_NELSON_PRIVATE not configured — skipping post");
    return;
  }
  try {
    const res = await fetch(SLACK_WEBHOOK_NELSON_PRIVATE, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(blocks ? { text, blocks } : { text }),
    });
    if (!res.ok) {
      console.error(`Slack post failed: ${res.status} ${await res.text().catch(() => "")}`);
    }
  } catch (err) {
    console.error(`Slack post error: ${(err as Error).message}`);
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
    return jsonResponse(401, { ok: false, error: "unauthorized" });
  }

  let body: any;
  try { body = await req.json(); } catch {
    return jsonResponse(400, { ok: false, error: "invalid json" });
  }

  // Supabase DB webhook payload shape:
  //   { type: "INSERT" | "UPDATE", table, schema, record, old_record }
  const type = String(body?.type ?? "").toUpperCase();
  const row  = body?.record ?? body?.new ?? null;
  const old  = body?.old_record ?? body?.old ?? null;

  if (!row) return jsonResponse(200, { ok: true, skipped: "no row" });

  const agentLabel = fmtAgent(row.agent_email, row.agent_name);
  const amount     = fmtMoney(row.amount);
  const contactUrl = row.contact_id
    ? `${DASHBOARD_BASE_URL}/admin.html?focus=${encodeURIComponent(row.contact_id)}`
    : null;

  if (type === "INSERT") {
    const text = `📨 *New offer logged* — ${agentLabel} offered ${amount}`;
    const blocks: any[] = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text:
            `📨 *New offer logged*\n` +
            `*Agent:* ${agentLabel}\n` +
            `*Amount:* ${amount}\n` +
            (row.notes ? `*Notes:* ${String(row.notes).slice(0, 300)}\n` : "") +
            (contactUrl ? `<${contactUrl}|Open contact in dashboard ↗>` : ""),
        },
      },
    ];
    await postSlack(text, blocks);
    return jsonResponse(200, { ok: true, posted: "insert" });
  }

  if (type === "UPDATE") {
    const oldOutcome = old?.outcome ?? "pending";
    const newOutcome = row?.outcome ?? "pending";
    if (oldOutcome === newOutcome) {
      return jsonResponse(200, { ok: true, skipped: "outcome unchanged" });
    }

    let emoji = "•";
    let label = `outcome changed to *${newOutcome}*`;
    if (newOutcome === "accepted") { emoji = "✅"; label = `*OFFER ACCEPTED*`; }
    else if (newOutcome === "rejected") { emoji = "❌"; label = "offer rejected"; }
    else if (newOutcome === "counter")  { emoji = "🔁"; label = `homeowner countered at ${fmtMoney(row.counter_amount)}`; }
    else if (newOutcome === "no_response") { emoji = "🌫"; label = "no response — closing the loop"; }

    const text = `${emoji} ${agentLabel} ${label} (${amount})`;
    const blocks: any[] = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text:
            `${emoji} ${label}\n` +
            `*Agent:* ${agentLabel}\n` +
            `*Original amount:* ${amount}\n` +
            (newOutcome === "counter" && row.counter_amount
              ? `*Counter:* ${fmtMoney(row.counter_amount)}\n`
              : "") +
            (row.notes ? `*Notes:* ${String(row.notes).slice(0, 300)}\n` : "") +
            (contactUrl ? `<${contactUrl}|Open contact in dashboard ↗>` : ""),
        },
      },
    ];
    await postSlack(text, blocks);
    return jsonResponse(200, { ok: true, posted: "update" });
  }

  return jsonResponse(200, { ok: true, skipped: `unhandled type ${type}` });
});
