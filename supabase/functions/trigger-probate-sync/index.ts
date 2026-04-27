// Supabase Edge Function: trigger-probate-sync
//
// Fires the probate-sync.yml GitHub workflow on demand from the dashboard.
// VAs never see GitHub — they hit a button, this function calls the GitHub
// API server-side using a PAT that lives in Supabase secrets.
//
// Security model:
//   1. Caller MUST be authenticated and have role 'admin' or 'editor'.
//      We verify by re-querying public.profiles with the caller's JWT —
//      anonymous or viewer-role calls are rejected.
//   2. The PAT is stored as a Supabase secret (GITHUB_DISPATCH_PAT) and
//      never sent to the browser. Use a fine-grained PAT scoped to:
//          Repository:   nmjr75/harris-leads (only this repo)
//          Permissions:  Actions: Read and write (only)
//      That scope can ONLY trigger workflows in this one repo. It cannot
//      read code, push commits, modify settings, or touch other repos.
//   3. Per-user rate limit: 1 trigger per 60 seconds. Stored in-memory
//      per Edge Function isolate — best-effort, not bulletproof, but
//      combined with GitHub's own concurrency lock on the workflow it's
//      sufficient. (The workflow has cancel-in-progress: false, so even
//      a flood of triggers serializes into one queue.)
//
// Returns:
//   200 { ok: true, run_url, message }       — workflow dispatched
//   401 { ok: false, error }                  — not authenticated
//   403 { ok: false, error }                  — wrong role
//   429 { ok: false, error }                  — rate limited
//   500 { ok: false, error }                  — GitHub API failure
//
// Deploy:
//   supabase functions deploy trigger-probate-sync
//   supabase secrets set GITHUB_DISPATCH_PAT=<your_PAT>
//   supabase secrets set GITHUB_REPO=nmjr75/harris-leads
//   supabase secrets set GITHUB_WORKFLOW_FILE=probate-sync.yml

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GITHUB_DISPATCH_PAT = Deno.env.get("GITHUB_DISPATCH_PAT")!;
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") ?? "nmjr75/harris-leads";
const GITHUB_WORKFLOW_FILE = Deno.env.get("GITHUB_WORKFLOW_FILE") ?? "probate-sync.yml";

const RATE_LIMIT_WINDOW_MS = 60_000; // 1 trigger / 60s per user
const lastTriggerByUser = new Map<string, number>();

// CORS — dashboard hits this from nmjr75.github.io
// IMPORTANT: x-client-info MUST be in Allow-Headers. The supabase-js v2
// library sends this header (for telemetry) on every fetch. If it's not in
// the preflight Allow-Headers list, the browser blocks the request with
// "Request header field x-client-info is not allowed by Access-Control-
// Allow-Headers in preflight response." This bit us 2026-04-27.
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }
  if (req.method !== "POST") {
    return jsonResponse({ ok: false, error: "Method not allowed" }, 405);
  }

  // ── Verify caller authentication ────────────────────────────────────────
  const authHeader = req.headers.get("Authorization") ?? "";
  if (!authHeader.startsWith("Bearer ")) {
    return jsonResponse({ ok: false, error: "Missing Authorization header" }, 401);
  }
  const jwt = authHeader.slice("Bearer ".length);

  // Use the caller's JWT to identify them (NOT the service role)
  const userClient = createClient(SUPABASE_URL, jwt, {
    auth: { persistSession: false, autoRefreshToken: false },
    global: { headers: { Authorization: authHeader } },
  });
  const { data: userData, error: userErr } = await userClient.auth.getUser();
  if (userErr || !userData?.user) {
    return jsonResponse({ ok: false, error: "Invalid or expired session" }, 401);
  }
  const userId = userData.user.id;
  const userEmail = userData.user.email ?? "(no email)";

  // ── Verify role (admin or editor) ───────────────────────────────────────
  // Use service role to read profiles (bypasses RLS) — we already verified
  // the caller's identity above, so this lookup is safe.
  const adminClient = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const { data: profile, error: profileErr } = await adminClient
    .from("profiles")
    .select("role")
    .eq("id", userId)
    .single();
  if (profileErr || !profile) {
    return jsonResponse(
      { ok: false, error: "Could not load user profile" },
      403,
    );
  }
  if (!["admin", "editor"].includes(profile.role)) {
    return jsonResponse(
      { ok: false, error: `Role '${profile.role}' is not allowed to trigger sync` },
      403,
    );
  }

  // ── Per-user rate limit ─────────────────────────────────────────────────
  const now = Date.now();
  const last = lastTriggerByUser.get(userId) ?? 0;
  if (now - last < RATE_LIMIT_WINDOW_MS) {
    const waitSec = Math.ceil((RATE_LIMIT_WINDOW_MS - (now - last)) / 1000);
    return jsonResponse(
      { ok: false, error: `Rate limited — try again in ${waitSec}s` },
      429,
    );
  }
  lastTriggerByUser.set(userId, now);

  // ── Optional body inputs (dry_run, tab_filter) ──────────────────────────
  let body: { dry_run?: boolean; tab_filter?: string } = {};
  try {
    if (req.headers.get("content-length") !== "0") {
      body = await req.json();
    }
  } catch (_) {
    // empty / invalid body OK — defaults apply
  }

  // ── Call GitHub workflow_dispatch API ───────────────────────────────────
  const dispatchUrl =
    `https://api.github.com/repos/${GITHUB_REPO}/actions/workflows/${GITHUB_WORKFLOW_FILE}/dispatches`;

  const ghResp = await fetch(dispatchUrl, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${GITHUB_DISPATCH_PAT}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
      "User-Agent": "harris-leads-dashboard",
    },
    body: JSON.stringify({
      ref: "main",
      inputs: {
        dry_run: body.dry_run ? "true" : "false",
        tab_filter: body.tab_filter ?? "",
      },
    }),
  });

  if (!ghResp.ok) {
    const text = await ghResp.text();
    console.error(`GitHub dispatch failed (${ghResp.status}): ${text}`);
    return jsonResponse(
      {
        ok: false,
        error: `GitHub API returned ${ghResp.status}`,
        detail: text.slice(0, 500),
      },
      500,
    );
  }

  // GitHub workflow_dispatch returns 204 No Content on success — no run_id
  // in the response. Construct the actions page URL so the user can click
  // through to see the run.
  const actionsUrl = `https://github.com/${GITHUB_REPO}/actions/workflows/${GITHUB_WORKFLOW_FILE}`;

  console.log(`Sync triggered by ${userEmail} (${profile.role})`);
  return jsonResponse({
    ok: true,
    message: "Sync started — typically completes within 30-60 seconds",
    run_url: actionsUrl,
    triggered_by: userEmail,
  });
});
