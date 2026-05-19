-- 042b_pipeline_stage_restructure.sql
-- Restructures the acquisitions pipeline stage taxonomy to match Nelson's
-- actual workflow. The new shape captures the real funnel — including the
-- "called but not contacted yet" intermediate state (Discovery), specific
-- reasons leads die (Not Interested / Wrong # / Signed Elsewhere replacing
-- the vague "Lost"), and the renames that match how the team thinks about
-- the process (Untouched, Interested, Contracts Signed).
--
-- Rename map:
--   active        → untouched         (no calls made yet — fresh inbox)
--   working       → interested        (homeowner is engaged, talking)
--   won           → contracts_signed  (deal closed under contract)
--   lost          → not_interested    (safe lump for migration; new leads
--                                      can pick a more specific terminal
--                                      stage via the dashboard menu)
--
-- New stages (no rename, just added):
--   discovery, offer_needed, offer_rejected, wrong_number, signed_elsewhere
--
-- Stages staying as-is:
--   run_comps, offer_made, ghosting, dnc, appointment_set, hit_list
--
-- Plus: adds ghl_contact_lifecycle.disposition_note (text, nullable) so the
-- dashboard can capture a one-line "why" when a VA moves a lead to one of
-- the terminal-reason stages (Not Interested / Wrong # / Signed Elsewhere).

-- ─── Step 1: data backfill BEFORE swapping the check constraint ────────
-- (must happen first so the new constraint doesn't reject existing rows)

update public.ghl_contact_lifecycle set stage = 'untouched'        where stage = 'active';
update public.ghl_contact_lifecycle set stage = 'interested'       where stage = 'working';
update public.ghl_contact_lifecycle set stage = 'contracts_signed' where stage = 'won';
update public.ghl_contact_lifecycle set stage = 'not_interested'   where stage = 'lost';

-- ─── Step 2: swap the check constraint to the new value list ───────────

alter table public.ghl_contact_lifecycle
  drop constraint if exists ghl_contact_lifecycle_stage_check;

alter table public.ghl_contact_lifecycle
  add constraint ghl_contact_lifecycle_stage_check
  check (stage = any (array[
    'untouched'::text,
    'discovery'::text,
    'interested'::text,
    'run_comps'::text,
    'offer_needed'::text,
    'offer_made'::text,
    'offer_rejected'::text,
    'ghosting'::text,
    'contracts_signed'::text,
    'not_interested'::text,
    'wrong_number'::text,
    'signed_elsewhere'::text,
    'dnc'::text
  ]));

-- ─── Step 3: add the disposition_note column for terminal-stage moves ──

alter table public.ghl_contact_lifecycle
  add column if not exists disposition_note text;

comment on column public.ghl_contact_lifecycle.disposition_note is
  'Optional one-line reason captured when a VA moves a lead to a terminal '
  'stage (not_interested / wrong_number / signed_elsewhere / dnc). Surfaces '
  'searchable intel on common objection patterns.';

-- ─── Step 4: verify counts after rename ────────────────────────────────
-- (run manually after applying; commented out so the migration is idempotent)
--
-- select stage, count(*) as n
--   from public.ghl_contact_lifecycle
--  group by stage
--  order by n desc;
