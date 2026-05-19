-- 049_joseph_email_swap.sql
--
-- Swap Joseph Castelan's canonical dashboard email
--   FROM: joseph.hhremodeling@gmail.com
--   TO:   joseph@continentalhouserenovations.com
--
-- Performed in lockstep with:
--   * Code edits in admin.html / index.html / probate.html /
--     SiftStack/src/ghl/auto_move_hit_list.py (8 occurrences)
--   * Supabase Auth → Users → Change email (keeps auth.users.id stable
--     so profiles.id, user_activity_log.user_id, and all auth.uid()-keyed
--     history carry forward automatically)
--   * REI Reply (GHL) Settings → My Staff (already done by Nelson before
--     this migration was authored)
--   * Cloudflare Access allowlist update (manual UI step)
--
-- This migration redefines is_assignee() with the new email and
-- back-fills the 7 email-keyed history tables so Joseph's RLS-gated
-- visibility, his per-VA call card, his note + stage attribution,
-- and his activity-log mirror all carry forward intact.
--
-- ─────────────────────────────────────────────────────────────────────
-- STEP 1 — PREVIEW (paste this BLOCK FIRST, alone, in SQL editor)
-- ─────────────────────────────────────────────────────────────────────
-- Counts that will be touched by STEP 2. Run this first and eyeball
-- the numbers before committing to the UPDATE. Per the no-destructive-
-- SQL rule, see counts before mutating.
--
-- select 'assignments_to'   as table_field, count(*) from ghl_lead_assignments where lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'assignments_by',                   count(*) from ghl_lead_assignments where lower(assigned_by_email) = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'calls_agent',                      count(*) from ghl_calls            where lower(agent_email)       = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'notes_created_by',                 count(*) from ghl_contact_notes    where lower(created_by_email)  = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'lifecycle_set_by',                 count(*) from ghl_contact_lifecycle where lower(set_by_email)     = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'tasks_assigned_to',                count(*) from ghl_contact_tasks    where lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'score_overrides_set_by',           count(*) from ghl_score_overrides  where lower(set_by_email)      = 'joseph.hhremodeling@gmail.com'
-- union all
-- select 'activity_log_email',               count(*) from user_activity_log    where lower(user_email)        = 'joseph.hhremodeling@gmail.com';

-- ─────────────────────────────────────────────────────────────────────
-- STEP 2 — REDEFINE is_assignee() + BACKFILL  (atomic transaction)
-- ─────────────────────────────────────────────────────────────────────
-- Wrapped in a single transaction so a partial failure rolls back
-- cleanly. Each UPDATE uses lower() comparison to catch any casing drift.

BEGIN;

-- 2a. Redefine the role-gate function. Walter unchanged. Paul is not
-- currently in this function (latent gap flagged separately — out of
-- scope for this migration).
CREATE OR REPLACE FUNCTION is_assignee() RETURNS BOOLEAN AS $$
    SELECT COALESCE(
        auth.jwt() ->> 'email' IN (
            'joseph@continentalhouserenovations.com',  -- Joseph Castelan (closer)
            'mark.realdealsource@gmail.com'            -- Walter (follow-up specialist)
        ),
        FALSE
    );
$$ LANGUAGE SQL SECURITY DEFINER STABLE;

-- 2b. Historical row backfills. Each statement is bounded by the OLD
-- email — no NULL-field conditions, no unbounded sweeps.

UPDATE ghl_lead_assignments
   SET assigned_to_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_lead_assignments
   SET assigned_by_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(assigned_by_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_calls
   SET agent_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(agent_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_contact_notes
   SET created_by_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(created_by_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_contact_lifecycle
   SET set_by_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(set_by_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_contact_tasks
   SET assigned_to_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com';

UPDATE ghl_score_overrides
   SET set_by_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(set_by_email) = 'joseph.hhremodeling@gmail.com';

UPDATE user_activity_log
   SET user_email = 'joseph@continentalhouserenovations.com'
 WHERE lower(user_email) = 'joseph.hhremodeling@gmail.com';

COMMIT;

-- ─────────────────────────────────────────────────────────────────────
-- STEP 3 — POST-MIGRATION VERIFY (paste after STEP 2 succeeds)
-- ─────────────────────────────────────────────────────────────────────
-- Should return ALL ZEROS. If any non-zero, investigate before
-- proceeding with the Supabase Auth email change.
--
-- select 'leftover_assignments_to'   as t, count(*) from ghl_lead_assignments where lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_assignments_by',  count(*) from ghl_lead_assignments where lower(assigned_by_email) = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_calls',           count(*) from ghl_calls            where lower(agent_email)       = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_notes',           count(*) from ghl_contact_notes    where lower(created_by_email)  = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_lifecycle',       count(*) from ghl_contact_lifecycle where lower(set_by_email)     = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_tasks',           count(*) from ghl_contact_tasks    where lower(assigned_to_email) = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_score_overrides', count(*) from ghl_score_overrides  where lower(set_by_email)      = 'joseph.hhremodeling@gmail.com'
-- union all select 'leftover_activity',        count(*) from user_activity_log    where lower(user_email)        = 'joseph.hhremodeling@gmail.com';
