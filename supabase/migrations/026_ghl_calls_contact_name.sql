-- 026_ghl_calls_contact_name.sql
-- Stores the contact's display name on every call row at sync time so the
-- Call Activity drill-down shows real names for contacts that aren't in
-- DATA.leads / DATA.hitList / DATA.assignments (e.g. contacts the VA
-- called who never replied to SMS, never reached the Hit List stage,
-- never got assigned).
--
-- Before this column existed, resolveContactName() in admin.html only
-- looked at the three client-side maps above and rendered "Unknown
-- contact" for everything else. Storing the name on the call row at
-- sync time is the right fix because:
--   1. The name is fixed at call time anyway (it's the GHL contact's
--      current display name as seen by the agent dialing).
--   2. It survives even if the contact later gets renamed in GHL.
--   3. No client-side join needed.
--
-- The sync-ghl-call Edge Function (deployed via Supabase UI) must be
-- updated in lockstep to populate this column. See
-- supabase/functions/sync-ghl-call/index.ts for the deployable source.

alter table public.ghl_calls
    add column if not exists contact_name text;

comment on column public.ghl_calls.contact_name is
    'Contact display name at the time of the call. Populated by '
    'sync-ghl-call from the GHL Call Details merge fields (preferred) or '
    'a /contacts/{id} fetch fallback. Used by the Call Activity tab so '
    'rows show real names for contacts that aren''t in the dashboard''s '
    'in-memory lookups.';
