-- 041_ghl_contact_tags.sql
-- Mirrors REI Reply contact tags into Supabase so the dashboard can surface
-- workflow-driven intent signals — primarily the "appointment booked" /
-- "appointment set" tag set REBA AI applies when a lead confirms a callback
-- time. Schema is intentionally generic so any future tag-driven surfacing
-- (e.g. "needs follow-up", "do not text") just needs a view + a pill, no
-- schema work.
--
-- Sync model: REI Reply workflow fires on "Contact Tag Added" (or "Contact
-- Tag Updated"), POSTs to the sync-ghl-contact-tags Edge Function with the
-- contact_id and the FULL current tag set. The function replaces the
-- contact's tag rows atomically (delete + insert), so tag REMOVALS propagate
-- naturally without a separate event type — empty tag list = clear all tags.

create table if not exists public.ghl_contact_tags (
  id          bigserial primary key,
  contact_id  text not null,
  tag         text not null,
  applied_at  timestamptz not null default now(),
  unique (contact_id, tag)
);

create index if not exists ghl_contact_tags_contact_idx
  on public.ghl_contact_tags(contact_id);
create index if not exists ghl_contact_tags_tag_lower_idx
  on public.ghl_contact_tags(lower(tag));

alter table public.ghl_contact_tags enable row level security;

-- Read: everyone authenticated can read. Tags are not sensitive PII and are
-- needed by the dashboard render path.
drop policy if exists "tags read all auth" on public.ghl_contact_tags;
create policy "tags read all auth" on public.ghl_contact_tags
  for select to authenticated using (true);

-- Write: only the Edge Function (using SUPABASE_SERVICE_ROLE_KEY) can mutate
-- this table. No insert/update/delete policy for `authenticated`, so VAs
-- cannot fabricate appointment status from the dashboard.

-- Appointment-set view: collapses the 4 known tag variants REBA applies
-- ("AI voice appointment booked", "AI chat appointment booked", "appointment
-- set", "appointment booked") into a single per-contact row. Case-insensitive
-- match so REI Reply UI capitalization changes don't break the surface.
drop view if exists public.ghl_contact_has_appointment;
create view public.ghl_contact_has_appointment as
select
  contact_id,
  max(applied_at)                          as latest_appointment_at,
  array_agg(distinct tag order by tag)     as appointment_tags
from public.ghl_contact_tags
where lower(tag) in (
  'ai voice appointment booked',
  'ai chat appointment booked',
  'appointment booked',
  'appointment set'
)
group by contact_id;

grant select on public.ghl_contact_has_appointment to authenticated;
