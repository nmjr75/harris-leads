-- 042_ghl_contact_appointments.sql
-- Stores the next-upcoming (or most-recent) appointment per contact, sourced
-- from the GHL Appointments API. Joined into ghl_contact_has_appointment so
-- the dashboard chip can render the actual scheduled date/time, not just
-- "appointment set" with no time context.
--
-- Sync: the sync-ghl-contact-tags Edge Function calls GET /contacts/{id}/
-- appointments after persisting tags. We store one row per contact (latest
-- upcoming, or most-recent past if none upcoming). Reschedules / cancels
-- propagate naturally because every webhook fire re-fetches and upserts.

create table if not exists public.ghl_contact_appointments (
  contact_id          text primary key,
  appointment_at      timestamptz,
  appointment_title   text,
  appointment_status  text,
  raw                 jsonb,
  updated_at          timestamptz not null default now()
);

create index if not exists ghl_contact_appointments_at_idx
  on public.ghl_contact_appointments(appointment_at);

alter table public.ghl_contact_appointments enable row level security;

drop policy if exists "appointments read all auth" on public.ghl_contact_appointments;
create policy "appointments read all auth" on public.ghl_contact_appointments
  for select to authenticated using (true);

-- Rebuild the view to include appointment_at from the new table. LEFT JOIN
-- so contacts with appointment tags but no calendar event still surface
-- (the dashboard chip just renders without a time in that case).
drop view if exists public.ghl_contact_has_appointment;
create view public.ghl_contact_has_appointment as
select
  t.contact_id,
  max(t.applied_at)                                     as latest_appointment_at,
  array_agg(distinct t.tag order by t.tag)              as appointment_tags,
  max(a.appointment_at)                                 as appointment_scheduled_at,
  max(a.appointment_title)                              as appointment_title,
  max(a.appointment_status)                             as appointment_status
from public.ghl_contact_tags t
left join public.ghl_contact_appointments a using (contact_id)
where lower(t.tag) in (
  'ai voice appointment booked',
  'ai chat appointment booked',
  'appointment booked',
  'appointment set'
)
group by t.contact_id;

grant select on public.ghl_contact_has_appointment to authenticated;
