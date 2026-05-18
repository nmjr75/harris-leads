-- 040_user_activity_log.sql
-- Tracks every dashboard session event so Nelson can audit when his
-- team is actually working vs. just logged in (off-hours access,
-- abandoned-tab gaps, etc).
--
-- Captures these events from admin.html (and any other dashboard page
-- that calls startActivityTracking() on auth):
--   login       → user just signed in / opened the page
--   heartbeat   → tab is visible and active (every 5 min)
--   focus       → tab regained focus (user came back to it)
--   blur        → tab lost focus (user switched away or minimized)
--   tab_close   → user closed the tab / navigated away
--   signout     → explicit sign-out click
--
-- RLS policy is strict: anyone authenticated can ONLY insert their own
-- events (user_id must match auth.uid()); only admin-role users can
-- SELECT. So VAs writing events from their browser cannot read each
-- other's data, and cannot read their own — keeping this strictly an
-- audit log Nelson controls.

create table if not exists public.user_activity_log (
  id          bigserial primary key,
  user_id     uuid references auth.users(id) on delete set null,
  user_email  text,
  event_type  text not null check (event_type in
              ('login','heartbeat','focus','blur','tab_close','signout')),
  page        text,
  user_agent  text,
  created_at  timestamptz not null default now()
);

create index if not exists user_activity_log_created_idx
  on public.user_activity_log(created_at desc);
create index if not exists user_activity_log_user_time_idx
  on public.user_activity_log(user_id, created_at desc);

alter table public.user_activity_log enable row level security;

-- Insert: any authenticated user can only insert their own events.
drop policy if exists "activity insert own" on public.user_activity_log;
create policy "activity insert own"
  on public.user_activity_log
  for insert
  to authenticated
  with check (user_id = auth.uid());

-- Select: only admin role can read (RLS via profiles.role).
drop policy if exists "activity admin read" on public.user_activity_log;
create policy "activity admin read"
  on public.user_activity_log
  for select
  to authenticated
  using (
    exists (
      select 1
      from public.profiles p
      where p.id = auth.uid()
        and p.role = 'admin'
    )
  );

grant insert, select on public.user_activity_log to authenticated;
grant usage, select on sequence public.user_activity_log_id_seq to authenticated;

-- Daily session summary view, admin-only by inheritance of RLS on the
-- underlying table. Bins activity into per-user, per-day buckets:
--   - first_seen / last_seen
--   - approximate active minutes (heartbeats × 5 min, capped at gap-aware)
--   - off-hours flag (first_seen before 8am OR last_seen after 8pm CT)
create or replace view public.user_activity_daily as
with by_day as (
  select
    user_id,
    user_email,
    (created_at at time zone 'America/Chicago')::date as activity_date,
    min(created_at) filter (where event_type = 'login') as first_login_at,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) filter (where event_type = 'heartbeat') as heartbeats,
    count(*) filter (where event_type = 'login') as logins,
    count(*) filter (where event_type = 'focus') as focuses,
    count(*) filter (where event_type = 'blur') as blurs
  from public.user_activity_log
  where user_id is not null
  group by user_id, user_email, (created_at at time zone 'America/Chicago')::date
)
select
  user_id,
  user_email,
  activity_date,
  first_login_at,
  first_seen_at,
  last_seen_at,
  heartbeats,
  logins,
  focuses,
  blurs,
  -- Rough active-time estimate: heartbeats × 5min. Caps at session
  -- span so a single 5-min heartbeat doesn't claim 5 min of activity
  -- if the whole session was only 2 min long.
  least(
    heartbeats * 5,
    greatest(1, ceil(extract(epoch from (last_seen_at - first_seen_at))/60))::int
  ) as approx_active_minutes,
  -- Off-hours flag: first sign-in before 8am CT or last seen after 8pm CT.
  (
    extract(hour from (first_seen_at at time zone 'America/Chicago')) < 8
    or extract(hour from (last_seen_at at time zone 'America/Chicago')) >= 20
  ) as off_hours
from by_day;

alter view public.user_activity_daily set (security_invoker = on);

grant select on public.user_activity_daily to authenticated;

comment on table public.user_activity_log is
  'Per-event session log for admin auditing. Each authenticated user '
  'can only insert their own rows; only admins can read.';
comment on view public.user_activity_daily is
  'Per-user per-day session summary (CT timezone). Inherits RLS from '
  'user_activity_log via security_invoker, so non-admins return empty.';
