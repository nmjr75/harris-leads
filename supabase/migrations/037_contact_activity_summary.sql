-- 037_contact_activity_summary.sql
-- Per-contact activity rollup for the dashboard chips + the Communication
-- section in the CRM Details panel. Surfaces total call/text counts,
-- last call/text timestamps, and the direction of the last text — so
-- VAs can scan the queue and instantly see what's been done.
--
-- Reads from:
--   * public.ghl_calls               (per-call rows, real-time via webhook)
--   * public.ghl_conversation_snapshots (text history, refreshed in batches)
--
-- The "freshness" of the text columns equals the freshness of the
-- snapshot worker (~6-hourly per active conversation as of 2026-05-16).
-- snapshot_age_seconds is exposed so the dashboard can render a
-- staleness indicator on each chip.

-- ─────────────────────────────────────────────────────────────────
-- Helper function: most recent non-system message from a snapshot.
-- ─────────────────────────────────────────────────────────────────
-- GHL message types we care about (from observed snapshot data):
--   2  = SMS
--   25 = unknown (likely email — appears in conversations with email contact)
--   28 = system event ("Opportunity created" etc — NOT a real message)
-- We skip type 28 so "last text" doesn't surface a system event.
create or replace function public.last_real_message(msgs jsonb)
returns jsonb
language sql
immutable
as $$
  select m
  from jsonb_array_elements(coalesce(msgs, '[]'::jsonb)) m
  where (m->>'type') is not null
    and (m->>'type')::int <> 28
  order by (m->>'dateAdded')::timestamptz desc
  limit 1
$$;

comment on function public.last_real_message is
  'Returns the most recent non-system-event message from a snapshot '
  'messages array, or NULL.';

-- ─────────────────────────────────────────────────────────────────
-- Latest snapshot per conversation_id. The snapshots table grows
-- over time (one row per pull), so we need the most-recent per
-- conversation as the basis for any per-contact rollup.
-- ─────────────────────────────────────────────────────────────────
create or replace view public.ghl_latest_conversation_snapshot as
select distinct on (conversation_id)
  conversation_id,
  contact_id,
  message_count,
  inbound_count,
  last_message_date,
  messages,
  pulled_at
from public.ghl_conversation_snapshots
where contact_id is not null
order by conversation_id, pulled_at desc;

alter view public.ghl_latest_conversation_snapshot set (security_invoker = on);

-- ─────────────────────────────────────────────────────────────────
-- Per-contact text rollup. One row per contact_id, aggregating
-- across ALL conversations attached to that contact.
-- ─────────────────────────────────────────────────────────────────
create or replace view public.ghl_contact_text_summary as
with by_conv as (
  select s.contact_id,
         s.conversation_id,
         coalesce(s.message_count, 0) as message_count,
         coalesce(s.inbound_count, 0) as inbound_count,
         s.pulled_at,
         s.last_message_date,
         public.last_real_message(s.messages) as last_msg
  from public.ghl_latest_conversation_snapshot s
),
ranked as (
  -- Within each contact, pick the conversation whose last_real_message
  -- has the latest dateAdded — that's the source of last_text_direction.
  select *,
         row_number() over (
           partition by contact_id
           order by (last_msg ->> 'dateAdded')::timestamptz desc nulls last
         ) as rn
  from by_conv
)
select
  contact_id,
  sum(message_count)::int                              as text_count_total,
  sum(inbound_count)::int                              as text_count_inbound,
  (sum(message_count) - sum(inbound_count))::int       as text_count_outbound,
  max((last_msg ->> 'dateAdded')::timestamptz)         as last_text_at,
  max(last_message_date)                               as last_message_date,
  max(pulled_at)                                       as last_snapshot_pulled_at,
  max(case when rn = 1 then last_msg ->> 'direction' end) as last_text_direction
from ranked
group by contact_id;

alter view public.ghl_contact_text_summary set (security_invoker = on);

-- ─────────────────────────────────────────────────────────────────
-- Per-contact call rollup. Webhook-fed, real-time.
-- ─────────────────────────────────────────────────────────────────
create or replace view public.ghl_contact_call_summary as
with ranked as (
  select *,
         row_number() over (partition by contact_id order by started_at desc) as rn
  from public.ghl_calls
  where contact_id is not null
)
select
  contact_id,
  count(*)::int                                              as call_count_total,
  count(*) filter (where direction = 'outbound')::int        as call_count_outbound,
  count(*) filter (where direction = 'inbound')::int         as call_count_inbound,
  max(started_at)                                            as last_call_at,
  max(case when rn = 1 then direction end)                   as last_call_direction,
  max(case when rn = 1 then duration_seconds end)            as last_call_duration_seconds,
  max(case when rn = 1 then status end)                      as last_call_status
from ranked
group by contact_id;

alter view public.ghl_contact_call_summary set (security_invoker = on);

-- ─────────────────────────────────────────────────────────────────
-- Combined per-contact activity. Full outer join so a contact with
-- only-calls or only-texts still gets a row.
-- ─────────────────────────────────────────────────────────────────
create or replace view public.ghl_contact_activity_summary as
select
  coalesce(c.contact_id, t.contact_id)               as contact_id,
  -- calls
  coalesce(c.call_count_total, 0)                    as call_count_total,
  coalesce(c.call_count_outbound, 0)                 as call_count_outbound,
  coalesce(c.call_count_inbound, 0)                  as call_count_inbound,
  c.last_call_at,
  c.last_call_direction,
  c.last_call_duration_seconds,
  c.last_call_status,
  -- texts
  coalesce(t.text_count_total, 0)                    as text_count_total,
  coalesce(t.text_count_outbound, 0)                 as text_count_outbound,
  coalesce(t.text_count_inbound, 0)                  as text_count_inbound,
  t.last_text_at,
  t.last_text_direction,
  t.last_snapshot_pulled_at,
  case
    when t.last_snapshot_pulled_at is null then null
    else extract(epoch from (now() - t.last_snapshot_pulled_at))::int
  end                                                as snapshot_age_seconds
from public.ghl_contact_call_summary c
full outer join public.ghl_contact_text_summary t
  on c.contact_id = t.contact_id;

alter view public.ghl_contact_activity_summary set (security_invoker = on);

-- Grants — these chain views inherit RLS from ghl_calls and
-- ghl_conversation_snapshots via security_invoker.
grant select on public.ghl_latest_conversation_snapshot to authenticated, service_role;
grant select on public.ghl_contact_text_summary          to authenticated, service_role;
grant select on public.ghl_contact_call_summary          to authenticated, service_role;
grant select on public.ghl_contact_activity_summary      to authenticated, service_role;

comment on view public.ghl_contact_activity_summary is
  'Per-contact call + text activity rollup. Calls are real-time via '
  'webhook; texts are batched via the scorer''s snapshot worker (see '
  'snapshot_age_seconds for freshness). One row per contact_id.';
