-- 039_text_filter_sms_only.sql + call-direction breakdown
--
-- Two fixes shipped together:
--
-- 1. Tighten the text filter from "anything not a system event" to
--    SMS-only. GHL message types observed in production:
--       1  = Call event (appears in conversation as a clickable
--            tile with audio — NOT a real text)
--       2  = SMS
--       25 = Email
--       28 = System event ("Opportunity created/updated/moved")
--
--    Migration 038 only excluded type=28, which let type=1 call
--    entries inflate the count (Jonathan Fink showed 7 = 4 SMS +
--    3 call entries). This pins the filter to type=2.
--
-- 2. Expose per-direction "last call" timestamps so the dashboard
--    can render outbound and inbound as separate chips. Conflating
--    them (e.g. showing "3 calls, 3h ago" when the 3h-ago one is a
--    missed inbound and the actual outreach was 10h ago) obscures
--    the actual call-attempt history a VA needs.

-- Tighter "last SMS" helper.
create or replace function public.last_real_message(msgs jsonb)
returns jsonb
language sql
immutable
as $$
  select m
  from jsonb_array_elements(coalesce(msgs, '[]'::jsonb)) m
  where (m->>'type') is not null
    and (m->>'type')::int = 2  -- SMS only
  order by (m->>'dateAdded')::timestamptz desc
  limit 1
$$;

comment on function public.last_real_message is
  'Returns the most recent SMS (type=2) from a snapshot messages '
  'array, or NULL.';

-- Text rollup — SMS-only.
create or replace view public.ghl_contact_text_summary as
with by_conv as (
  select
    s.contact_id,
    s.conversation_id,
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int = 2
    ) as sms_total,
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int = 2
        and m->>'direction' = 'inbound'
    ) as sms_inbound,
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int = 2
        and m->>'direction' = 'outbound'
    ) as sms_outbound,
    s.pulled_at,
    s.last_message_date,
    public.last_real_message(s.messages) as last_msg
  from public.ghl_latest_conversation_snapshot s
),
ranked as (
  select *,
         row_number() over (
           partition by contact_id
           order by (last_msg ->> 'dateAdded')::timestamptz desc nulls last
         ) as rn
  from by_conv
)
select
  contact_id,
  sum(sms_total)::int                                      as text_count_total,
  sum(sms_inbound)::int                                    as text_count_inbound,
  sum(sms_outbound)::int                                   as text_count_outbound,
  max((last_msg ->> 'dateAdded')::timestamptz)             as last_text_at,
  max(last_message_date)                                   as last_message_date,
  max(pulled_at)                                           as last_snapshot_pulled_at,
  max(case when rn = 1 then last_msg ->> 'direction' end)  as last_text_direction
from ranked
group by contact_id;

alter view public.ghl_contact_text_summary set (security_invoker = on);

-- Call rollup — adds per-direction "last call" timestamps + statuses
-- so the dashboard can render outbound and inbound separately.
create or replace view public.ghl_contact_call_summary as
with ranked as (
  select *,
         row_number() over (partition by contact_id order by started_at desc) as rn,
         row_number() over (partition by contact_id, direction order by started_at desc) as rn_dir
  from public.ghl_calls
  where contact_id is not null
)
select
  contact_id,
  count(*)::int                                                  as call_count_total,
  count(*) filter (where direction = 'outbound')::int            as call_count_outbound,
  count(*) filter (where direction = 'inbound')::int             as call_count_inbound,
  max(started_at)                                                as last_call_at,
  max(case when rn = 1 then direction end)                       as last_call_direction,
  max(case when rn = 1 then duration_seconds end)                as last_call_duration_seconds,
  max(case when rn = 1 then status end)                          as last_call_status,
  -- per-direction breakdown so the chip can separate "Walter's last outreach"
  -- from "homeowner's last ring".
  max(case when rn_dir = 1 and direction = 'outbound' then started_at end)        as last_outbound_call_at,
  max(case when rn_dir = 1 and direction = 'outbound' then status end)            as last_outbound_call_status,
  max(case when rn_dir = 1 and direction = 'outbound' then duration_seconds end)  as last_outbound_call_duration_seconds,
  max(case when rn_dir = 1 and direction = 'outbound' then agent_name end)        as last_outbound_call_agent,
  max(case when rn_dir = 1 and direction = 'inbound'  then started_at end)        as last_inbound_call_at,
  max(case when rn_dir = 1 and direction = 'inbound'  then status end)            as last_inbound_call_status,
  max(case when rn_dir = 1 and direction = 'inbound'  then duration_seconds end)  as last_inbound_call_duration_seconds
from ranked
group by contact_id;

alter view public.ghl_contact_call_summary set (security_invoker = on);

-- Combined view — extend to expose the new per-direction columns so
-- the dashboard reads everything from one query.
create or replace view public.ghl_contact_activity_summary as
select
  coalesce(c.contact_id, t.contact_id)                  as contact_id,
  coalesce(c.call_count_total, 0)                       as call_count_total,
  coalesce(c.call_count_outbound, 0)                    as call_count_outbound,
  coalesce(c.call_count_inbound, 0)                     as call_count_inbound,
  c.last_call_at,
  c.last_call_direction,
  c.last_call_duration_seconds,
  c.last_call_status,
  c.last_outbound_call_at,
  c.last_outbound_call_status,
  c.last_outbound_call_duration_seconds,
  c.last_outbound_call_agent,
  c.last_inbound_call_at,
  c.last_inbound_call_status,
  c.last_inbound_call_duration_seconds,
  coalesce(t.text_count_total, 0)                       as text_count_total,
  coalesce(t.text_count_outbound, 0)                    as text_count_outbound,
  coalesce(t.text_count_inbound, 0)                     as text_count_inbound,
  t.last_text_at,
  t.last_text_direction,
  t.last_snapshot_pulled_at,
  case
    when t.last_snapshot_pulled_at is null then null
    else extract(epoch from (now() - t.last_snapshot_pulled_at))::int
  end                                                   as snapshot_age_seconds
from public.ghl_contact_call_summary c
full outer join public.ghl_contact_text_summary t
  on c.contact_id = t.contact_id;

alter view public.ghl_contact_activity_summary set (security_invoker = on);

grant select on public.ghl_contact_text_summary     to authenticated, service_role;
grant select on public.ghl_contact_call_summary     to authenticated, service_role;
grant select on public.ghl_contact_activity_summary to authenticated, service_role;
