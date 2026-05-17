-- 038_real_message_counts.sql
-- Fix: ghl_contact_text_summary previously used
-- ghl_conversation_snapshots.message_count, which counts EVERYTHING in
-- the messages array including GHL system events (type=28
-- "Opportunity created", "Opportunity updated", "Opportunity moved").
-- That made the chip show "11 texts" on Jonathan Fink's row when only
-- ~4 real texts existed.
--
-- This rewrite counts via jsonb_array_elements + filter (type <> 28),
-- matching the same filter last_real_message() uses for last-text
-- direction. Numbers now reflect actual SMS / email exchanges only.

create or replace view public.ghl_contact_text_summary as
with by_conv as (
  select
    s.contact_id,
    s.conversation_id,
    -- Real messages = anything in the array that isn't a system event.
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int <> 28
    ) as real_total,
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int <> 28
        and m->>'direction' = 'inbound'
    ) as real_inbound,
    (
      select count(*)::int
      from jsonb_array_elements(coalesce(s.messages, '[]'::jsonb)) m
      where (m->>'type') is not null
        and (m->>'type')::int <> 28
        and m->>'direction' = 'outbound'
    ) as real_outbound,
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
  sum(real_total)::int                                     as text_count_total,
  sum(real_inbound)::int                                   as text_count_inbound,
  sum(real_outbound)::int                                  as text_count_outbound,
  max((last_msg ->> 'dateAdded')::timestamptz)             as last_text_at,
  max(last_message_date)                                   as last_message_date,
  max(pulled_at)                                           as last_snapshot_pulled_at,
  max(case when rn = 1 then last_msg ->> 'direction' end)  as last_text_direction
from ranked
group by contact_id;

alter view public.ghl_contact_text_summary set (security_invoker = on);

grant select on public.ghl_contact_text_summary to authenticated, service_role;
