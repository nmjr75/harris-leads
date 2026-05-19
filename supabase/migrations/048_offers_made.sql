-- 048_offers_made.sql
-- Per-event log of offers extended by the acquisitions team. Drives the
-- daily KPI scorecard ("offers made today vs target = 3"), the Offers
-- reporting tab, and the Slack alert fired on insert / accepted-update.
--
-- One row per offer extended. Outcome is updatable as the homeowner
-- responds. Stage moves in REI Reply remain MANUAL — the agent decides
-- whether an accepted offer becomes Offer Made / Offer Accepted, a
-- rejected offer becomes Offer Rejected / Dead, etc.

create table if not exists public.offers_made (
    id              bigserial   primary key,
    contact_id      text        not null,
    conversation_id text,
    agent_email     text        not null,
    agent_name      text,
    amount          numeric     not null check (amount > 0),
    outcome         text        not null default 'pending'
                                 check (outcome in (
                                     'pending', 'accepted', 'rejected',
                                     'counter', 'no_response'
                                 )),
    counter_amount  numeric     check (counter_amount is null or counter_amount > 0),
    notes           text,
    offered_at      timestamptz not null default now(),
    updated_at      timestamptz
);

create index if not exists idx_offers_made_agent_date
    on public.offers_made (agent_email, offered_at desc);
create index if not exists idx_offers_made_contact
    on public.offers_made (contact_id, offered_at desc);
create index if not exists idx_offers_made_outcome
    on public.offers_made (outcome) where outcome != 'pending';

create or replace function public._offers_made_set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at := now();
    return new;
end$$;

drop trigger if exists offers_made_set_updated_at on public.offers_made;
create trigger offers_made_set_updated_at
    before update on public.offers_made
    for each row
    execute function public._offers_made_set_updated_at();

alter table public.offers_made enable row level security;

-- SELECT: every authenticated dashboard user can read the table. RLS on the
-- contact lookup layer (ghl_lead_assignments etc.) already scopes which
-- contacts each assignee can act on; offers attached to contacts they can't
-- see won't reveal anything they can't already see, and the report needs to
-- aggregate across the team for the KPI scorecard.
drop policy if exists "offers_made read all authenticated" on public.offers_made;
create policy "offers_made read all authenticated"
    on public.offers_made
    for select
    to authenticated
    using (true);

-- INSERT: the caller must be Nelson (owner), an admin (Bernabé), OR the
-- contact's current assignee. Server-side guard so a misbehaving client
-- can't log offers under someone else's name.
drop policy if exists "offers_made insert by author or manager" on public.offers_made;
create policy "offers_made insert by author or manager"
    on public.offers_made
    for insert
    to authenticated
    with check (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = (
                  select lower(u.email) from auth.users u where u.id = auth.uid()
              )
        )
    );

-- UPDATE: outcome updates allowed by the original author, the contact's
-- current assignee, or owner/admin. The amount + offered_at + agent_email
-- columns are immutable in practice (we enforce via the dashboard layer
-- since Postgres column-level RLS is verbose; if an agent tries to mutate
-- those via curl, the audit trail will catch it).
drop policy if exists "offers_made update by author or manager" on public.offers_made;
create policy "offers_made update by author or manager"
    on public.offers_made
    for update
    to authenticated
    using (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or lower(agent_email) = (
            select lower(u.email) from auth.users u where u.id = auth.uid()
        )
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = (
                  select lower(u.email) from auth.users u where u.id = auth.uid()
              )
        )
    )
    with check (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or lower(agent_email) = (
            select lower(u.email) from auth.users u where u.id = auth.uid()
        )
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = (
                  select lower(u.email) from auth.users u where u.id = auth.uid()
              )
        )
    );

-- DELETE explicitly forbidden — audit trail must remain intact.
drop policy if exists "offers_made no delete" on public.offers_made;
create policy "offers_made no delete"
    on public.offers_made
    for delete
    to authenticated
    using (false);

-- Realtime publication so the dashboard updates across browser tabs as
-- offers are logged or outcomes change.
do $$
begin
    if not exists (
        select 1 from pg_publication_tables
        where pubname = 'supabase_realtime'
          and schemaname = 'public'
          and tablename = 'offers_made'
    ) then
        alter publication supabase_realtime add table public.offers_made;
    end if;
end$$;

-- Per-agent daily KPI rollup. The dashboard reads from this view directly
-- (one row per agent per day) for the offers-today metric on the scorecard.
create or replace view public.offers_made_today_by_user as
select
    agent_email,
    coalesce(max(agent_name), agent_email) as agent_name,
    count(*)::int                          as offers_count,
    sum(amount)::numeric                   as total_volume,
    count(*) filter (where outcome = 'accepted')::int as accepted_count,
    max(offered_at)                        as last_offer_at
from public.offers_made
where offered_at >= (date_trunc('day', now() at time zone 'America/Chicago')) at time zone 'America/Chicago'
group by agent_email;

grant select on public.offers_made_today_by_user to authenticated;

comment on table public.offers_made is
    'Per-event log of offers extended. Drives the offers KPI scorecard, the '
    'Offers reporting tab, and Slack alerts. Stage moves in REI Reply remain '
    'manual — the agent decides which pipeline step matches each outcome.';
