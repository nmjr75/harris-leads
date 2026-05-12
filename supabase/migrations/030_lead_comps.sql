-- 030_lead_comps.sql
-- Up to 3 comp rows per contact for the Comps tab on the CRM slide-out
-- panel. VA enters address + sold date + sale price + sqft; UI auto-
-- computes $/sqft, applies any per-comp adjustment, averages the three
-- adjusted values into the lead's ARV.
--
-- Schema notes:
--   * slot = 1 / 2 / 3 — composite-unique with contact_id so we never
--     get more than 3 comps per lead (matches typical investor workflow:
--     pick 3 best comparable sales).
--   * adjustment_amount is signed cents-as-numeric for + and - tweaks
--     (e.g. +$5K for an extra bedroom, -$8K for inferior condition).

create table if not exists public.lead_comps (
    id              bigserial primary key,
    contact_id      text not null,
    slot            smallint not null check (slot between 1 and 3),

    address         text,
    sold_date       date,
    sale_price      numeric(12,2),
    sqft            integer,
    -- Free-text describing the adjustment rationale, e.g.
    -- "+$5K extra bedroom, -$8K worse condition" → net -3000
    adjustment_notes  text,
    adjustment_amount numeric(12,2) default 0,

    updated_by      uuid references auth.users(id),
    updated_at      timestamptz not null default now(),
    organization_id text,

    unique (contact_id, slot)
);

create index if not exists lead_comps_contact_idx
    on public.lead_comps(contact_id);

alter table public.lead_comps enable row level security;

drop policy if exists "lead_comps read" on public.lead_comps;
create policy "lead_comps read"
    on public.lead_comps
    for select to authenticated using (true);

drop policy if exists "lead_comps write" on public.lead_comps;
create policy "lead_comps write"
    on public.lead_comps
    for insert to authenticated with check (true);

drop policy if exists "lead_comps update" on public.lead_comps;
create policy "lead_comps update"
    on public.lead_comps
    for update to authenticated using (true) with check (true);

drop policy if exists "lead_comps delete" on public.lead_comps;
create policy "lead_comps delete"
    on public.lead_comps
    for delete to authenticated using (true);

comment on table public.lead_comps is
    'Per-contact comparable sales for the Comps tab. Max 3 per contact '
    '(slot = 1/2/3). Auto-computed adjusted_value = sale_price + '
    'adjustment_amount drives the ARV calc on the Deal tab.';
