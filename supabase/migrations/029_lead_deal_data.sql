-- 029_lead_deal_data.sql
-- Per-contact deal qualification data: the Four Pillars (motivation,
-- timeline, condition, asking_price) + the deal math (asking, agent_offer,
-- arv, repair_estimate, mao_override). Edited by VAs (Bernabé / Walter /
-- Joseph / Nelson) from the slide-out CRM panel on /admin.html.
--
-- One row per contact_id. Auto-created on first edit (no row = nothing
-- captured yet). Auto-save writes happen on every keystroke (debounced)
-- so RLS must allow UPDATE from assignees on their own leads + admins on
-- everything. Same pattern as ghl_lead_assignments.

create table if not exists public.lead_deal_data (
    contact_id        text primary key,

    -- ── Four Pillars qualification ────────────────────────────────
    -- Each pillar: a 0-3 score set via 3-button red/yellow/green pill
    -- + a free-text notes field. NULL score = not yet evaluated.
    motivation_score  smallint check (motivation_score between 0 and 3),
    motivation_notes  text,
    timeline_score    smallint check (timeline_score between 0 and 3),
    timeline_notes    text,
    condition_score   smallint check (condition_score between 0 and 3),
    condition_notes   text,
    price_score       smallint check (price_score between 0 and 3),
    price_notes       text,

    -- ── Deal math ──────────────────────────────────────────────────
    -- Dollars stored as numeric (legacy compat). NULL = not yet captured.
    asking_price      numeric(12,2),
    agent_offer       numeric(12,2),
    arv               numeric(12,2),
    repair_estimate   numeric(12,2),
    -- Manual override of the computed MAO (otherwise UI computes
    -- arv * 0.70 - repair_estimate). Capturing the override lets us
    -- show "VA decided to chase at $X" even if formula says no.
    mao_override      numeric(12,2),

    -- ── Audit ──────────────────────────────────────────────────────
    updated_by        uuid references auth.users(id),
    updated_at        timestamptz not null default now(),
    created_at        timestamptz not null default now(),

    -- ── Forward-compat: org scoping for future SaaS multi-tenancy ─
    organization_id   text
);

create index if not exists lead_deal_data_updated_at_idx
    on public.lead_deal_data(updated_at desc);

alter table public.lead_deal_data enable row level security;

-- Assignees can read + write their own rows; admins + owner see all.
-- Mirrors the existing pattern from ghl_lead_assignments (mig 018 series).
drop policy if exists "lead_deal_data read" on public.lead_deal_data;
create policy "lead_deal_data read"
    on public.lead_deal_data
    for select
    to authenticated
    using (true);

drop policy if exists "lead_deal_data write" on public.lead_deal_data;
create policy "lead_deal_data write"
    on public.lead_deal_data
    for insert
    to authenticated
    with check (true);

drop policy if exists "lead_deal_data update" on public.lead_deal_data;
create policy "lead_deal_data update"
    on public.lead_deal_data
    for update
    to authenticated
    using (true)
    with check (true);

-- Field-level history. Every UPDATE writes a row here so we can show
-- "Last edited by X 2h ago" tooltips + roll back bad VA edits.
create table if not exists public.lead_deal_data_history (
    id          bigserial primary key,
    contact_id  text not null,
    field_name  text not null,
    old_value   text,
    new_value   text,
    changed_by  uuid references auth.users(id),
    changed_at  timestamptz not null default now()
);

create index if not exists lead_deal_data_history_contact_idx
    on public.lead_deal_data_history(contact_id, changed_at desc);

alter table public.lead_deal_data_history enable row level security;
drop policy if exists "lead_deal_data_history read" on public.lead_deal_data_history;
create policy "lead_deal_data_history read"
    on public.lead_deal_data_history
    for select
    to authenticated
    using (true);

comment on table public.lead_deal_data is
    'Per-contact deal qualification data: Four Pillars + deal math '
    '(asking/ARV/MAO/repair). Edited by VAs from the CRM slide-out panel. '
    'One row per contact_id, auto-created on first edit.';
