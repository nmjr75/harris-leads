-- 027_property_enrichment.sql
-- Caches DataSift SiftMap per-property enrichment so the dashboard can
-- show year-built / sqft / value / equity / beds / baths / MLS status
-- inline on every lead row, regardless of which county they're in.
--
-- The SiftMap UI is per-property (no bulk export). The enricher worker
-- (scraper/siftmap_enricher.py) navigates to the SiftMap URL with each
-- contact's address as a query param, scrapes the Property Details
-- side panel, and upserts a row keyed on normalized_address. Each
-- address is looked up ONCE then served forever from this table — no
-- repeat hits to DataSift, well under the 100K/mo lookup quota.
--
-- Dashboard joins on normalized_address (case- + punctuation-insensitive)
-- so spelling variants in GHL contact addresses still match the cached
-- row.

-- ─────────────────────────────────────────────────────────────────
-- 1. property_enrichment table
-- ─────────────────────────────────────────────────────────────────
create table if not exists public.property_enrichment (
    -- Stable lookup key: alphanumerics-only, upper-cased.
    normalized_address text primary key,

    -- The address as it appeared when we looked it up (for debugging).
    raw_address        text not null,
    city               text,
    state              text,
    postal_code        text,

    -- Property fundamentals (static; cached forever).
    bedrooms           int,
    bathrooms          numeric(4,1),
    sqft               int,
    acres              numeric(10,4),
    year_built         int,
    structure_type     text,    -- "Single Family Residential", etc.
    ownership_type     text,    -- "Individual Owned", "LLC", etc.

    -- Valuation + equity.
    estimated_value    numeric(12,2),
    equity_pct         numeric(5,2),

    -- Status flags from SiftMap badges.
    mls_status         text,    -- "Off Market", "On Market", etc.
    days_on_market     int,
    owner_occupied     boolean,
    off_market         boolean,

    -- AI investor scores when DataSift exposed them (otherwise null).
    ai_off_market_investor numeric(5,2),
    ai_on_market_investor  numeric(5,2),
    ai_realtor             numeric(5,2),

    -- Provenance.
    source             text not null default 'siftmap',
    raw_panel_text     text,                  -- full scraped panel text (debug)
    looked_up_at       timestamptz not null default now(),
    last_refreshed_at  timestamptz not null default now()
);

create index if not exists property_enrichment_postal_idx
    on public.property_enrichment(postal_code);

create index if not exists property_enrichment_state_idx
    on public.property_enrichment(state);

comment on table public.property_enrichment is
    'Per-property data cached from DataSift SiftMap. One row per '
    'normalized address. Populated by scraper/siftmap_enricher.py on '
    'a daily cron. Read by the dashboard inline on every lead row.';

comment on column public.property_enrichment.normalized_address is
    'Address with all non-alphanumerics stripped + upper-cased. Stable '
    'join key; survives "Dr." vs "Drive" + casing variants.';

-- ─────────────────────────────────────────────────────────────────
-- 2. RLS — authenticated read; service-role write (worker)
-- ─────────────────────────────────────────────────────────────────
alter table public.property_enrichment enable row level security;

drop policy if exists "property_enrichment authenticated read"
    on public.property_enrichment;
create policy "property_enrichment authenticated read"
    on public.property_enrichment
    for select
    to authenticated
    using (true);

-- ─────────────────────────────────────────────────────────────────
-- 3. Helper RPC: which addresses still need enrichment?
--    Returns addresses present in ghl_latest_contact_address that
--    don't yet have a property_enrichment row. Used by the worker to
--    pick the next batch.
-- ─────────────────────────────────────────────────────────────────
create or replace function public.addresses_needing_enrichment(
    p_limit int default 100
)
returns table (
    raw_address  text,
    city         text,
    state        text,
    postal_code  text,
    normalized_address text
)
language sql
stable
as $$
    select distinct
        ca.contact_address       as raw_address,
        ca.contact_city          as city,
        ca.contact_state         as state,
        ca.contact_postal_code   as postal_code,
        upper(regexp_replace(coalesce(ca.contact_address,''), '[^A-Za-z0-9]+', '', 'g'))
                                 as normalized_address
    from public.ghl_latest_contact_address ca
    where ca.contact_address is not null
      and ca.contact_address <> ''
      and not exists (
          select 1
          from public.property_enrichment pe
          where pe.normalized_address =
              upper(regexp_replace(coalesce(ca.contact_address,''), '[^A-Za-z0-9]+', '', 'g'))
      )
    order by 1
    limit p_limit;
$$;

grant execute on function public.addresses_needing_enrichment(int) to authenticated;
grant execute on function public.addresses_needing_enrichment(int) to service_role;

comment on function public.addresses_needing_enrichment is
    'Returns up to p_limit distinct dashboard-contact addresses that '
    'don''t yet have a property_enrichment row. Worker iterates these '
    'on each cron tick.';
