-- 031_property_overrides.sql
-- VA-editable overrides for the property data SiftMap pulls. Lets the
-- VAs correct beds/baths/sqft/year/value/mortgage when DataSift's
-- enrichment is wrong. The dashboard reads property_enrichment LEFT
-- JOINed with property_overrides; override values win when present.
--
-- locked_fields[] is the field-level lock list. If 'bedrooms' is in the
-- array, the SiftMap enricher will NOT overwrite the bedrooms value on
-- its next run. Same pattern as records.manual_overrides (mig 002+).

create table if not exists public.property_overrides (
    normalized_address text primary key,

    -- Override values. NULL = no override (use enrichment value).
    bedrooms          int,
    bathrooms         numeric(4,1),
    sqft              int,
    year_built        int,
    estimated_value   numeric(12,2),
    mortgage_balance  numeric(12,2),
    condition         text,           -- e.g. "Light", "Medium", "Heavy", "Tear-down"
    condition_notes   text,
    free_form_notes   text,           -- whatever the VA wants to capture

    -- Fields the VA has touched; SiftMap enricher must skip these.
    locked_fields     text[] not null default array[]::text[],

    updated_by        uuid references auth.users(id),
    updated_at        timestamptz not null default now(),
    organization_id   text
);

create index if not exists property_overrides_updated_at_idx
    on public.property_overrides(updated_at desc);

alter table public.property_overrides enable row level security;

drop policy if exists "property_overrides read" on public.property_overrides;
create policy "property_overrides read"
    on public.property_overrides
    for select to authenticated using (true);

drop policy if exists "property_overrides write" on public.property_overrides;
create policy "property_overrides write"
    on public.property_overrides
    for insert to authenticated with check (true);

drop policy if exists "property_overrides update" on public.property_overrides;
create policy "property_overrides update"
    on public.property_overrides
    for update to authenticated using (true) with check (true);

comment on table public.property_overrides is
    'VA-editable corrections to SiftMap property data. Keyed on '
    'normalized_address (matches property_enrichment). locked_fields '
    'protects edited fields from being overwritten by the daily enricher.';
