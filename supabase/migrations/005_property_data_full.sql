-- 005_property_data_full.sql
-- Expands property_data so the weekly HCAD bulk sync can populate all
-- the fields the SIFTstack worker + dashboard modal want to display.
--
-- Source columns map to HCAD's real_acct.txt + building_res.txt + fixtures.txt.
-- One row per Harris County parcel_acct. Keyed on parcel_acct.
--
-- New columns:
--   situs_*    — the property's physical location (what matches records.prop_address)
--   owner_*    — HCAD's current owner of record
--   mail_*     — HCAD's mailing address for tax notices
--   land_val / bld_val / tot_appr_val  — component values
--   legal_description
--   normalized_situs (generated) + index → fast address→parcel_acct lookup

alter table public.property_data
    add column if not exists situs_address       text,
    add column if not exists situs_city          text,
    add column if not exists situs_zip           text,
    add column if not exists owner_name          text,
    add column if not exists mail_address        text,
    add column if not exists mail_city           text,
    add column if not exists mail_state          text,
    add column if not exists mail_zip            text,
    add column if not exists land_val            numeric(12,2),
    add column if not exists bld_val             numeric(12,2),
    add column if not exists tot_appr_val        numeric(12,2),
    add column if not exists legal_description   text;

-- Generated column for case/punctuation-insensitive address matching.
-- Strips everything except alphanumerics, uppercases. Used by the
-- SIFTstack worker to resolve records.prop_address → property_data.parcel_acct.
alter table public.property_data
    add column if not exists normalized_situs text
    generated always as (
        upper(regexp_replace(coalesce(situs_address, ''), '[^A-Za-z0-9]+', '', 'g'))
    ) stored;

create index if not exists property_data_normalized_situs_idx
    on public.property_data(normalized_situs);

create index if not exists property_data_situs_zip_idx
    on public.property_data(situs_zip);

-- Helper function: resolve records.prop_address → parcel_acct.
-- Used by the worker to back-fill records.parcel_acct before enrichment.
create or replace function public.resolve_parcel_by_address(
    p_address text,
    p_zip     text default null
)
returns text
language sql
stable
as $$
    select parcel_acct
    from public.property_data
    where normalized_situs = upper(regexp_replace(coalesce(p_address, ''), '[^A-Za-z0-9]+', '', 'g'))
      and (p_zip is null or situs_zip = p_zip)
    limit 1
$$;

comment on function public.resolve_parcel_by_address is
    'Look up property_data.parcel_acct by normalized situs address. '
    'Optional zip narrows the match. Returns null if no match.';

comment on column public.property_data.normalized_situs is
    'Situs address with all non-alphanumerics stripped, upper-cased. '
    'Used for fuzzy address lookups from records.prop_address.';
