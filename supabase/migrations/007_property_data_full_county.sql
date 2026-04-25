-- 007_property_data_full_county.sql
-- Expands property_data to hold the full Harris County HCAD payload
-- (1.6M parcels, not just the address-matched subset) and adds the
-- fields required for REI-quality decisioning:
--
--   * Decoded labels for coded fields (state class, school district, etc.)
--   * Market area + neighborhood identifiers
--   * Current-owner acquisition date + prior-year values (YoY signal)
--   * Flat exemption flags (has_homestead, has_over65, has_disabled,
--     has_disabled_vet) from Real_jur_exempt.zip
--   * Map facet / key map identifiers for VA lookup
--
-- Also creates the hcad_code_descriptions reference table used at load
-- time to denormalize labels onto property_data rows.

-- ─────────────────────────────────────────────────────────────────
-- 1. New columns on property_data
-- ─────────────────────────────────────────────────────────────────
alter table public.property_data
    add column if not exists state_class_label    text,
    add column if not exists school_district_code text,
    add column if not exists school_district_label text,
    add column if not exists neighborhood_code    text,
    add column if not exists neighborhood_group   text,
    add column if not exists market_area_code     text,
    add column if not exists market_area_label    text,
    add column if not exists map_facet            text,
    add column if not exists key_map              text,
    add column if not exists assessed_val         numeric(12,2),
    add column if not exists acreage              numeric(10,4),
    add column if not exists prior_tot_appr_val   numeric(12,2),
    add column if not exists prior_tot_mkt_val    numeric(12,2),
    add column if not exists new_owner_date       date,
    add column if not exists has_homestead        boolean default false,
    add column if not exists has_over65           boolean default false,
    add column if not exists has_disabled         boolean default false,
    add column if not exists has_disabled_vet     boolean default false,
    add column if not exists last_synced_at       timestamptz;

-- Indexes for the new filterable fields
create index if not exists property_data_school_dist_idx
    on public.property_data(school_district_code);
create index if not exists property_data_neighborhood_idx
    on public.property_data(neighborhood_code);
create index if not exists property_data_market_area_idx
    on public.property_data(market_area_code);
create index if not exists property_data_has_homestead_idx
    on public.property_data(has_homestead) where has_homestead = true;
create index if not exists property_data_new_owner_date_idx
    on public.property_data(new_owner_date);

-- ─────────────────────────────────────────────────────────────────
-- 2. hcad_code_descriptions reference table
--    Holds every code → description mapping from
--    Code_description_real.zip. Keyed on (code_type, code).
--    code_type = the filename stem, e.g. 'state_class',
--    'school_district', 'exemption_category'.
-- ─────────────────────────────────────────────────────────────────
create table if not exists public.hcad_code_descriptions (
    code_type   text not null,
    code        text not null,
    description text not null,
    updated_at  timestamptz not null default now(),
    primary key (code_type, code)
);

-- Public-read, service-role-write
alter table public.hcad_code_descriptions enable row level security;

drop policy if exists "hcad_code_descriptions read-all"
    on public.hcad_code_descriptions;
create policy "hcad_code_descriptions read-all"
    on public.hcad_code_descriptions
    for select
    using (true);

comment on table  public.hcad_code_descriptions is
    'HCAD Code_description_real.zip decode tables. Loaded by sync_hcad_bulk.py.';
comment on column public.property_data.has_homestead is
    'True if any RES (Residential Homestead) exemption exists on the parcel in any taxing district. Owner-occupied signal.';
comment on column public.property_data.has_over65 is
    'True if any over-65 exemption (OVR/APO/POV/SUR) exists on the parcel.';
comment on column public.property_data.has_disabled is
    'True if any non-veteran disability exemption (DIS/APD/PDS) exists on the parcel.';
comment on column public.property_data.has_disabled_vet is
    'True if any disabled-veteran exemption (V11-V14/V21-V24/VTX/VCH/VS1-VS4/STX) exists on the parcel.';
comment on column public.property_data.new_owner_date is
    'HCAD new_own_dt — date the current owner acquired the parcel. Key REI signal for "years held".';
comment on column public.property_data.last_synced_at is
    'When this row was last refreshed by sync_hcad_bulk.py.';
