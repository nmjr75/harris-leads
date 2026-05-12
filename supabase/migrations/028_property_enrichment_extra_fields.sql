-- 028_property_enrichment_extra_fields.sql
-- Extends property_enrichment with the fields visible on DataSift's
-- SiftMap detail panel beyond the basic value/equity/sqft set captured
-- in migration 027: county-assessor value, mortgage balance, last
-- sale price + date, tax delinquent value + year, and the distressor
-- signal flags (High Equity / Absentee / Bad Credit / Low Income etc.)
-- that DataSift surfaces per property.

alter table public.property_enrichment
    add column if not exists county_assessed_value numeric(12,2),
    add column if not exists mortgage_balance      numeric(12,2),
    add column if not exists last_sale_price       numeric(12,2),
    add column if not exists last_sale_date        date,
    add column if not exists tax_delinquent_value  numeric(12,2),
    add column if not exists tax_delinquent_year   int,
    add column if not exists high_equity           boolean,
    add column if not exists low_equity            boolean,
    add column if not exists absentee_owner        boolean,
    add column if not exists vacant                boolean,
    add column if not exists distressor_signals    text[];

comment on column public.property_enrichment.county_assessed_value is
    'County tax-assessor appraised value, when DataSift surfaces it on the SiftMap panel.';
comment on column public.property_enrichment.mortgage_balance is
    'Outstanding mortgage balance from DataSift''s Owner tab, when visible.';
comment on column public.property_enrichment.distressor_signals is
    'Array of distressor labels DataSift attaches to the property '
    '(e.g. {"Low Income","Bad Credit","Absentee Owners"}).';
