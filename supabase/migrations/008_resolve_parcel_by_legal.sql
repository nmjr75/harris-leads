-- 008_resolve_parcel_by_legal.sql
-- Adds the infrastructure for live legal-description → parcel_acct
-- resolution. Used by:
--   * scraper/supabase_sync.py — to fill prop_address on freshly-scraped
--     records before they're auto-queued
--   * scraper/run_siftstack_worker.py — as a fallback when the
--     address-based resolver returns null
--
-- 1. pg_trgm extension + GIN trigram index on legal_description
--    so ILIKE '%subdiv%' substring searches stay fast on 1.6M rows.
-- 2. resolve_parcel_by_legal RPC — accepts the parsed clerk legal
--    components (subdivision required, section/lot/block optional)
--    and returns a single parcel_acct only when the match is unique.

create extension if not exists pg_trgm;

create index if not exists property_data_legal_trgm_idx
    on public.property_data
    using gin (legal_description gin_trgm_ops);

create or replace function public.resolve_parcel_by_legal(
    p_subdiv  text,
    p_section text default null,
    p_lot     text default null,
    p_block   text default null
)
returns text
language sql
stable
as $$
    with matches as (
        select parcel_acct
        from public.property_data
        where legal_description ilike '%' || upper(p_subdiv) || '%'
          and (p_lot     is null or legal_description ~* ('\mLT\s+'  || upper(p_lot)     || '\M'))
          and (p_block   is null or legal_description ~* ('\mBLK\s+' || upper(p_block)   || '\M'))
          and (p_section is null or legal_description ~* ('\mSEC\s+' || upper(p_section) || '\M'))
        limit 2
    ),
    counted as (
        select parcel_acct, count(*) over () as total from matches
    )
    select parcel_acct from counted where total = 1 limit 1;
$$;

comment on function public.resolve_parcel_by_legal is
    'Resolve a clerk-style parsed legal description (subdivision required, '
    'section/lot/block optional) to a unique HCAD parcel_acct. Returns null '
    'when zero or multiple matches exist. Used for live legal-fallback '
    'matching after address-based resolution fails.';
