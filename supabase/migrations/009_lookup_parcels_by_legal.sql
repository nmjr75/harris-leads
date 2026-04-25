-- 009_lookup_parcels_by_legal.sql
-- Adds a public-readable RPC for the legal-description lookup tool
-- at /lookup.html. Returns up to 20 candidate parcels matching the
-- subdivision + lot/block/section tokens.
--
-- Why a new RPC instead of reusing resolve_parcel_by_legal:
--   * resolve_parcel_by_legal returns ONLY when the match is unique
--     (refuse-when-ambiguous). The lookup tool serving VAs needs the
--     candidate list when ambiguous so a human can pick.
--   * Marked SECURITY DEFINER + granted to anon so the public lookup
--     page can call it without authenticating. property_data itself
--     stays RLS-locked to authenticated users only.

create or replace function public.lookup_parcels_by_legal(
    p_subdiv  text,
    p_section text default null,
    p_lot     text default null,
    p_block   text default null
)
returns table (
    parcel_acct           text,
    situs_address         text,
    situs_city            text,
    situs_zip             text,
    owner_name            text,
    legal_description     text,
    structure_type        text,
    state_class_label     text,
    school_district_label text,
    market_area_label     text,
    tot_appr_val          numeric,
    sqft                  integer,
    bedrooms              integer,
    bathrooms             numeric,
    year_built            integer,
    has_homestead         boolean,
    new_owner_date        date
)
language sql
stable
security definer
set search_path = public
as $$
    select
        parcel_acct,
        situs_address,
        situs_city,
        situs_zip,
        owner_name,
        legal_description,
        structure_type,
        state_class_label,
        school_district_label,
        market_area_label,
        tot_appr_val,
        sqft,
        bedrooms,
        bathrooms,
        year_built,
        has_homestead,
        new_owner_date
    from public.property_data
    where legal_description ilike '%' || upper(p_subdiv) || '%'
      and (p_lot     is null or legal_description ~* ('\mLT\s+'  || upper(p_lot)     || '\M'))
      and (p_block   is null or legal_description ~* ('\mBLK\s+' || upper(p_block)   || '\M'))
      and (p_section is null or legal_description ~* ('\mSEC\s+' || upper(p_section) || '\M'))
    order by parcel_acct
    limit 20;
$$;

-- Allow anonymous (publishable-key) callers to invoke this RPC.
grant execute on function public.lookup_parcels_by_legal(text, text, text, text) to anon;
grant execute on function public.lookup_parcels_by_legal(text, text, text, text) to authenticated;

comment on function public.lookup_parcels_by_legal is
    'VA-facing lookup: returns up to 20 candidate parcels matching the '
    'parsed legal-description tokens. SECURITY DEFINER so the public '
    'lookup.html page can read without auth.';
