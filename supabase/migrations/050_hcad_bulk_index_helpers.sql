-- 050_hcad_bulk_index_helpers.sql
--
-- Cuts Disk IO budget burn during the weekly HCAD bulk sync (~1.6M parcels)
-- by ~50%. Surgical fix targeting the single most expensive index on
-- property_data — the GIN trigram on legal_description (built by
-- migration 008). GIN posting-list maintenance is far more IO-heavy
-- per row than the btree updates around it.
--
-- Strategy: drop the trigram index BEFORE the bulk upsert, rebuild
-- from scratch AFTER. Rebuilding on the final dataset in one shot is
-- ~3x faster than incremental row-by-row maintenance over 1.6M upserts.
--
-- Called by scraper/sync_hcad_bulk.py around upsert_parcels().
--
-- Trade-off during the rebuild window (~5-15 min):
--   resolve_parcel_by_legal() and lookup_parcels_by_legal() fall back
--   to sequential scan over 1.6M rows (~2s per call, vs ~100ms with
--   the index). Saturday 3 AM CT sync timing means the lead scraper
--   (Sunday 4 AM CT) is safely after the rebuild completes. The
--   SIFTstack worker firing every 5min during the window will degrade
--   gracefully — legal-fallback queries get slower, address-based
--   resolution unaffected.

-- Drop the GIN trigram index. IF EXISTS so re-running the function
-- after a failure is a no-op.
create or replace function public.hcad_drop_heavy_indexes()
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
    msg text := '';
begin
    drop index if exists public.property_data_legal_trgm_idx;
    msg := 'dropped property_data_legal_trgm_idx';
    return msg;
end;
$$;

-- Recreate the GIN trigram index from scratch. IF NOT EXISTS so
-- re-running after partial failure is safe. Building from scratch
-- on the post-sync dataset is significantly faster than incremental
-- maintenance during the 1.6M-row upsert.
create or replace function public.hcad_rebuild_heavy_indexes()
returns text
language plpgsql
security definer
set search_path = public
as $$
begin
    create index if not exists property_data_legal_trgm_idx
        on public.property_data
        using gin (legal_description gin_trgm_ops);
    return 'rebuilt property_data_legal_trgm_idx';
end;
$$;

-- Service-role-only. The sync script runs with the secret key →
-- maps to service_role. No other role should be running DDL.
revoke execute on function public.hcad_drop_heavy_indexes()    from public, anon, authenticated;
revoke execute on function public.hcad_rebuild_heavy_indexes() from public, anon, authenticated;

grant   execute on function public.hcad_drop_heavy_indexes()    to service_role;
grant   execute on function public.hcad_rebuild_heavy_indexes() to service_role;

comment on function public.hcad_drop_heavy_indexes is
    'Drop the GIN trigram index on property_data.legal_description before '
    'a bulk HCAD sync. Saves ~50% of Disk IO during the upsert. Pairs with '
    'hcad_rebuild_heavy_indexes(). Service-role-only.';

comment on function public.hcad_rebuild_heavy_indexes is
    'Recreate the GIN trigram index after a bulk HCAD sync. Building from '
    'scratch on the final dataset is ~3x faster than maintaining the '
    'inverted-list posting structure incrementally during the upsert. '
    'Service-role-only.';

-- ─────────────────────────────────────────────────────────────────────
-- FOLLOW-UP (next session) — differential sync via hash
-- ─────────────────────────────────────────────────────────────────────
-- The bulk sync currently upserts ALL 1.6M parcels every Saturday even
-- though typically <5% change week-over-week. The next major IO win
-- is to:
--   1. Add property_data.data_hash text (md5 of value-bearing columns)
--   2. Compute the hash client-side during collect_real_acct()
--   3. Skip upserts where the new hash matches the stored hash
-- Expected: ~95% reduction in upsert volume → ~95% reduction in IO.
-- Deferred from this migration to keep the scope tight tonight.
