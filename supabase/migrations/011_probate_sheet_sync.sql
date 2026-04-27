-- =============================================================================
-- Migration 011: probate sheet sync support
-- -----------------------------------------------------------------------------
-- Adds the columns the EstateTrace Google-Sheet sync needs to land its data
-- in public.records. The sync writes one row per probate case (or per
-- property when a case has multiple — they're suffixed -PROP2, -PROP3 in the
-- sheet). doc_num = the case_number from the sheet (e.g., '26-CPR-044102').
--
-- Why these columns:
--   case_status            — sheet's Status column ('Active'|'Pending'|'no_property_found')
--                            used by /probate route to filter the VA work queue
--   additional_applicants  — App 2+ data (some cases have multiple PRs)
--   sheet_tab_name         — which tab the row came from (e.g., 'Fortbend_County_TX_2026-04')
--                            audit trail; also lets us filter the dashboard by tab
--   sheet_synced_at        — when sync last touched this row (for staleness queries)
--   source                 — origin system: 'estatetrace' | 'clerk_lead' | 'clerk_foreclosure'
--                            so /probate route filters cleanly on EstateTrace-only data
--
-- Compatibility:
--   - All columns are nullable. Existing records keep working unchanged.
--   - The log_record_changes trigger already protects mail_address from
--     sheet-sync overwrite via the manual_overrides array (schema.sql line 317).
--     No trigger update needed.
--   - sheet sync writes via service_role (auth.uid() is null), so the trigger
--     classifies its writes as 'scraper' source — same as existing scrapers.
-- =============================================================================

alter table public.records
    add column if not exists case_status           text,
    add column if not exists additional_applicants jsonb,
    add column if not exists sheet_tab_name        text,
    add column if not exists sheet_synced_at       timestamptz,
    add column if not exists source                text;

-- Indexes for the dashboard /probate route queries
create index if not exists records_source_idx
    on public.records(source);
create index if not exists records_case_status_idx
    on public.records(case_status);
create index if not exists records_sheet_tab_idx
    on public.records(sheet_tab_name);
create index if not exists records_sheet_synced_idx
    on public.records(sheet_synced_at desc nulls last);

-- Composite index for the common /probate filter:
-- "all EstateTrace probate rows where status != no_property_found"
create index if not exists records_probate_workqueue_idx
    on public.records(source, case_status, county)
    where source = 'estatetrace';

-- =============================================================================
-- Backfill existing records with source='clerk' so the new column has values
-- for non-EstateTrace rows. Distinguishes lead-scraper from foreclosure-scraper
-- by doc_type prefix (FRCL = foreclosure, everything else = clerk lead).
-- =============================================================================
update public.records
   set source = case
                  when doc_type = 'FRCL' then 'clerk_foreclosure'
                  else 'clerk_lead'
                end
 where source is null;
