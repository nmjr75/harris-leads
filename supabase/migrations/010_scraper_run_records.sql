-- =============================================================================
-- Migration 010: per-run audit trail for scraper activity
-- -----------------------------------------------------------------------------
-- Adds:
--   1. New columns on scraper_runs to support a true Run-picker on the dashboard:
--      gh_run_number, workflow_name, total_seen, total_created, total_updated.
--   2. New junction table scraper_run_records that stores ONE row per
--      (run, record) pair. Captures whether the run created the record,
--      updated existing fields, or just verified-it-still-exists. The diff
--      itself lives in fields_changed (jsonb) for full provenance.
--
-- Why this exists:
--   The existing batch_id column on records only marks "first-discovery batch".
--   It cannot answer "which records did run #42 see / create / update?" because
--   records aren't re-stamped with batch_id on subsequent runs (by design, to
--   preserve discovery history). This junction table answers the run-picker
--   question without breaking the batch_id semantics.
--
-- Performance:
--   - 3 indexes cover the dashboard's two query patterns:
--     a) "all records seen by run X" → idx on run_id
--     b) "all runs that touched record Y" → idx on record_id
--     c) "doc_num history over time" → composite idx on (doc_num, observed_at)
--   - Rough sizing: 1184 records × ~365 runs/year ≈ 432k rows/year per scraper.
--     With 4 scrapers (current 2 + future 2), ~1.7M rows/year. Well within
--     Pro tier's 8 GB. Add a 90-day purge cron later if needed.
--
-- RLS:
--   - admin: full access
--   - editor: read all, never write (writes only via service_role from scrapers)
--   - viewer: read all
--   - service_role: bypasses RLS (scrapers write via service key)
-- =============================================================================

-- 1. Extend scraper_runs --------------------------------------------------------
alter table public.scraper_runs
    add column if not exists gh_run_number  int,
    add column if not exists workflow_name  text,
    add column if not exists total_seen     int default 0,
    add column if not exists total_created  int default 0,
    add column if not exists total_updated  int default 0;

-- Useful for fast Run-picker lookups
create index if not exists scraper_runs_gh_run_idx
    on public.scraper_runs(gh_run_number desc nulls last);
create index if not exists scraper_runs_workflow_started_idx
    on public.scraper_runs(workflow_name, started_at desc);

-- 2. Junction table -------------------------------------------------------------
create table if not exists public.scraper_run_records (
    id              bigserial primary key,
    run_id          bigint not null references public.scraper_runs(id) on delete cascade,
    record_id       uuid   not null references public.records(id)      on delete cascade,
    doc_num         text   not null,
    -- 'created'   = run discovered + inserted this record for the first time
    -- 'updated'   = run found existing record and changed >=1 field
    -- 'seen'      = run verified record exists, no field changes (no-op)
    -- 'enriched'  = run filled previously-NULL fields (subset of updated, useful
    --               for "address was None, now filled" alerts)
    action          text not null check (action in ('created','updated','seen','enriched')),
    -- jsonb diff: {column_name: [old_value, new_value], ...}
    -- NULL when action='seen' (no diff)
    fields_changed  jsonb,
    observed_at     timestamptz not null default now(),
    -- prevent accidental duplicate junction rows from a buggy scraper retry
    unique (run_id, record_id)
);

-- Indexes for the dashboard's two main query patterns + history lookup
create index if not exists scraper_run_records_run_idx
    on public.scraper_run_records(run_id);
create index if not exists scraper_run_records_record_idx
    on public.scraper_run_records(record_id);
create index if not exists scraper_run_records_doc_observed_idx
    on public.scraper_run_records(doc_num, observed_at desc);
create index if not exists scraper_run_records_action_idx
    on public.scraper_run_records(action);

-- 3. RLS ------------------------------------------------------------------------
alter table public.scraper_run_records enable row level security;

-- Admin: full access
drop policy if exists "scraper_run_records admin all" on public.scraper_run_records;
create policy "scraper_run_records admin all"
    on public.scraper_run_records
    for all
    using (
        exists (
            select 1 from public.profiles p
            where p.id = auth.uid() and p.role = 'admin'
        )
    )
    with check (
        exists (
            select 1 from public.profiles p
            where p.id = auth.uid() and p.role = 'admin'
        )
    );

-- Editor + viewer: read-only
drop policy if exists "scraper_run_records read for editors and viewers" on public.scraper_run_records;
create policy "scraper_run_records read for editors and viewers"
    on public.scraper_run_records
    for select
    using (
        exists (
            select 1 from public.profiles p
            where p.id = auth.uid() and p.role in ('editor','viewer')
        )
    );

-- Service role bypasses RLS automatically; no explicit policy needed.

-- =============================================================================
-- Verification queries (run manually after applying):
--
--   -- Confirm new columns on scraper_runs:
--   select column_name, data_type from information_schema.columns
--   where table_name = 'scraper_runs' and table_schema = 'public'
--   order by ordinal_position;
--
--   -- Confirm junction table:
--   select column_name, data_type from information_schema.columns
--   where table_name = 'scraper_run_records' and table_schema = 'public'
--   order by ordinal_position;
--
--   -- Confirm indexes:
--   select indexname from pg_indexes
--   where tablename = 'scraper_run_records' and schemaname = 'public';
--
--   -- Confirm RLS enabled:
--   select relname, relrowsecurity from pg_class
--   where relname = 'scraper_run_records';
-- =============================================================================
