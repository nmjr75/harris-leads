-- =============================================================================
-- CountyData — Phase 1 Schema
-- =============================================================================
-- Creates 7 tables, 1 audit trigger, 1 helper function, storage bucket,
-- and all RLS policies. Runs once, re-runnable where safe.
--
-- Paste entire file into: Supabase → SQL Editor → New Query → Run.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Extensions
-- -----------------------------------------------------------------------------
create extension if not exists "pgcrypto";     -- gen_random_uuid()

-- =============================================================================
-- Table: profiles
-- -----------------------------------------------------------------------------
-- One row per user. Linked 1:1 with auth.users. Stores display name + role.
-- Role governs what the user can do (see RLS policies below).
-- =============================================================================
create table if not exists public.profiles (
    id              uuid primary key references auth.users(id) on delete cascade,
    display_name    text not null,
    role            text not null default 'viewer'
                    check (role in ('admin', 'editor', 'viewer')),
    created_at      timestamptz not null default now()
);

-- Helper: fetch current user's role. Used in every RLS policy below.
create or replace function public.current_user_role()
returns text
language sql
security definer
stable
set search_path = public
as $$
    select role from public.profiles where id = auth.uid()
$$;

-- Auto-create a profile row when a new auth user signs up (default role = viewer)
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into public.profiles (id, display_name, role)
    values (
        new.id,
        coalesce(new.raw_user_meta_data->>'display_name', split_part(new.email, '@', 1)),
        'viewer'
    );
    return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
    after insert on auth.users
    for each row execute function public.handle_new_user();

-- =============================================================================
-- Table: records
-- -----------------------------------------------------------------------------
-- Unified table: clerk leads + foreclosure postings + future county types.
-- doc_type distinguishes (L/P, PROB, AFFT, FRCL, etc).
-- county + state make this multi-county ready.
-- manual_overrides tracks which fields humans edited so scraper won't overwrite.
-- =============================================================================
create table if not exists public.records (
    id                  uuid primary key default gen_random_uuid(),
    doc_num             text unique not null,
    doc_type            text,
    cat                 text,
    cat_label           text,

    -- Locality (multi-county ready)
    county              text not null default 'Harris',
    state               text not null default 'TX',
    filed_date          date,
    sale_date           date,

    -- Parties (editable)
    owner               text,
    co_borrower         text,
    grantor_name        text,
    grantee_names       text,
    mortgagee           text,

    -- Property (editable)
    prop_address        text,
    prop_city           text,
    prop_state          text default 'TX',
    prop_zip            text,
    legal_description   text,
    amount              text,

    -- Mailing (editable)
    mail_address        text,
    mail_city           text,
    mail_state          text default 'TX',
    mail_zip            text,

    -- Linking
    parcel_acct         text,
    clerk_url           text,
    hcad_url            text,
    pdf_url             text,

    -- Scoring
    match_confidence    text,
    address_source      text,
    flags               text[] not null default array[]::text[],
    score               int,

    -- Scraper metadata
    batch_id            text,
    batch_label         text,
    date_scraped        date,
    needs_ocr           boolean default false,

    -- Audit
    manual_overrides    text[] not null default array[]::text[],
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now(),
    updated_by          uuid references auth.users(id)
);

create index if not exists records_county_state_idx on public.records(county, state);
create index if not exists records_filed_date_idx   on public.records(filed_date desc);
create index if not exists records_doc_type_idx     on public.records(doc_type);
create index if not exists records_parcel_acct_idx  on public.records(parcel_acct);
create index if not exists records_score_idx        on public.records(score desc);
create index if not exists records_match_conf_idx   on public.records(match_confidence);

-- =============================================================================
-- Table: property_data
-- -----------------------------------------------------------------------------
-- HCAD property characteristics, keyed by parcel account.
-- Populated by extended HCAD scraper. Joined to records via parcel_acct.
-- =============================================================================
create table if not exists public.property_data (
    parcel_acct         text primary key,
    county              text not null default 'Harris',
    state               text not null default 'TX',
    bedrooms            int,
    bathrooms           numeric(3,1),
    sqft                int,
    year_built          int,
    lot_size_sqft       int,
    market_value        numeric(12,2),
    appraised_value     numeric(12,2),
    structure_type      text,
    hcad_updated_at     timestamptz not null default now()
);

create index if not exists property_data_county_idx on public.property_data(county, state);

-- =============================================================================
-- Table: record_history
-- -----------------------------------------------------------------------------
-- Append-only audit log. Written by the log_record_changes trigger.
-- One row per field changed. Immutable (no UPDATE / DELETE policies).
-- =============================================================================
create table if not exists public.record_history (
    id              bigserial primary key,
    record_id       uuid not null references public.records(id) on delete cascade,
    field_name      text not null,
    old_value       text,
    new_value       text,
    changed_by      uuid references auth.users(id),
    changed_at      timestamptz not null default now(),
    source          text not null default 'manual'
                    check (source in ('manual', 'scraper', 'siftstack'))
);

create index if not exists record_history_record_idx on public.record_history(record_id, changed_at desc);
create index if not exists record_history_user_idx   on public.record_history(changed_by);

-- =============================================================================
-- Table: siftstack_queue
-- -----------------------------------------------------------------------------
-- Records queued for SiftStack processing. Populated when a VA hits "SIFTstack"
-- button. A GitHub Actions worker polls pending rows, runs enrichment + upload.
-- =============================================================================
create table if not exists public.siftstack_queue (
    id              bigserial primary key,
    record_id       uuid not null references public.records(id) on delete cascade,
    batch_id        uuid not null,
    queued_by       uuid not null references auth.users(id),
    queued_at       timestamptz not null default now(),
    status          text not null default 'pending'
                    check (status in ('pending', 'processing', 'completed', 'failed')),
    error           text
);

create index if not exists siftstack_queue_status_idx on public.siftstack_queue(status, queued_at);
create index if not exists siftstack_queue_batch_idx  on public.siftstack_queue(batch_id);

-- =============================================================================
-- Table: siftstack_runs
-- -----------------------------------------------------------------------------
-- One row per batch. Outcome summary.
-- =============================================================================
create table if not exists public.siftstack_runs (
    id                  bigserial primary key,
    batch_id            uuid unique not null,
    triggered_by        uuid not null references auth.users(id),
    triggered_at        timestamptz not null default now(),
    completed_at        timestamptz,
    record_count        int,
    datasift_list_name  text,
    drive_csv_url       text,
    skip_trace_results  jsonb,
    status              text not null default 'pending'
                        check (status in ('pending', 'processing', 'completed', 'failed')),
    summary             jsonb
);

-- =============================================================================
-- Table: scraper_runs
-- -----------------------------------------------------------------------------
-- Scraper metadata per GitHub Actions run. Observability.
-- =============================================================================
create table if not exists public.scraper_runs (
    id                  bigserial primary key,
    run_id              text,
    started_at          timestamptz not null default now(),
    completed_at        timestamptz,
    date_range_from     date,
    date_range_to       date,
    records_ingested    int,
    pdfs_attempted      int,
    pdfs_succeeded      int,
    status              text not null default 'running'
                        check (status in ('running', 'completed', 'failed')),
    error_summary       text
);

create index if not exists scraper_runs_started_idx on public.scraper_runs(started_at desc);

-- =============================================================================
-- Storage bucket: clerk_pdfs
-- -----------------------------------------------------------------------------
-- Holds raw clerk PDF files. records.pdf_url points here.
-- Private: access via signed URLs only.
-- =============================================================================
insert into storage.buckets (id, name, public)
values ('clerk_pdfs', 'clerk_pdfs', false)
on conflict (id) do nothing;

-- =============================================================================
-- Trigger function: log_record_changes
-- -----------------------------------------------------------------------------
-- On UPDATE of records:
--   1. Enforces editor role can only change editable fields (silently resets
--      attempts to change protected fields).
--   2. Sets updated_at + updated_by automatically.
--   3. Appends changed editable fields to manual_overrides (for humans only).
--   4. Writes one row to record_history per changed field.
-- =============================================================================
create or replace function public.log_record_changes()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
    v_source    text := 'manual';
    v_user_id   uuid := auth.uid();
    v_role      text := public.current_user_role();
    v_changed   text[] := array[]::text[];
begin
    -- Scraper calls use the secret key; auth.uid() returns null in that case.
    if v_user_id is null then
        v_source := 'scraper';
    end if;

    -- If editor (not admin, not scraper), silently revert any change to
    -- non-editable columns. Defence in depth — the UI shouldn't allow it anyway.
    if v_source = 'manual' and v_role = 'editor' then
        new.id              := old.id;
        new.doc_num         := old.doc_num;
        new.doc_type        := old.doc_type;
        new.cat             := old.cat;
        new.cat_label       := old.cat_label;
        new.county          := old.county;
        new.state           := old.state;
        new.filed_date      := old.filed_date;
        new.grantee_names   := old.grantee_names;
        new.mortgagee       := old.mortgagee;
        new.parcel_acct     := old.parcel_acct;
        new.clerk_url       := old.clerk_url;
        new.hcad_url        := old.hcad_url;
        new.pdf_url         := old.pdf_url;
        new.match_confidence:= old.match_confidence;
        new.address_source  := old.address_source;
        new.flags           := old.flags;
        new.score           := old.score;
        new.batch_id        := old.batch_id;
        new.batch_label     := old.batch_label;
        new.date_scraped    := old.date_scraped;
        new.needs_ocr       := old.needs_ocr;
        new.created_at      := old.created_at;
    end if;

    -- If scraper, skip manual-override fields entirely (never overwrite human edits)
    if v_source = 'scraper' and old.manual_overrides is not null then
        if 'owner'             = any(old.manual_overrides) then new.owner := old.owner; end if;
        if 'co_borrower'       = any(old.manual_overrides) then new.co_borrower := old.co_borrower; end if;
        if 'grantor_name'      = any(old.manual_overrides) then new.grantor_name := old.grantor_name; end if;
        if 'legal_description' = any(old.manual_overrides) then new.legal_description := old.legal_description; end if;
        if 'prop_address'      = any(old.manual_overrides) then new.prop_address := old.prop_address; end if;
        if 'prop_city'         = any(old.manual_overrides) then new.prop_city := old.prop_city; end if;
        if 'prop_zip'          = any(old.manual_overrides) then new.prop_zip := old.prop_zip; end if;
        if 'mail_address'      = any(old.manual_overrides) then new.mail_address := old.mail_address; end if;
        if 'mail_city'         = any(old.manual_overrides) then new.mail_city := old.mail_city; end if;
        if 'mail_zip'          = any(old.manual_overrides) then new.mail_zip := old.mail_zip; end if;
        if 'amount'            = any(old.manual_overrides) then new.amount := old.amount; end if;
        if 'sale_date'         = any(old.manual_overrides) then new.sale_date := old.sale_date; end if;
    end if;

    -- Refresh audit metadata
    new.updated_at := now();
    if v_source = 'manual' then
        new.updated_by := v_user_id;
    end if;

    -- Compare editable fields, log each change, collect names
    if new.owner is distinct from old.owner then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'owner', old.owner, new.owner, v_user_id, v_source);
        v_changed := array_append(v_changed, 'owner');
    end if;
    if new.co_borrower is distinct from old.co_borrower then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'co_borrower', old.co_borrower, new.co_borrower, v_user_id, v_source);
        v_changed := array_append(v_changed, 'co_borrower');
    end if;
    if new.grantor_name is distinct from old.grantor_name then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'grantor_name', old.grantor_name, new.grantor_name, v_user_id, v_source);
        v_changed := array_append(v_changed, 'grantor_name');
    end if;
    if new.legal_description is distinct from old.legal_description then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'legal_description', old.legal_description, new.legal_description, v_user_id, v_source);
        v_changed := array_append(v_changed, 'legal_description');
    end if;
    if new.prop_address is distinct from old.prop_address then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_address', old.prop_address, new.prop_address, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_address');
    end if;
    if new.prop_city is distinct from old.prop_city then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_city', old.prop_city, new.prop_city, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_city');
    end if;
    if new.prop_zip is distinct from old.prop_zip then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_zip', old.prop_zip, new.prop_zip, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_zip');
    end if;
    if new.mail_address is distinct from old.mail_address then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_address', old.mail_address, new.mail_address, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_address');
    end if;
    if new.mail_city is distinct from old.mail_city then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_city', old.mail_city, new.mail_city, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_city');
    end if;
    if new.mail_zip is distinct from old.mail_zip then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_zip', old.mail_zip, new.mail_zip, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_zip');
    end if;
    if new.amount is distinct from old.amount then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'amount', old.amount, new.amount, v_user_id, v_source);
        v_changed := array_append(v_changed, 'amount');
    end if;
    if new.sale_date is distinct from old.sale_date then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'sale_date', old.sale_date::text, new.sale_date::text, v_user_id, v_source);
        v_changed := array_append(v_changed, 'sale_date');
    end if;

    -- If a human made the change, merge the changed field names into manual_overrides
    if v_source = 'manual' and array_length(v_changed, 1) > 0 then
        new.manual_overrides := (
            select coalesce(array_agg(distinct x), array[]::text[])
            from unnest(coalesce(new.manual_overrides, array[]::text[]) || v_changed) x
        );
    end if;

    return new;
end;
$$;

drop trigger if exists trg_log_record_changes on public.records;
create trigger trg_log_record_changes
    before update on public.records
    for each row execute function public.log_record_changes();

-- =============================================================================
-- Row-Level Security — enable on all tables
-- =============================================================================
alter table public.profiles          enable row level security;
alter table public.records           enable row level security;
alter table public.property_data     enable row level security;
alter table public.record_history    enable row level security;
alter table public.siftstack_queue   enable row level security;
alter table public.siftstack_runs    enable row level security;
alter table public.scraper_runs      enable row level security;

-- -----------------------------------------------------------------------------
-- profiles policies
-- -----------------------------------------------------------------------------
drop policy if exists "profiles: self and admin can read"  on public.profiles;
drop policy if exists "profiles: self can update name"     on public.profiles;
drop policy if exists "profiles: admin can update all"     on public.profiles;

create policy "profiles: self and admin can read" on public.profiles
    for select to authenticated
    using (id = auth.uid() or public.current_user_role() = 'admin');

create policy "profiles: self can update name" on public.profiles
    for update to authenticated
    using (id = auth.uid())
    with check (id = auth.uid() and role = (select role from public.profiles where id = auth.uid()));

create policy "profiles: admin can update all" on public.profiles
    for update to authenticated
    using (public.current_user_role() = 'admin')
    with check (public.current_user_role() = 'admin');

-- -----------------------------------------------------------------------------
-- records policies
-- -----------------------------------------------------------------------------
drop policy if exists "records: authenticated read"      on public.records;
drop policy if exists "records: editor+ update"          on public.records;
drop policy if exists "records: admin insert"            on public.records;
drop policy if exists "records: admin delete"            on public.records;

create policy "records: authenticated read" on public.records
    for select to authenticated
    using (true);

create policy "records: editor+ update" on public.records
    for update to authenticated
    using (public.current_user_role() in ('admin', 'editor'))
    with check (public.current_user_role() in ('admin', 'editor'));

create policy "records: admin insert" on public.records
    for insert to authenticated
    with check (public.current_user_role() = 'admin');

create policy "records: admin delete" on public.records
    for delete to authenticated
    using (public.current_user_role() = 'admin');

-- -----------------------------------------------------------------------------
-- property_data policies
-- -----------------------------------------------------------------------------
drop policy if exists "property_data: authenticated read" on public.property_data;

create policy "property_data: authenticated read" on public.property_data
    for select to authenticated
    using (true);
-- INSERT / UPDATE / DELETE: no policies = only service role (scraper) can write

-- -----------------------------------------------------------------------------
-- record_history policies (read-only for users; trigger writes via definer)
-- -----------------------------------------------------------------------------
drop policy if exists "record_history: authenticated read" on public.record_history;

create policy "record_history: authenticated read" on public.record_history
    for select to authenticated
    using (true);
-- No INSERT/UPDATE/DELETE policies = immutable from the client side.
-- The log_record_changes trigger runs as security definer so it can write.

-- -----------------------------------------------------------------------------
-- siftstack_queue policies
-- -----------------------------------------------------------------------------
drop policy if exists "siftstack_queue: authenticated read" on public.siftstack_queue;
drop policy if exists "siftstack_queue: editor+ insert"    on public.siftstack_queue;
drop policy if exists "siftstack_queue: admin update"      on public.siftstack_queue;

create policy "siftstack_queue: authenticated read" on public.siftstack_queue
    for select to authenticated
    using (true);

create policy "siftstack_queue: editor+ insert" on public.siftstack_queue
    for insert to authenticated
    with check (public.current_user_role() in ('admin', 'editor') and queued_by = auth.uid());

create policy "siftstack_queue: admin update" on public.siftstack_queue
    for update to authenticated
    using (public.current_user_role() = 'admin')
    with check (public.current_user_role() = 'admin');

-- -----------------------------------------------------------------------------
-- siftstack_runs policies
-- -----------------------------------------------------------------------------
drop policy if exists "siftstack_runs: authenticated read" on public.siftstack_runs;

create policy "siftstack_runs: authenticated read" on public.siftstack_runs
    for select to authenticated
    using (true);
-- Writes: service role only (worker runs under sb_secret_ key).

-- -----------------------------------------------------------------------------
-- scraper_runs policies
-- -----------------------------------------------------------------------------
drop policy if exists "scraper_runs: authenticated read" on public.scraper_runs;

create policy "scraper_runs: authenticated read" on public.scraper_runs
    for select to authenticated
    using (true);
-- Writes: service role only (scraper workflow).

-- -----------------------------------------------------------------------------
-- Storage policies: clerk_pdfs bucket
-- -----------------------------------------------------------------------------
drop policy if exists "clerk_pdfs: authenticated read" on storage.objects;

create policy "clerk_pdfs: authenticated read" on storage.objects
    for select to authenticated
    using (bucket_id = 'clerk_pdfs');
-- Writes: service role only (scraper uploads PDFs).

-- =============================================================================
-- Done.
-- =============================================================================
-- After running this:
--   1. Log into the dashboard once via Supabase Auth (creates your auth.users row)
--   2. Run: update public.profiles set role='admin' where id = auth.uid();
--      (promotes you from default 'viewer' to 'admin')
-- =============================================================================
