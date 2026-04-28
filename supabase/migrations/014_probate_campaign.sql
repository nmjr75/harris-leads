-- 014_probate_campaign.sql
-- Probate -> DataSift automation queue.
--
-- Mirrors the siftstack_queue + records.siftstack_status pattern from
-- migration 003, but with a distinct queue table and status column so
-- the probate workflow is fully decoupled from the clerk-records pipeline.
--
-- Why a separate queue:
--   1. Probate records are pre-enriched by VA work (verification + skip-trace).
--      Reusing siftstack_queue would re-trigger obituary research / heir
--      verification / entity research and risk overwriting VA-confirmed data.
--   2. The probate worker reads from contact_phones / contact_emails (which
--      the existing siftstack worker has no awareness of) and skips the
--      heavy enrichment_pipeline step entirely.
--   3. Failures or DataSift outages on the probate side should not block
--      the daily clerk pipeline (and vice versa).
--
-- Status values (records.probate_campaign_status):
--   null         -> never submitted (default)
--   'queued'     -> row exists in probate_campaign_queue, worker hasn't processed
--   'completed'  -> worker uploaded successfully to DataSift
--   'failed'     -> worker tried but failed (see queue.error)

-- ── Columns on records ───────────────────────────────────────────────────
alter table public.records
    add column if not exists probate_campaign_status        text,
    add column if not exists probate_campaign_completed_at  timestamptz;

comment on column public.records.probate_campaign_status is
    'Probate -> DataSift submission status: null / queued / completed / failed. '
    'Maintained by triggers on probate_campaign_queue.';

comment on column public.records.probate_campaign_completed_at is
    'Timestamp set when probate-datasift-worker finishes processing this '
    'record (completed or failed). Null until then.';

create index if not exists records_probate_campaign_status_idx
    on public.records(probate_campaign_status);

-- ── Queue table ──────────────────────────────────────────────────────────
create table if not exists public.probate_campaign_queue (
    id           bigserial primary key,
    record_id    uuid not null references public.records(id) on delete cascade,
    batch_id     uuid not null,
    queued_by    uuid references auth.users(id),
    queued_at    timestamptz not null default now(),
    status       text not null default 'pending'
                 check (status in ('pending', 'processing', 'completed', 'failed')),
    error        text,
    completed_at timestamptz
);

create index if not exists probate_campaign_queue_status_idx
    on public.probate_campaign_queue(status, queued_at);
create index if not exists probate_campaign_queue_batch_idx
    on public.probate_campaign_queue(batch_id);
create index if not exists probate_campaign_queue_record_idx
    on public.probate_campaign_queue(record_id);

comment on table public.probate_campaign_queue is
    'Records queued for probate -> DataSift upload. Populated when a VA '
    'clicks "Submit to SIFTstack" on /probate.html. Drained every 5 min '
    'by the probate-datasift-worker GitHub Actions workflow.';

-- ── Trigger: queue INSERT -> records.probate_campaign_status = 'queued' ──
create or replace function public.sync_record_probate_status_on_queue_insert()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    update public.records
    set probate_campaign_status = 'queued'
    where id = new.record_id
      and (probate_campaign_status is null or probate_campaign_status = 'failed');
    return new;
end;
$$;

drop trigger if exists trg_sync_record_probate_status_on_queue_insert
    on public.probate_campaign_queue;
create trigger trg_sync_record_probate_status_on_queue_insert
    after insert on public.probate_campaign_queue
    for each row
    execute function public.sync_record_probate_status_on_queue_insert();

-- ── Trigger: queue UPDATE (completed/failed) -> mirror onto record ───────
create or replace function public.sync_record_probate_status_on_queue_complete()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    if new.status in ('completed', 'failed')
       and new.status is distinct from old.status then
        update public.records
        set probate_campaign_status        = new.status,
            probate_campaign_completed_at  = now()
        where id = new.record_id;
    end if;
    return new;
end;
$$;

drop trigger if exists trg_sync_record_probate_status_on_queue_complete
    on public.probate_campaign_queue;
create trigger trg_sync_record_probate_status_on_queue_complete
    after update on public.probate_campaign_queue
    for each row
    execute function public.sync_record_probate_status_on_queue_complete();

-- ── RLS ──────────────────────────────────────────────────────────────────
alter table public.probate_campaign_queue enable row level security;

drop policy if exists "probate_campaign_queue: authenticated read"
    on public.probate_campaign_queue;
drop policy if exists "probate_campaign_queue: editor+ insert"
    on public.probate_campaign_queue;
drop policy if exists "probate_campaign_queue: admin update"
    on public.probate_campaign_queue;

create policy "probate_campaign_queue: authenticated read"
    on public.probate_campaign_queue
    for select
    using (auth.uid() is not null);

create policy "probate_campaign_queue: editor+ insert"
    on public.probate_campaign_queue
    for insert
    with check (
        public.current_user_role() in ('admin', 'editor')
        and queued_by = auth.uid()
    );

create policy "probate_campaign_queue: admin update"
    on public.probate_campaign_queue
    for update
    using (public.current_user_role() = 'admin')
    with check (public.current_user_role() = 'admin');

-- Service-role (worker) bypasses RLS automatically — no policy needed.
