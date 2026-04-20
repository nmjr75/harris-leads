-- 003_siftstack_status.sql
-- Track per-record SIFTstack submission status and completion timestamp.
--
-- Status values (records.siftstack_status):
--   null         → never submitted (default)
--   'queued'     → row exists in siftstack_queue, worker hasn't processed yet
--   'completed'  → worker finished enrichment successfully
--   'failed'     → worker tried but failed (see siftstack_queue.error)
--
-- Two triggers on siftstack_queue keep records.siftstack_status in sync:
--   1. On INSERT  → set records.siftstack_status = 'queued'
--   2. On UPDATE to status 'completed'/'failed' → mirror onto records
--      + stamp records.siftstack_completed_at = now()
--
-- Both triggers run SECURITY DEFINER so they bypass the editor RLS check.
-- They do NOT go through log_record_changes' revert list because we are
-- intentionally not adding siftstack_status to that list — the trigger-
-- written values must persist.

-- ── Columns ───────────────────────────────────────────────────────────
alter table public.records
    add column if not exists siftstack_status       text,
    add column if not exists siftstack_completed_at timestamptz;

comment on column public.records.siftstack_status is
    'SIFTstack submission status: null / queued / completed / failed. '
    'Maintained by triggers on siftstack_queue.';

comment on column public.records.siftstack_completed_at is
    'Timestamp set when SIFTstack worker finishes processing this record '
    '(completed or failed). Null until then.';

create index if not exists records_siftstack_status_idx
    on public.records(siftstack_status);

-- ── Trigger: queue INSERT → record status = 'queued' ─────────────────
create or replace function public.sync_record_status_on_queue_insert()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    update public.records
    set siftstack_status = 'queued'
    where id = new.record_id
      and (siftstack_status is null or siftstack_status = 'failed');
    return new;
end;
$$;

drop trigger if exists trg_sync_record_status_on_queue_insert on public.siftstack_queue;
create trigger trg_sync_record_status_on_queue_insert
    after insert on public.siftstack_queue
    for each row
    execute function public.sync_record_status_on_queue_insert();

-- ── Trigger: queue UPDATE (completed/failed) → mirror onto record ────
create or replace function public.sync_record_status_on_queue_complete()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    if new.status in ('completed', 'failed')
       and new.status is distinct from old.status then
        update public.records
        set siftstack_status       = new.status,
            siftstack_completed_at = now()
        where id = new.record_id;
    end if;
    return new;
end;
$$;

drop trigger if exists trg_sync_record_status_on_queue_complete on public.siftstack_queue;
create trigger trg_sync_record_status_on_queue_complete
    after update on public.siftstack_queue
    for each row
    execute function public.sync_record_status_on_queue_complete();
