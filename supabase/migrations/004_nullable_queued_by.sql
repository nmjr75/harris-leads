-- 004_nullable_queued_by.sql
-- Make siftstack_queue.queued_by nullable so the scraper (running under
-- service_role) can auto-queue records without a human user ID.
--
-- Dashboard button inserts still set queued_by = auth.uid() via the
-- existing RLS policy — service_role bypasses RLS entirely and can
-- insert null queued_by for auto-queued rows.

alter table public.siftstack_queue
    alter column queued_by drop not null;

comment on column public.siftstack_queue.queued_by is
    'User who queued the record. Null when auto-queued by the scraper.';
