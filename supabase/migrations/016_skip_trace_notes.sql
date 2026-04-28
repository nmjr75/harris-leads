-- 016_skip_trace_notes.sql
-- Add a free-form notes column to skip_trace_jobs so VAs can leave
-- multi-line context (leads, follow-ups, source notes) separate from
-- the existing single-line "comment" field.

alter table public.skip_trace_jobs
    add column if not exists notes text;

comment on column public.skip_trace_jobs.notes is
    'Free-form VA notes about this skip-trace search. Multi-line, '
    'distinct from the one-line "comment" field. Used by /leads.html '
    'skip-trace panel.';
