-- 045_ghl_score_overrides.sql
-- Owner-only manual score override. The AI scorer (motivation_scorer.py)
-- writes append-only rows to ghl_motivation_scores; this table is a
-- separate, single-row-per-contact override surface. Display reads the
-- override first and falls back to the AI score. Clearing the override
-- = deleting the row, after which the AI score is shown again.
--
-- Why a separate table (not a column on motivation_scores or a manual
-- row in motivation_scores): keeps the AI scorer's audit trail clean and
-- append-only, and makes "revert to AI score" a trivial DELETE.

create table if not exists public.ghl_score_overrides (
    contact_id     text        primary key,
    score          text        not null
                                check (score in ('HOT','WARM','COOL','COLD','DEAD')),
    reason         text,
    set_by_email   text        not null,
    set_at         timestamptz not null default now()
);

alter table public.ghl_score_overrides enable row level security;

-- Everyone authenticated can read so the dashboard renders the same
-- override-aware score for every role. RLS on write is the protection.
drop policy if exists "score_overrides read all authenticated" on public.ghl_score_overrides;
create policy "score_overrides read all authenticated"
    on public.ghl_score_overrides
    for select
    to authenticated
    using (true);

-- Only Nelson can insert / update / delete. Inline email allowlist
-- matches the pattern in migration 044 (get_team_users_email_gate) and
-- the dashboard's NELSON_EMAILS constant. profiles.role is unreliable
-- as an owner gate per the team-panel fix.
drop policy if exists "score_overrides nelson writes" on public.ghl_score_overrides;
create policy "score_overrides nelson writes"
    on public.ghl_score_overrides
    for all
    to authenticated
    using (
        coalesce(
            (select lower(email) from auth.users where id = auth.uid())
                = any(array['nmjr75@gmail.com', 'nelson@htxpropertybuyers.com']),
            false
        )
    )
    with check (
        coalesce(
            (select lower(email) from auth.users where id = auth.uid())
                = any(array['nmjr75@gmail.com', 'nelson@htxpropertybuyers.com']),
            false
        )
    );

-- Add to realtime publication so dashboard tabs stay in sync across
-- browsers when Nelson edits a score. Wrapped in a DO block because
-- adding an already-published table raises an error.
do $$
begin
    if not exists (
        select 1 from pg_publication_tables
        where pubname = 'supabase_realtime'
          and schemaname = 'public'
          and tablename = 'ghl_score_overrides'
    ) then
        alter publication supabase_realtime add table public.ghl_score_overrides;
    end if;
end$$;

comment on table public.ghl_score_overrides is
    'Owner-only manual score corrections. Dashboard renders override.score in '
    'place of the AI scorer''s value when a row exists for a contact_id. '
    'Delete the row to revert to the AI score.';
