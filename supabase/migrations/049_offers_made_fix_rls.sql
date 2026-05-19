-- 049_offers_made_fix_rls.sql
-- Migration 048 used inline subqueries against auth.users inside the
-- offers_made RLS policies (e.g. `select email from auth.users where
-- id = auth.uid()`). Authenticated users don't have SELECT on auth.users,
-- so every insert / update from the dashboard failed with
-- "permission denied for table users".
--
-- This is the SAME bug pattern that hit migration 045 (fixed in 046).
-- The fix is the SAME pattern: a SECURITY DEFINER helper that owns the
-- auth.users read, returning the value the policy needs.
--
-- Adds public.auth_caller_email() — a generic helper that returns the
-- calling user's lowercased email — and rewrites the two write policies
-- on offers_made to call it instead of doing the read inline.

create or replace function public.auth_caller_email()
returns text
language sql
security definer
stable
set search_path = public, auth
as $$
    select lower(u.email)
    from auth.users u
    where u.id = auth.uid();
$$;

grant execute on function public.auth_caller_email() to authenticated;

comment on function public.auth_caller_email() is
    'Returns the lowercased email of the calling auth user. SECURITY '
    'DEFINER so it can read auth.users on behalf of authenticated callers '
    'inside RLS policies. Use this anywhere a policy needs to compare to '
    'the caller''s email.';

-- INSERT policy: caller must be Nelson (owner), admin/editor profile, or
-- the contact's current assignee.
drop policy if exists "offers_made insert by author or manager" on public.offers_made;
create policy "offers_made insert by author or manager"
    on public.offers_made
    for insert
    to authenticated
    with check (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = public.auth_caller_email()
        )
    );

-- UPDATE policy: outcome edits allowed by the original author, the
-- contact's current assignee, or owner/admin.
drop policy if exists "offers_made update by author or manager" on public.offers_made;
create policy "offers_made update by author or manager"
    on public.offers_made
    for update
    to authenticated
    using (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or lower(agent_email) = public.auth_caller_email()
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = public.auth_caller_email()
        )
    )
    with check (
        public.is_nelson_caller()
        or exists (
            select 1 from public.profiles p
            where p.id = auth.uid()
              and lower(p.role) in ('admin', 'editor')
        )
        or lower(agent_email) = public.auth_caller_email()
        or exists (
            select 1 from public.ghl_current_assignment a
            where a.contact_id = offers_made.contact_id
              and lower(a.assigned_to_email) = public.auth_caller_email()
        )
    );
