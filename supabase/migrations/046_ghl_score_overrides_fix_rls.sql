-- 046_ghl_score_overrides_fix_rls.sql
-- Migration 045's RLS used an inline subquery against auth.users to check
-- the caller's email. Authenticated users don't have SELECT on auth.users
-- by default in Supabase, so RLS evaluation failed with
-- "permission denied for table users" and Nelson couldn't write overrides.
--
-- Fix: SECURITY DEFINER helper. The function runs with the owner's
-- privileges (so it CAN read auth.users), the helper returns a boolean
-- the RLS policy can check, and only the helper executes inside auth.
-- Same pattern as caller_is_owner() in migration 032.

create or replace function public.is_nelson_caller()
returns boolean
language sql
security definer
stable
set search_path = public, auth
as $$
    select coalesce(
        (select lower(u.email) = any(array[
            'nmjr75@gmail.com',
            'nelson@htxpropertybuyers.com'
         ])
         from auth.users u
         where u.id = auth.uid()),
        false
    );
$$;

grant execute on function public.is_nelson_caller() to authenticated;

comment on function public.is_nelson_caller() is
    'Returns true iff the calling user is Nelson (owner). Uses '
    'SECURITY DEFINER so authenticated callers can check without needing '
    'SELECT on auth.users themselves. Use this in RLS policies instead of '
    'inline auth.users subqueries.';

-- Replace the broken write policy from migration 045.
drop policy if exists "score_overrides nelson writes" on public.ghl_score_overrides;
create policy "score_overrides nelson writes"
    on public.ghl_score_overrides
    for all
    to authenticated
    using (public.is_nelson_caller())
    with check (public.is_nelson_caller());
