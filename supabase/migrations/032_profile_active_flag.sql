-- 032_profile_active_flag.sql
-- Adds an is_active toggle to profiles so Nelson (owner) can temporarily
-- suspend any VA's dashboard access from the new Access Control modal on
-- /admin.html. Combined with the JS check in admin.html, a suspended user
-- is force-signed-out and shown an "Account suspended" screen on next
-- page load (within 60s — the dashboard auto-refresh tick).
--
-- Hardens via two layers:
--   1. RLS — only owner can UPDATE is_active / suspended_at / suspended_by.
--   2. JS — dashboard refuses to render mainUI when profiles.is_active = false.

alter table public.profiles
    add column if not exists is_active     boolean not null default true,
    add column if not exists suspended_at  timestamptz,
    add column if not exists suspended_by  uuid references auth.users(id);

-- SECURITY DEFINER helper that returns true iff the caller is the owner.
-- Avoids RLS recursion when an owner UPDATE on profiles needs to check
-- profiles.role for the calling user.
create or replace function public.caller_is_owner()
returns boolean
language sql security definer stable
set search_path = public
as $$
    select coalesce(
        (select role = 'owner' from public.profiles where id = auth.uid()),
        false
    );
$$;
grant execute on function public.caller_is_owner() to authenticated;

-- UPDATE policy: only the owner can flip is_active / suspended_*.
-- Other profile fields are governed by whatever policies already exist
-- on the table (mig 022 dropped open-read but didn't lock writes).
drop policy if exists "profiles owner can suspend users" on public.profiles;
create policy "profiles owner can suspend users"
    on public.profiles
    for update
    to authenticated
    using (public.caller_is_owner())
    with check (public.caller_is_owner());

-- Read policy: every authenticated user must be able to read their OWN
-- is_active row so the JS gate works. Owner also needs to read everyone
-- to drive the Access Control modal.
drop policy if exists "profiles self read" on public.profiles;
create policy "profiles self read"
    on public.profiles
    for select
    to authenticated
    using (id = auth.uid() or public.caller_is_owner());

comment on column public.profiles.is_active is
    'False = dashboard access suspended. Set by owner from the Access '
    'Control modal. JS gate in admin.html signs the user out + shows '
    'an "Account suspended" screen when this is false.';

-- ─────────────────────────────────────────────────────────────────
-- Immediate effect: suspend Joseph right now.
-- ─────────────────────────────────────────────────────────────────
update public.profiles
   set is_active    = false,
       suspended_at = now(),
       suspended_by = (select id from auth.users
                       where lower(email) = 'nmjr75@gmail.com'
                       limit 1)
 where id = (select id from auth.users
             where lower(email) = 'joseph.hhremodeling@gmail.com'
             limit 1);
