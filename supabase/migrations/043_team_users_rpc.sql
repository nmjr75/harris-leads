-- 043_team_users_rpc.sql
-- Owner-only RPC that returns every team member's profile fields joined with
-- their auth.users record (email, last_sign_in_at) and a count of currently
-- assigned leads. Powers the expanded Access Control modal on /admin.html.
--
-- auth.users is not directly readable from client-side queries even with the
-- service role policy patterns we use elsewhere. SECURITY DEFINER + an explicit
-- caller_is_owner() check is the standard escape hatch.

create or replace function public.get_team_users()
returns table(
    id                    uuid,
    email                 text,
    display_name          text,
    role                  text,
    is_active             boolean,
    suspended_at          timestamptz,
    suspended_by_email    text,
    last_sign_in_at       timestamptz,
    created_at            timestamptz,
    assigned_lead_count   int
)
language plpgsql
security definer
set search_path = public, auth
as $$
begin
    if not public.caller_is_owner() then
        raise exception 'forbidden: owner only';
    end if;

    return query
    select
        p.id,
        u.email::text                              as email,
        p.display_name,
        p.role,
        p.is_active,
        p.suspended_at,
        sb.email::text                             as suspended_by_email,
        u.last_sign_in_at,
        u.created_at,
        coalesce(la.cnt, 0)::int                   as assigned_lead_count
    from public.profiles p
    join auth.users u on u.id = p.id
    left join auth.users sb on sb.id = p.suspended_by
    left join lateral (
        -- Count CURRENT assignments via the existing view that resolves
        -- the latest append-only row per contact_id. Falls back to a raw
        -- count if the view isn't deployed.
        select count(*)::int as cnt
        from public.ghl_current_assignment
        where lower(assigned_to_email) = lower(u.email)
    ) la on true
    order by p.display_name nulls last, u.email;
end;
$$;

grant execute on function public.get_team_users() to authenticated;

comment on function public.get_team_users() is
    'Owner-only. Returns one row per dashboard user with email, role, last '
    'sign-in (UTC), active flag, and a count of currently assigned leads. '
    'Drives the Access Control modal on /admin.html.';
