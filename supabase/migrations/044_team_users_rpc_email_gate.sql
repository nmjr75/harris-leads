-- 044_team_users_rpc_email_gate.sql
-- Replaces the get_team_users() owner gate. Migration 043 used
-- caller_is_owner(), which checks profiles.role = 'owner' — but the
-- dashboard's NELSON_EMAILS allowlist and the profiles.role values are
-- out of sync (Nelson's profile row currently has role='admin', not
-- 'owner'). The result: the RPC raised 'forbidden' and the dashboard
-- silently fell back to a profiles read that doesn't include email or
-- last_sign_in_at. Symptom: every row in the Access Control modal
-- showed "(no email)" and "Never signed in".
--
-- Fix: gate on auth.users.email against the same allowlist the
-- dashboard JS uses. Adjust the array below if Nelson ever changes
-- his canonical login email.
--
-- CREATE OR REPLACE — safe to re-run; same function name, same return
-- shape, only the gate changes.

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
declare
    caller_email text;
    owner_emails text[] := array[
        'nmjr75@gmail.com',
        'nelson@htxpropertybuyers.com'
    ];
begin
    select lower(u.email) into caller_email
    from auth.users u
    where u.id = auth.uid();

    if caller_email is null or not (caller_email = any(owner_emails)) then
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
        select count(*)::int as cnt
        from public.ghl_current_assignment
        where lower(assigned_to_email) = lower(u.email)
    ) la on true
    order by p.display_name nulls last, u.email;
end;
$$;

grant execute on function public.get_team_users() to authenticated;
