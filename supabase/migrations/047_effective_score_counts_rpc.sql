-- 047_effective_score_counts_rpc.sql
-- Server-side aggregation of effective HOT/WARM/COOL/COLD/DEAD counts.
-- Combines ghl_latest_scores (AI scorer output) with ghl_score_overrides
-- (Nelson's manual corrections) via a coalesce — override wins. Returns
-- one row per effective_score value with the count.
--
-- Why this RPC instead of pulling rows to the browser:
--
-- 1. Supabase's REST layer enforces a max-rows cap on SELECT queries.
--    Even .range(0, 99999) from the JS client is subject to it. A view
--    with > max-rows total rows (ghl_latest_scores currently has 1127)
--    silently returns a truncated subset, so any client-side aggregation
--    is wrong by an unbounded amount.
--
-- 2. With server-side GROUP BY, the result set is at most 5 rows (one
--    per score value). No row cap can ever apply. The browser receives
--    exactly the answer it needs.
--
-- 3. The top-card count and the filter-pill count now BOTH read from this
--    same SQL — divergence becomes architecturally impossible.
--
-- SECURITY INVOKER (the SQL default): function runs as the calling user,
-- so RLS on ghl_latest_scores still scopes results per-role. Owner sees
-- everything; Joseph/Walter (assignees) see only their assigned contacts'
-- counts. Matches the prior loadStats behavior, just without the cap.

create or replace function public.get_effective_score_counts()
returns table(effective_score text, n int)
language sql
stable
security invoker
set search_path = public
as $$
    select
        coalesce(o.score, s.score)::text as effective_score,
        count(*)::int                    as n
    from public.ghl_latest_scores s
    left join public.ghl_score_overrides o on o.contact_id = s.contact_id
    group by 1;
$$;

grant execute on function public.get_effective_score_counts() to authenticated;

comment on function public.get_effective_score_counts() is
    'Returns one row per effective-score bucket (HOT/WARM/COOL/COLD/DEAD) '
    'with the count of contacts in that bucket. Override wins over AI '
    'score. Runs SECURITY INVOKER so per-role RLS on ghl_latest_scores '
    'continues to scope what each user sees.';
