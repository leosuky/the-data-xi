-- Shot-count reconciler, now PER TEAM. Flags a (match, team) where a provider's
-- shot count disagrees with WhoScored (only when that provider has shots for the
-- team, so a missing provider doesn't fire); for WhoScored-absent matches it
-- compares Fotmob against Sofascore instead. Team-based alignment depends on
-- equal per-team counts, so this is the precise tripwire for membership drift
-- (e.g. a provider booking an own goal to a different team). severity=warn.
{{ config(severity='warn') }}
with c as (
    select combo_id, team_uid,
        count(*) filter (where src='ws')   as ws_n,
        count(*) filter (where src='fot')  as fot_n,
        count(*) filter (where src='sofa') as sofa_n
    from (
        select combo_id, team_uid, 'ws'   as src from {{ ref('stg_ws_shots') }}
        union all select combo_id, team_uid, 'fot'  from {{ ref('stg_fot_shots') }}
        union all select combo_id, team_uid, 'sofa' from {{ ref('stg_sofa_shots') }}
    ) u
    where team_uid is not null
    group by combo_id, team_uid
)
select * from c
where (ws_n > 0 and fot_n  > 0 and ws_n <> fot_n)
   or (ws_n > 0 and sofa_n > 0 and ws_n <> sofa_n)
   -- WhoScored-absent match (e.g. Saudi): reconcile the other two providers
   or (ws_n = 0 and fot_n > 0 and sofa_n > 0 and fot_n <> sofa_n)
