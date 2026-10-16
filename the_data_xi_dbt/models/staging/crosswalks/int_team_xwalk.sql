{{ config(materialized='table') }}
/*
  int_team_xwalk - ws-anchored team identity, with a Sofascore fallback.
  team_uid = WhoScored team_id (alias-overridable). Resolution is trivial:
  within a match, is_home_team maps the two teams across all providers; vote
  globally so a one-off bad match can't corrupt a club's id.

  Fallback: a team WhoScored has never seen (e.g. the whole Saudi Pro League)
  is anchored on its Sofascore id instead: team_uid = -sofa_team_id (negative,
  so it can never collide with a WhoScored id), ws_team_id NULL, and its Fotmob
  id voted against Sofascore the same way. Existing ws-anchored uids never move.

  Guard worth adding as a dbt test: each (combo_id) should yield exactly two
  is_home_team values across each provider - a home/away flip would mismap a
  whole match cleanly and silently.
*/

with ws as (
    select distinct combo_id, is_home_team, team_id as ws_id
    from {{ source('raw', 'ws_lineups') }}
),
sofa as (
    select distinct combo_id, is_home_team, team_id as prov_id
    from {{ source('raw', 'sofa_lineups') }}
),
fot as (
    select distinct combo_id, is_home_team, team_id as prov_id
    from {{ source('raw', 'fot_lineups') }}
),



{{ xwalk_vote_team('sofa') }},
{{ xwalk_vote_team('fot') }},

ws_universe as (select distinct ws_id from ws),
joined as (
    select u.ws_id as ws_team_id, s.prov_id as sofa_team_id, f.prov_id as fot_team_id
    from ws_universe u
    left join vote_sofa s on s.ws_id=u.ws_id and s.rn=1
    left join vote_fot  f on f.ws_id=u.ws_id and f.rn=1
),

-- ── Sofascore fallback anchor: teams with no WhoScored mapping ───────────────
sofa_only as (
    select distinct prov_id as sofa_id from sofa
    where prov_id not in (select sofa_team_id from joined where sofa_team_id is not null)
),
vote_sofa_fot as (
    select s.prov_id as sofa_id, f.prov_id as fot_id,
           row_number() over (partition by s.prov_id order by count(*) desc, f.prov_id) rn
    from sofa s join fot f using (combo_id, is_home_team)
    where s.prov_id in (select sofa_id from sofa_only)
      and f.prov_id not in (select fot_team_id from joined where fot_team_id is not null)
    group by s.prov_id, f.prov_id
)

select
    coalesce(a.canonical_ws_id, j.ws_team_id) as team_uid,
    j.ws_team_id, j.sofa_team_id, j.fot_team_id
from joined j
left join {{ ref('team_alias') }} a on a.bad_ws_id = j.ws_team_id

union all

select
    -so.sofa_id     as team_uid,
    null::bigint    as ws_team_id,
    so.sofa_id      as sofa_team_id,
    v.fot_id        as fot_team_id
from sofa_only so
left join vote_sofa_fot v on v.sofa_id = so.sofa_id and v.rn = 1
