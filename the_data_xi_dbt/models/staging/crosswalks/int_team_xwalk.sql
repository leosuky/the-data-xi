{{ config(materialized='table') }}
/*
  int_team_xwalk - ws-anchored team identity.
  team_uid = WhoScored team_id (alias-overridable). Resolution is trivial:
  within a match, is_home_team maps the two teams across all providers; vote
  globally so a one-off bad match can't corrupt a club's id.

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
)

select
    coalesce(a.canonical_ws_id, j.ws_team_id) as team_uid,
    j.ws_team_id, j.sofa_team_id, j.fot_team_id
from joined j
left join {{ ref('team_alias') }} a on a.bad_ws_id = j.ws_team_id
