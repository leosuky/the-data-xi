{{ config(materialized='table') }}
-- One row per team. ws-anchored team_uid + provider ids + Sofascore attributes.
with nm as (
    select team_uid, max(team_name) as ws_name
    from {{ ref('stg_ws_lineups') }} group by team_uid
)
select
    x.team_uid,
    x.ws_team_id, x.sofa_team_id, x.fot_team_id,
    coalesce(s.name, nm.ws_name)            as name,
    s.country, s.city,
    s.stadium, s.stadium_capacity,
    s.founded_date
from {{ ref('int_team_xwalk') }} x
left join {{ ref('stg_sofa_teams') }} s on s.team_uid = x.team_uid
left join nm                            on nm.team_uid = x.team_uid
