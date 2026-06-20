{{ config(materialized='table') }}
-- One row per player. ws-anchored uid + all 3 provider ids + SCD position
-- (most-recent granular starting position, usual_position fallback).
with sofa as (
    select player_uid, name, birth_date, nationality, position as sofa_position
    from {{ ref('stg_sofa_players') }}
),
ws_name as (
    select player_uid, max(player_name) as ws_name
    from {{ ref('stg_ws_lineups') }} group by player_uid
),
-- latest granular position (fot position, else usual_position) by match_date
pos as (
    select distinct on (l.player_uid)
        l.player_uid,
        coalesce(l.position, l.usual_position) as position
    from {{ ref('stg_fot_lineups') }} l
    join {{ ref('dim_match') }} dm on dm.combo_id = l.combo_id
    where coalesce(l.position, l.usual_position) is not null
    order by l.player_uid, dm.match_date desc nulls last
),
-- most recent team
team as (
    select distinct on (l.player_uid) l.player_uid, l.team_uid
    from {{ ref('stg_ws_lineups') }} l
    join {{ ref('dim_match') }} dm on dm.combo_id = l.combo_id
    order by l.player_uid, dm.match_date desc nulls last
)
select
    x.player_uid,
    x.ws_player_id, x.sofa_player_id, x.fot_player_id,
    coalesce(s.name, wn.ws_name)         as full_name,
    s.birth_date, s.nationality,
    coalesce(p.position, s.sofa_position) as position,
    t.team_uid                            as most_recent_team_uid
from {{ ref('int_player_xwalk') }} x
left join sofa    s  on s.player_uid  = x.player_uid
left join ws_name wn on wn.player_uid = x.player_uid
left join pos     p  on p.player_uid  = x.player_uid
left join team    t  on t.player_uid  = x.player_uid
