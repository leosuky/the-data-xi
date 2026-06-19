/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.fotmob_id,
    s.combo_id,
    s.player_id,
    s.player_name,
    s.team_id,
    s.team_name,
    s.is_home_team,
    s.role,
    s.shirt_number,
    s.position_id,
    s.position,
    s.usual_position_id,
    s.usual_position,
    s.is_captain,
    s.age,
    s.country,
    s.country_code,
    s.rating,
    s.formation,
    px.player_uid,
    tx.team_uid
from {{ source('raw','fot_lineups') }} s
left join {{ ref('int_player_xwalk') }} px on px.fot_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.fot_team_id = s.team_id
