/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.player_id,
    s.player_name,
    s.team_id,
    s.team_name,
    s.is_home_team,
    s.shirt_number,
    s.position,
    s.is_starter,
    s.is_captain,
    s.is_man_of_the_match,
    s.age,
    s.height_cm,
    s.weight_kg,
    s.subbed_in_minute,
    s.subbed_in_for,
    s.subbed_out_minute,
    s.subbed_out_for,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_lineups') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
