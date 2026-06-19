/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.player_id,
    s.player_name,
    s.fouls_committed,
    s.fouls_won,
    s.fouls_defensive,
    s.fouls_offensive,
    s.fouls_aerial,
    s.fouls_own_box,
    s.fouls_own_half,
    s.fouls_midfield,
    s.fouls_danger_area,
    s.fouls_first_half,
    s.fouls_second_half,
    s.fouls_late_game,
    s.yellow_cards,
    s.red_cards,
    s.second_yellow_cards,
    s.offsides_committed,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_player_discipline') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
