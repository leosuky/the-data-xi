/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.player_id,
    s.player_name,
    s.touches,
    s.touches_own_third,
    s.touches_middle_third,
    s.touches_final_third,
    s.touches_in_box,
    s.avg_touch_x,
    s.dribbles_attempted,
    s.dribbles_won,
    s.dribbles_lost,
    s.dribble_success_pct,
    s.dispossessed,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_player_possession') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
