/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.player_id,
    s.player_name,
    s.avg_x,
    s.avg_y,
    s.touch_count,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_avg_positions') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
