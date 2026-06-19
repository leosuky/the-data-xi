/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.fotmob_id,
    s.combo_id,
    s.shot_id,
    s.event_type,
    s.team_id,
    s.player_id,
    s.player_name,
    s.x,
    s.y,
    s.minute,
    s.minute_added,
    s.period,
    s.is_blocked,
    s.is_on_target,
    s.is_own_goal,
    s.is_from_inside_box,
    s.is_saved_off_line,
    s.blocked_x,
    s.blocked_y,
    s.goal_crossed_y,
    s.goal_crossed_z,
    s.xg,
    s.xgot,
    s.shot_type,
    s.situation,
    s.keeper_id,
    s.on_goal_shot,
    px.player_uid,
    tx.team_uid
from {{ source('raw','fot_shots') }} s
left join {{ ref('int_player_xwalk') }} px on px.fot_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.fot_team_id = s.team_id
