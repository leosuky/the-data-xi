/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.whoscored_match_id,
    s.team_id,
    s.state_id,
    s.score_diff_at,
    s.man_state_at,
    s.period,
    s.minute,
    s.second,
    s.minute_display,
    s.time_s,
    s.event_type,
    s.player_id,
    s.player_name,
    s.player_in_id,
    s.player_in_name,
    s.player_out_id,
    s.player_out_name,
    s.detail,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_context_events') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
