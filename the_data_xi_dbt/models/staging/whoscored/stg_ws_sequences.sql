/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.whoscored_match_id,
    s.team_id,
    s.possession_id,
    s.length,
    s.duration_s,
    s.start_x,
    s.start_y,
    s.end_x,
    s.end_y,
    s.n_passes,
    s.is_open_play,
    s.is_shot,
    s.is_ten_plus_pass,
    s.is_build_up_attack,
    s.is_direct_attack,
    s.is_high_turnover,
    s.outcome,
    s.event_id_start,
    s.event_id_end,
    s.player_id_start,
    s.player_id_end,
    s.player_ids,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_sequences') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
