/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.player_id,
    s.player_name,
    s.progressive_passes,
    s.passes_entering_final_third,
    s.penalty_area_passes,
    s.avg_pass_length,
    s.avg_vertical_gain,
    s.forward_pass_pct,
    s.backward_pass_pct,
    s.xt_total,
    s.xt_positive,
    s.xt_per_pass,
    s.key_passes,
    s.assists,
    s.chances_created,
    s.avg_hold_seconds,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_player_passing_adv') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
