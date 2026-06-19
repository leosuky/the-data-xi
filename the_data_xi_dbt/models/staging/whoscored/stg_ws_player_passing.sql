/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.player_id,
    s.player_name,
    s.passes_total,
    s.passes_accurate,
    s.passes_inaccurate,
    s.pass_accuracy_pct,
    s.longballs_attempted,
    s.longballs_accurate,
    s.longball_accuracy_pct,
    s.throughballs,
    s.head_passes,
    s.layoffs,
    s.crosses_attempted,
    s.crosses_accurate,
    s.key_passes,
    s.assists,
    s.big_chances_created,
    s.passes_entering_final_third,
    s.passes_in_final_third,
    s.passes_in_final_third_accurate,
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_player_passing') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
