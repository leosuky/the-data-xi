/* trivial tier (team-grain): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.whoscored_match_id,
    s.state_id,
    s.team_id,
    s.opponent_id,
    s.is_home,
    s.period,
    s.start_s,
    s.end_s,
    s.duration_s,
    s.start_minute,
    s.end_minute,
    s.start_display,
    s.end_display,
    s.score_for,
    s.score_against,
    s.score_diff,
    s.score_bucket,
    s.man_diff,
    s.man_state,
    s.trigger,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_team_states') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
