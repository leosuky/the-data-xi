/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.whoscored_match_id,
    s.state_id,
    s.period,
    s.start_s,
    s.end_s,
    s.duration_s,
    s.home_score,
    s.away_score,
    s.home_players,
    s.away_players,
    s.trigger,
    s.home_score_diff,
    s.man_diff_home,
    s.start_minute,
    s.start_second,
    s.start_display,
    s.end_minute,
    s.end_second,
    s.end_display
from {{ source('raw','ws_game_states') }} s
