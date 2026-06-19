/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.home_team,
    s.home_team_id,
    s.away_team,
    s.away_team_id,
    s.score,
    s.ht_score,
    s.venue,
    s.attendance,
    s.referee,
    s.start_time
from {{ source('raw','ws_match_meta') }} s
