/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.match_id,
    s.tournament_id,
    s.season_id,
    s.venue,
    s.referee_id,
    s.home_team_id,
    s.away_team_id,
    s.home_manager_id,
    s.away_manager_id,
    s.home_formation,
    s.away_formation,
    s.attendance,
    s.winnercode,
    s.starttimestamp,
    s.motm_rating,
    s.motm_player_name,
    s.motm_player_id,
    s.has_shotmap,
    s.has_xg
from {{ source('raw','sofa_matches') }} s
