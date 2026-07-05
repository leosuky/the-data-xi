{{ config(materialized='table') }}
-- One row per match (combo_id). Resolves teams/coaches/referee/comp/season into
-- uids so the facts stay slim. Pulls the specific columns requested per source.
--
-- The combo_id spine is the UNION of every provider's match table, not just
-- WhoScored. A match exists if ANY provider has it, so a WhoScored-absent match
-- (garbage/missing matchCentreData) still gets a row — with season/competition
-- from Sofascore and date/scores from Fotmob — instead of vanishing from the
-- marts. Its WhoScored-derived fields (attendance, home/away team_uid resolved
-- off ws team ids) are NULL unless an FBref backfill supplied a ws_match_meta
-- row for it — in which case team_uids resolve but has_ws_events is still false
-- (that flag is a real column on ws_match_meta: true for genuine event-stream
-- loads, false for FBref-backfilled matches that lack the event texture).
with match_keys as (
    select combo_id from {{ ref('stg_ws_match_meta') }}
    union
    select combo_id from {{ ref('stg_fot_match') }}
    union
    select combo_id from {{ ref('stg_sofa_matches') }}
)
select
    k.combo_id,
    -- resolved keys
    hx.team_uid              as home_team_uid,
    ax.team_uid              as away_team_uid,
    sm.home_manager_id       as home_coach_uid,
    sm.away_manager_id       as away_coach_uid,
    sm.referee_id            as referee_uid,
    sm.tournament_id         as competition_uid,
    sm.season_id             as season_uid,
    -- WhoScored
    w.attendance,
    coalesce(w.has_ws_events, false) as has_ws_events,
    -- Fotmob
    f.match_round, f.home_score, f.away_score, f.match_date,
    f.stadium_name, f.stadium_city, f.stadium_capacity,
    f.home_color, f.away_color, f.home_color_dark, f.away_color_dark,
    f.home_font_color, f.away_font_color, f.home_font_dark, f.away_font_dark,
    f.has_shot_data, f.has_momentum_data,
    f.motm_id, f.motm_name, f.motm_rating,
    -- Sofascore
    sm.winnercode, sm.has_shotmap, sm.has_xg,
    sm.home_formation, sm.away_formation
from match_keys k
left join {{ ref('stg_ws_match_meta') }} w  on w.combo_id  = k.combo_id
left join {{ ref('stg_fot_match') }}     f  on f.combo_id  = k.combo_id
left join {{ ref('stg_sofa_matches') }}  sm on sm.combo_id = k.combo_id
left join {{ ref('int_team_xwalk') }}    hx on hx.ws_team_id = w.home_team_id
left join {{ ref('int_team_xwalk') }}    ax on ax.ws_team_id = w.away_team_id
