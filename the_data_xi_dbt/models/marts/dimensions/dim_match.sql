{{ config(materialized='table') }}
-- One row per match (combo_id). Resolves teams/coaches/referee/comp/season into
-- uids so the facts stay slim. Pulls the specific columns requested per source.
select
    w.combo_id,
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
from {{ ref('stg_ws_match_meta') }} w
left join {{ ref('stg_fot_match') }}    f  on f.combo_id = w.combo_id
left join {{ ref('stg_sofa_matches') }} sm on sm.combo_id = w.combo_id
left join {{ ref('int_team_xwalk') }}   hx on hx.ws_team_id = w.home_team_id
left join {{ ref('int_team_xwalk') }}   ax on ax.ws_team_id = w.away_team_id
