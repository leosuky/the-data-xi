{{ config(materialized='table') }}
-- One row per match (combo_id). Resolves teams/coaches/referee/comp/season into
-- uids so the facts stay slim. Pulls the specific columns requested per source.
--
-- The combo_id spine is the UNION of every provider's match table, not just
-- WhoScored. A match exists if ANY provider has it, so a WhoScored-absent match
-- (garbage/missing matchCentreData) still gets a row — with season/competition
-- from Sofascore and date/scores from Fotmob — instead of vanishing from the
-- marts. Its WhoScored-only fields are NULL (attendance falls back to Fotmob /
-- Sofascore, see below). Team uids resolve
-- off WhoScored team ids first, then fall back to the Sofascore and Fotmob team
-- ids through int_team_xwalk, so a WhoScored-absent match still gets its teams
-- (incl. Sofascore-anchored teams WhoScored has never seen). has_ws_events stays
-- false for these
-- (that flag is a real column on ws_match_meta: true for genuine event-stream
-- loads, false for FBref-backfilled matches that lack the event texture).
-- Status columns only exist once matches are (re)loaded with the status-aware readers.
-- attendance: WhoScored first, but WhoScored sometimes stores 0 for a crowd it
-- didn't record, so a 0 falls through to Fotmob, then Sofascore. A real 0
-- (behind-closed-doors games) survives because all three then agree on 0.
{% set sm_cols = adapter.get_columns_in_relation(ref('stg_sofa_matches')) | map(attribute='name') | list %}
{% set fm_cols = adapter.get_columns_in_relation(ref('stg_fot_match'))    | map(attribute='name') | list %}
{% set att_fm = "nullif(f.attendance::text, '')::int" if 'attendance' in fm_cols else 'null::int' %}
{% set att_sm = "nullif(sm.attendance::text, '')::int" if 'attendance' in sm_cols else 'null::int' %}
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
    coalesce(hx.team_uid, hs.team_uid, hf.team_uid) as home_team_uid,
    coalesce(ax.team_uid, as_.team_uid, af.team_uid) as away_team_uid,
    sm.home_manager_id       as home_coach_uid,
    sm.away_manager_id       as away_coach_uid,
    sm.referee_id            as referee_uid,
    sm.tournament_id         as competition_uid,
    sm.season_id             as season_uid,
    -- attendance: first non-zero of WhoScored / Fotmob / Sofascore, else 0
    coalesce(
        nullif(w.attendance, 0),
        nullif({{ att_fm }}, 0),
        nullif({{ att_sm }}, 0),
        w.attendance, {{ att_fm }}, {{ att_sm }}
    ) as attendance,
    coalesce(w.has_ws_events, false) as has_ws_events,
    -- Fotmob
    f.match_round, f.home_score, f.away_score, f.match_date,
    f.stadium_name, f.stadium_city, f.stadium_capacity,
    f.home_color, f.away_color, f.home_color_dark, f.away_color_dark,
    f.home_font_color, f.away_font_color, f.home_font_dark, f.away_font_dark,
    f.has_shot_data, f.has_momentum_data,
    f.motm_id, f.motm_name, f.motm_rating,
    -- Status: Sofascore type ('finished' / 'canceled' / 'postponed'), else the
    -- Fotmob cancelled flag. Discovery skips non-played matches, so anything
    -- other than 'finished' here is a leftover from an older load.
    coalesce(
        {{ cast_col_or_null(sm_cols, 'sm', 'status_type', 'text') }},
        case when lower({{ cast_col_or_null(fm_cols, 'f', 'is_cancelled', 'text') }}) = 'true' then 'canceled' end,
        'finished'
    ) as match_status,
    -- Sofascore
    sm.winnercode, sm.has_shotmap, sm.has_xg,
    sm.home_formation, sm.away_formation
from match_keys k
left join {{ ref('stg_ws_match_meta') }} w  on w.combo_id  = k.combo_id
left join {{ ref('stg_fot_match') }}     f  on f.combo_id  = k.combo_id
left join {{ ref('stg_sofa_matches') }}  sm on sm.combo_id = k.combo_id
left join {{ ref('int_team_xwalk') }}    hx  on hx.ws_team_id   = w.home_team_id
left join {{ ref('int_team_xwalk') }}    ax  on ax.ws_team_id   = w.away_team_id
left join {{ ref('int_team_xwalk') }}    hs  on hs.sofa_team_id = sm.home_team_id
left join {{ ref('int_team_xwalk') }}    as_ on as_.sofa_team_id = sm.away_team_id
left join {{ ref('int_team_xwalk') }}    hf  on hf.fot_team_id  = f.home_team_id
left join {{ ref('int_team_xwalk') }}    af  on af.fot_team_id  = f.away_team_id
