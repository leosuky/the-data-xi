{{ config(materialized='table') }}
-- Slim per-(player,match) spine: identity, minutes, started, captain, and
-- position from ALL THREE providers (decision #6). Stores BOTH ratings,
-- prefixed: fot_rating (fot_lineups.rating) + sofa_rating (sofa match_rating).
-- match_rating is a dynamic sofa stat column, so reference it only if present.
{% set sps_cols = adapter.get_columns_in_relation(ref('stg_sofa_player_stats'))
                  | map(attribute='name') | list %}
with ws as (
    select combo_id, player_uid, team_uid, player_name, is_home_team,
           is_starter, is_captain, position as ws_position,
           subbed_in_minute, subbed_out_minute
    from {{ ref('stg_ws_lineups') }}
),
fot as (
    select combo_id, player_uid, position as fot_position,
           usual_position as fot_usual_position,
           position_id as fot_position_id, usual_position_id as fot_usual_position_id,
           rating as fot_rating
    from {{ ref('stg_fot_lineups') }}
),
sofa as (
    select combo_id, player_uid, position as sofa_position, minutes_played
    from {{ ref('stg_sofa_lineups') }}
),
sofa_rating as (
    select combo_id, player_uid,
           {% if 'match_rating' in sps_cols %}match_rating{% else %}null::numeric{% endif %} as sofa_rating
    from {{ ref('stg_sofa_player_stats') }}
)
select
    w.combo_id, w.player_uid, w.team_uid,
    dm.season_uid, dm.competition_uid, dm.match_date,
    w.player_name, w.is_home_team, w.is_starter, w.is_captain,
    s.minutes_played, w.subbed_in_minute, w.subbed_out_minute,
    w.ws_position, s.sofa_position, f.fot_position, f.fot_usual_position,
    f.fot_position_id, f.fot_usual_position_id,
    f.fot_rating,
    sr.sofa_rating
from ws w
left join fot         f  on f.combo_id  = w.combo_id and f.player_uid  = w.player_uid
left join sofa        s  on s.combo_id  = w.combo_id and s.player_uid  = w.player_uid
left join sofa_rating sr on sr.combo_id = w.combo_id and sr.player_uid = w.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = w.combo_id
