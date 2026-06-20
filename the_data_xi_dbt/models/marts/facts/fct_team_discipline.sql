{{ config(materialized='table') }}
-- fct_team_discipline: stg_ws_team_discipline + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_discipline') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
