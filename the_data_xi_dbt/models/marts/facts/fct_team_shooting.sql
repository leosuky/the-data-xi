{{ config(materialized='table') }}
-- fct_team_shooting: stg_ws_team_shooting + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_shooting') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
