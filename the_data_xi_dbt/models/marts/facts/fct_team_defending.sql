{{ config(materialized='table') }}
-- fct_team_defending: stg_ws_team_defending + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_defending') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
