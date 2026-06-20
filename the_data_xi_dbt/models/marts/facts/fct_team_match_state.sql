{{ config(materialized='table') }}
-- fct_team_match_state: stg_ws_team_state_agg + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_state_agg') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
