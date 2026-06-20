{{ config(materialized='table') }}
-- fct_player_goalkeeping: stg_ws_player_goalkeep + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_goalkeep') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
