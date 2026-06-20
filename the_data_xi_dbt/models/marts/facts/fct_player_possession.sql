{{ config(materialized='table') }}
-- fct_player_possession: stg_ws_player_possession + denormalized season/competition/date for direct filtering.
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_possession') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
