{{ config(materialized='table') }}
-- fct_player_avg_positions: each player's average on-ball position per match
-- (stg_ws_avg_positions) + season/competition/date for filtering. The nodes of
-- a pass network (edges: fct_pass_network).
select
    s.*,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_avg_positions') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
