{{ config(materialized='table') }}
-- fct_player_possession: ws possession + ws carries, denormalized season keys.
select
    s.*,
    c.carries, c.carry_distance_m, c.progressive_carries, c.carry_final_third_entries,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_possession') }} s
left join {{ ref('stg_ws_player_carries') }} c
    on c.combo_id = s.combo_id and c.player_uid = s.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
