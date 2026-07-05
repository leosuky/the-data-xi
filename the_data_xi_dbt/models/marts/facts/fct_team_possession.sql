{{ config(materialized='table') }}
-- fct_team_possession: ws possession + ws poss_adv (unique cols) + ws carries.
select
    b.*,
    {{ select_new_columns(ref('stg_ws_team_poss_adv'), ref('stg_ws_team_possession'), 'a') }},
    cc.carries, cc.carry_distance_m, cc.progressive_carries, cc.carry_final_third_entries,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_possession') }} b
left join {{ ref('stg_ws_team_poss_adv') }} a  on a.combo_id  = b.combo_id and a.team_uid  = b.team_uid
left join {{ ref('stg_ws_team_carries') }}  cc on cc.combo_id = b.combo_id and cc.team_uid = b.team_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = b.combo_id
