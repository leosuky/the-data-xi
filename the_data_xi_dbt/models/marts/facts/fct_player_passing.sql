{{ config(materialized='table') }}
-- fct_player_passing: stg_ws_player_passing + stg_ws_player_passing_adv (only its unique columns), joined on (combo_id, player_uid).
select
    b.*,
    {{ select_new_columns(ref('stg_ws_player_passing_adv'), ref('stg_ws_player_passing'), 'a') }},
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_passing') }} b
left join {{ ref('stg_ws_player_passing_adv') }} a on a.combo_id = b.combo_id and a.player_uid = b.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = b.combo_id
