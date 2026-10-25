{{ config(materialized='table') }}
-- fct_pass_network: directed passer -> receiver counts of accurate passes, per
-- team-match (stg_ws_pass_network), with both players mapped to player_uid and
-- season/competition/date denormalised for filtering. Pair with
-- fct_player_avg_positions to draw a pass network.
select
    s.*,
    coalesce(pp.player_uid, s.passer_id)   as passer_uid,
    coalesce(pr.player_uid, s.receiver_id) as receiver_uid,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_pass_network') }} s
left join {{ ref('int_player_xwalk') }} pp on pp.ws_player_id = s.passer_id
left join {{ ref('int_player_xwalk') }} pr on pr.ws_player_id = s.receiver_id
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
