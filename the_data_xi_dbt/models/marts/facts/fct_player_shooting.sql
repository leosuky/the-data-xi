{{ config(materialized='table') }}
-- WhoScored shooting detail (+SCA) + Fotmob xg/xgot rolled up per player-match.
with fot_xg as (
    select combo_id, player_uid, sum(xg) as xg, sum(xgot) as xgot
    from {{ ref('stg_fot_shots') }}
    group by combo_id, player_uid
)
select
    s.*,
    x.xg, x.xgot,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_shooting') }} s
left join fot_xg x on x.combo_id = s.combo_id and x.player_uid = s.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
