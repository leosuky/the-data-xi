{{ config(materialized='table') }}
-- WhoScored shooting detail (+SCA) + Fotmob xg/xgot + Data XI xG/xGOT rolled up per player-match.
-- PROVIDER-AGNOSTIC spine: a row exists if the player has WhoScored shooting OR
-- any Fotmob shot, so xg/xgot still appear for a WhoScored-absent match (the ws
-- shooting columns are NULL).
with fot_xg as (
    select combo_id, player_uid, sum(xg) as xg, sum(xgot) as xgot
    from {{ ref('stg_fot_shots') }}
    where player_uid is not null
    group by combo_id, player_uid
),
-- Data XI xG / xGOT (own goals excluded; np = non-penalty; xGOT 0 off target)
dxi as (
    select combo_id, player_uid,
           sum(dxi_xg)                                    as dxi_xg,
           sum(dxi_xg)   filter (where not is_penalty)    as dxi_npxg,
           sum(dxi_xgot)                                  as dxi_xgot,
           sum(dxi_xgot) filter (where not is_penalty)    as dxi_npxgot
    from {{ ref('stg_ws_shot_xg') }}
    where not is_own_goal and player_uid is not null
    group by combo_id, player_uid
),
keys as (
    select combo_id, player_uid from {{ ref('stg_ws_player_shooting') }}
    union
    select combo_id, player_uid from fot_xg
)
select
    k.combo_id, k.player_uid,
    {{ passthrough_columns(ref('stg_ws_player_shooting'), 's',
         exclude=['combo_id','player_uid']) }},
    x.xg, x.xgot,
    d.dxi_xg, d.dxi_npxg, d.dxi_xgot, d.dxi_npxgot,
    dm.season_uid, dm.competition_uid, dm.match_date
from keys k
left join {{ ref('stg_ws_player_shooting') }} s on s.combo_id = k.combo_id and s.player_uid = k.player_uid
left join fot_xg x on x.combo_id = k.combo_id and x.player_uid = k.player_uid
left join dxi d on d.combo_id = k.combo_id and d.player_uid = k.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = k.combo_id
