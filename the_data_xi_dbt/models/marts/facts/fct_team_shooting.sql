{{ config(materialized='table') }}
-- fct_team_shooting: stg_ws_team_shooting + Data XI xG / xGOT (for and against)
-- + denormalized season/competition/date for direct filtering.
-- Own goals excluded; np = non-penalty; xGOT is 0 for shots off target.
with dxi as (
    select combo_id, team_uid,
           sum(dxi_xg)                                    as dxi_xg,
           sum(dxi_xg)   filter (where not is_penalty)    as dxi_npxg,
           sum(dxi_xgot)                                  as dxi_xgot,
           sum(dxi_xgot) filter (where not is_penalty)    as dxi_npxgot
    from {{ ref('stg_ws_shot_xg') }}
    where not is_own_goal
    group by combo_id, team_uid
)
select
    s.*,
    d.dxi_xg, d.dxi_npxg, d.dxi_xgot, d.dxi_npxgot,
    o.dxi_xg as dxi_xg_against, o.dxi_npxg as dxi_npxg_against,
    o.dxi_xgot as dxi_xgot_against, o.dxi_npxgot as dxi_npxgot_against,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_team_shooting') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
left join dxi d on d.combo_id = s.combo_id and d.team_uid = s.team_uid
left join dxi o on o.combo_id = s.combo_id
               and o.team_uid = case when s.team_uid = dm.home_team_uid then dm.away_team_uid
                                     else dm.home_team_uid end
