{{ config(materialized='table') }}
-- fct_player_goalkeeping: stg_ws_player_goalkeep + Data XI xGOT faced
-- + denormalized season/competition/date for direct filtering.
-- dxi_*: shots on target faced while in goal (keeper_uid on the shot), own
-- goals excluded, penalties separate. Goals prevented = xgot_faced -
-- goals_conceded; its variance (sum of p(1-p)) gives the standard error.
-- Centred per competition-season in agg_keeper_season.
with faced as (
    select combo_id, keeper_uid,
           count(*)      filter (where not is_penalty)              as dxi_sot_faced,
           sum(dxi_xgot) filter (where not is_penalty)              as dxi_xgot_faced,
           count(*)      filter (where not is_penalty and is_goal)  as dxi_goals_conceded,
           sum(dxi_xgot * (1 - dxi_xgot)) filter (where not is_penalty) as dxi_xgot_var,
           count(*)      filter (where is_penalty)                  as dxi_pen_sot_faced,
           sum(dxi_xgot) filter (where is_penalty)                  as dxi_pen_xgot_faced,
           count(*)      filter (where is_penalty and is_goal)      as dxi_pen_goals_conceded
    from {{ ref('stg_ws_shot_xg') }}
    where is_on_target and keeper_uid is not null
    group by combo_id, keeper_uid
)
select
    s.*,
    coalesce(f.dxi_sot_faced, 0)          as dxi_sot_faced,
    coalesce(f.dxi_xgot_faced, 0)         as dxi_xgot_faced,
    coalesce(f.dxi_goals_conceded, 0)     as dxi_goals_conceded,
    coalesce(f.dxi_xgot_var, 0)           as dxi_xgot_var,
    coalesce(f.dxi_xgot_faced, 0) - coalesce(f.dxi_goals_conceded, 0) as dxi_goals_prevented,
    coalesce(f.dxi_pen_sot_faced, 0)      as dxi_pen_sot_faced,
    coalesce(f.dxi_pen_xgot_faced, 0)     as dxi_pen_xgot_faced,
    coalesce(f.dxi_pen_goals_conceded, 0) as dxi_pen_goals_conceded,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_player_goalkeep') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
left join faced f on f.combo_id = s.combo_id and f.keeper_uid = s.player_uid
