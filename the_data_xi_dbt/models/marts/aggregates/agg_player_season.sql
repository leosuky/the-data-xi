{{ config(materialized='view') }}
-- Per-(player, competition, season): totals -> per-90 -> percentile ranks.
-- Metric set is a representative starter; extend by adding sums + per90 + ranks.
with base as (
    select
        pm.player_uid, pm.season_uid, pm.competition_uid,
        max(dp.position)                 as position,
        sum(pm.minutes_played)           as minutes,
        count(*)                         as matches,
        sum(coalesce(sh.goals,0))        as goals,
        sum(coalesce(sh.total_shots,0))  as shots,
        sum(coalesce(sh.xg,0))           as xg,
        sum(coalesce(po.touches,0))      as touches,
        sum(coalesce(po.dribbles_won,0)) as dribbles
    from {{ ref('fct_player_match') }} pm
    left join {{ ref('fct_player_shooting') }}   sh on sh.combo_id=pm.combo_id and sh.player_uid=pm.player_uid
    left join {{ ref('fct_player_possession') }} po on po.combo_id=pm.combo_id and po.player_uid=pm.player_uid
    left join {{ ref('dim_player') }} dp on dp.player_uid = pm.player_uid
    group by pm.player_uid, pm.season_uid, pm.competition_uid
),
per90 as (
    select *,
        nullif(minutes,0)/90.0 as n90
    from base
    where minutes >= 450                      -- sample floor
),
rates as (
    select *,
        goals    / n90 as goals_p90,
        shots    / n90 as shots_p90,
        xg       / n90 as xg_p90,
        touches  / n90 as touches_p90,
        dribbles / n90 as dribbles_p90
    from per90
)
select *,
    percent_rank() over (partition by season_uid, competition_uid, position order by goals_p90)    as goals_p90_pct,
    percent_rank() over (partition by season_uid, competition_uid, position order by xg_p90)       as xg_p90_pct,
    percent_rank() over (partition by season_uid, competition_uid, position order by touches_p90)  as touches_p90_pct,
    percent_rank() over (partition by season_uid, competition_uid, position order by dribbles_p90) as dribbles_p90_pct
from rates
