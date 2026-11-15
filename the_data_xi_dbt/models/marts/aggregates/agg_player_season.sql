-- Per-(player, competition, season): totals -> per-90 -> percentile ranks.
-- Metric set is a representative starter; extend by adding sums + per90 + ranks.
--
-- Data XI placement_added = sum(xGOT - xG) over non-penalty shots, centred on
-- the competition-season's average shot (off target counts as xGOT 0).
-- A description of how a player placed his shots, not a skill rating: one
-- season is ~88% chance (research, Nov 2026). se ~ 0.184 * sqrt(shots).
with dxi_shots as (
    select x.player_uid, dm.season_uid, dm.competition_uid,
           x.dxi_xg, x.dxi_xgot, x.dxi_xgot - x.dxi_xg as added
    from {{ ref('stg_ws_shot_xg') }} x
    join {{ ref('dim_match') }} dm on dm.combo_id = x.combo_id
    where not x.is_penalty and not x.is_own_goal and x.player_uid is not null
),
dxi_league as (
    select season_uid, competition_uid, avg(added) as league_added
    from dxi_shots
    group by season_uid, competition_uid
),
dxi as (
    select s.player_uid, s.season_uid, s.competition_uid,
           count(*)                                     as dxi_np_shots,
           sum(s.dxi_xg)                                as dxi_npxg,
           sum(s.dxi_xgot)                              as dxi_npxgot,
           sum(s.added) - count(*) * max(l.league_added) as placement_added,
           0.184 * sqrt(count(*))                       as placement_added_se
    from dxi_shots s
    join dxi_league l using (season_uid, competition_uid)
    group by s.player_uid, s.season_uid, s.competition_uid
),
base as (
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
base_dxi as (
    select b.*, d.dxi_np_shots, d.dxi_npxg, d.dxi_npxgot, d.placement_added, d.placement_added_se
    from base b
    left join dxi d using (player_uid, season_uid, competition_uid)
),
per90 as (
    select *,
        nullif(minutes,0)/90.0 as n90
    from base_dxi
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
