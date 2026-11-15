-- Per-(keeper, competition, season): Data XI goals prevented.
-- Non-penalty shots on target faced, own goals excluded (penalties separate:
-- no measurable penalty-saving skill in the research).
--
-- goals_prevented is CENTRED on the average keeper of the same competition and
-- season: our xGOT runs ~7% low outside the top five and any xGOT's level moves
-- a few points between seasons, so uncentred, whole leagues look below par.
--   league_rate            = (sum xgot - sum goals) / sum shots on target, all keepers
--   goals_prevented        = xgot_faced - goals_conceded - sot_faced * league_rate
--   goals_prevented_se     = sqrt(sum p(1-p))  (typically ~4 goals a season)
--   goals_prevented_shrunk = goals_prevented * sot_faced / (sot_faced + 500)
-- One season is ~80% chance (research, Nov 2026): show the +/- and prefer
-- multi-season, shrunk numbers. Never a one-season verdict.
with shots as (
    select x.keeper_uid, dm.season_uid, dm.competition_uid,
           x.is_penalty, x.is_goal, x.dxi_xgot
    from {{ ref('stg_ws_shot_xg') }} x
    join {{ ref('dim_match') }} dm on dm.combo_id = x.combo_id
    where x.is_on_target and x.keeper_uid is not null
),
keeper as (
    select keeper_uid as player_uid, season_uid, competition_uid,
           count(*)      filter (where not is_penalty)                  as sot_faced,
           sum(dxi_xgot) filter (where not is_penalty)                  as xgot_faced,
           count(*)      filter (where not is_penalty and is_goal)      as goals_conceded,
           sum(dxi_xgot * (1 - dxi_xgot)) filter (where not is_penalty) as xgot_var,
           count(*)      filter (where is_penalty)                      as pen_sot_faced,
           sum(dxi_xgot) filter (where is_penalty)                      as pen_xgot_faced,
           count(*)      filter (where is_penalty and is_goal)          as pen_goals_conceded
    from shots
    group by keeper_uid, season_uid, competition_uid
),
league as (
    select season_uid, competition_uid,
           (sum(xgot_faced) - sum(goals_conceded)) / nullif(sum(sot_faced), 0) as league_rate
    from keeper
    group by season_uid, competition_uid
)
select
    k.*,
    l.league_rate,
    k.xgot_faced - k.goals_conceded - k.sot_faced * l.league_rate              as goals_prevented,
    sqrt(k.xgot_var)                                                           as goals_prevented_se,
    (k.xgot_faced - k.goals_conceded - k.sot_faced * l.league_rate)
        * k.sot_faced / (k.sot_faced + 500.0)                                  as goals_prevented_shrunk
from keeper k
join league l using (season_uid, competition_uid)
where k.sot_faced > 0
