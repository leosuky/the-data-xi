-- Per-(team, competition, season): totals + percentile ranks across teams.
-- goals = the team's score (own goals by the opponent included); the shooting
-- table's goals are its players' goals only, which leaves own goals out.
with base as (
    select
        ts.team_uid, ts.season_uid, ts.competition_uid,
        count(*)                        as matches,
        sum(coalesce(case when ts.team_uid = dm.home_team_uid then nullif(dm.home_score::text, '')::int
                          when ts.team_uid = dm.away_team_uid then nullif(dm.away_score::text, '')::int
                     end, ts.goals, 0)) as goals,
        sum(coalesce(ts.total_shots,0)) as shots
    from {{ ref('fct_team_shooting') }} ts
    left join {{ ref('dim_match') }} dm on dm.combo_id = ts.combo_id
    group by ts.team_uid, ts.season_uid, ts.competition_uid
)
select *,
    goals::numeric / nullif(matches,0) as goals_per_match,
    percent_rank() over (partition by season_uid, competition_uid order by goals) as goals_pct,
    percent_rank() over (partition by season_uid, competition_uid order by shots) as shots_pct
from base
