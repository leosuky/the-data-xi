{{ config(materialized='view') }}
-- Per-(team, competition, season): totals + percentile ranks across teams.
with base as (
    select
        ts.team_uid, ts.season_uid, ts.competition_uid,
        count(*)                        as matches,
        sum(coalesce(ts.goals,0))       as goals,
        sum(coalesce(ts.total_shots,0)) as shots
    from {{ ref('fct_team_shooting') }} ts
    group by ts.team_uid, ts.season_uid, ts.competition_uid
)
select *,
    goals::numeric / nullif(matches,0) as goals_per_match,
    percent_rank() over (partition by season_uid, competition_uid order by goals) as goals_pct,
    percent_rank() over (partition by season_uid, competition_uid order by shots) as shots_pct
from base
