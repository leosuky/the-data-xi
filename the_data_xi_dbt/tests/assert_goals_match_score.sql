-- Goals counted from fct_shots (by credited_team_uid, so own goals count for
-- the right side) must equal the final score. Catches lost/duplicated goal
-- rows and wrong own-goal flags. Only finished matches with shot data.
{{ config(severity='warn') }}
with g as (
    select combo_id,
           sum(case when credited_team_uid = m.home_team_uid and is_goal then 1 else 0 end) as home_goals,
           sum(case when credited_team_uid = m.away_team_uid and is_goal then 1 else 0 end) as away_goals
    from {{ ref('fct_shots') }} s
    join {{ ref('dim_match') }} m using (combo_id)
    group by combo_id
)
select m.combo_id, m.home_score, g.home_goals, m.away_score, g.away_goals
from {{ ref('dim_match') }} m
join g using (combo_id)
where m.match_status = 'finished'
  and (m.home_score::int <> g.home_goals or m.away_score::int <> g.away_goals)
