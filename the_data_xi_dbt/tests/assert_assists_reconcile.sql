-- Assists must add up three ways, per (match, team), WhoScored matches only:
--   1. assisted goals: WhoScored goal shots carrying the Assisted qualifier
--   2. team assists (fct_team_passing)
--   3. the team's player assists summed (fct_player_passing)
-- Each assist is resolved from the goal's RelatedEventId. When that lookup
-- misses (the eventId-collision bug that undercounted assists), 2 and 3 fall
-- below 1; when player and team logic drift apart, 3 differs from 2.
-- severity=warn: it reports, it never blocks the marts.
{{ config(severity='warn') }}
with shots as (
    select combo_id, team_uid,
           sum(case when is_goal and is_assisted then 1 else 0 end) as assisted_goals
    from {{ ref('fct_shots') }}
    where ws_row_id is not null
    group by combo_id, team_uid
),
team as (
    select combo_id, team_uid, sum(coalesce(assists, 0)) as team_assists
    from {{ ref('fct_team_passing') }}
    group by combo_id, team_uid
),
players as (
    select combo_id, team_uid, sum(coalesce(assists, 0)) as player_assists
    from {{ ref('fct_player_passing') }}
    group by combo_id, team_uid
)
select t.combo_id, t.team_uid,
       coalesce(s.assisted_goals, 0) as assisted_goals,
       t.team_assists,
       coalesce(p.player_assists, 0) as player_assists
from team t
left join shots   s using (combo_id, team_uid)
left join players p using (combo_id, team_uid)
where t.team_assists <> coalesce(s.assisted_goals, 0)
   or t.team_assists <> coalesce(p.player_assists, 0)
