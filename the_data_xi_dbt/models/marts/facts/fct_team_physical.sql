{{ config(materialized='table') }}
-- fct_team_physical: per-(team, match, period) physical totals from Sofascore's
-- team statistics. These are the official team totals, so they INCLUDE players
-- whose individual tracking is missing — use this, not a sum of
-- fct_player_physical, for team distance. Tracking exists from 2026/27 only.
-- period: FULL-TIME / FIRST-HALF / SECOND-HALF.
--
-- team_uid comes from the Sofascore match row (not dim_match), so leagues with
-- no WhoScored data (e.g. Saudi Pro League) still resolve their teams.
{% set cols = adapter.get_columns_in_relation(ref('stg_sofa_match_stats'))
              | map(attribute='name') | list %}
with teams as (
    select sm.combo_id,
           hx.team_uid as home_team_uid,
           ax.team_uid as away_team_uid
    from {{ ref('stg_sofa_matches') }} sm
    left join {{ ref('int_team_xwalk') }} hx on hx.sofa_team_id = sm.home_team_id
    left join {{ ref('int_team_xwalk') }} ax on ax.sofa_team_id = sm.away_team_id
),
long as (
    select ms.combo_id, ms.period, true as is_home_team,
           {{ col_or_null(cols, 'ms', 'distance_covered_homevalue') }}  as distance_km,
           {{ col_or_null(cols, 'ms', 'number_of_sprints_homevalue') }} as sprints,
           {{ col_or_null(cols, 'ms', 'distance_covered_awayvalue') }}  as opp_distance_km,
           {{ col_or_null(cols, 'ms', 'number_of_sprints_awayvalue') }} as opp_sprints
    from {{ ref('stg_sofa_match_stats') }} ms
    union all
    select ms.combo_id, ms.period, false,
           {{ col_or_null(cols, 'ms', 'distance_covered_awayvalue') }},
           {{ col_or_null(cols, 'ms', 'number_of_sprints_awayvalue') }},
           {{ col_or_null(cols, 'ms', 'distance_covered_homevalue') }},
           {{ col_or_null(cols, 'ms', 'number_of_sprints_homevalue') }}
    from {{ ref('stg_sofa_match_stats') }} ms
)
select
    l.combo_id,
    case when l.is_home_team then t.home_team_uid else t.away_team_uid end as team_uid,
    case when l.is_home_team then t.away_team_uid else t.home_team_uid end as opponent_team_uid,
    l.is_home_team,
    l.period,
    dm.season_uid, dm.competition_uid, dm.match_date,
    l.distance_km,
    l.sprints,
    l.opp_distance_km,
    l.opp_sprints,
    l.distance_km - l.opp_distance_km as distance_diff_km,
    l.sprints - l.opp_sprints         as sprints_diff
from long l
left join teams t on t.combo_id = l.combo_id
left join {{ ref('dim_match') }} dm on dm.combo_id = l.combo_id
where l.distance_km is not null
