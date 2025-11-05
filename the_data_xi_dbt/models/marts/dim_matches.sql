{{
    config(
        materialized='incremental',
        unique_key='match_id',
        incremental_strategy='merge'   
    )
}}

with match_details as (
    select
        id as match_id,
        combo_id,
        match_date,
        -- Using COALESCE to handle potential nulls, defaulting to 0
        coalesce(homescore_normaltime, 0) as home_team_goals,
        coalesce(awayscore_normaltime, 0) as away_team_goals,
        coalesce(attendance, 0) as attendance,
        home_team_id,
        away_team_id,
        referee_id,
        tournament_id,
        season_id,
        winnercode,
        home_formation,
        away_formation,
        roundinfo_round as match_round,
        home_manager_id,
        away_manager_id
    from {{ ref('stg_match_details') }}
),
tournament as (
    select * from {{ ref('stg_tournaments') }}
),
season as (
    select * from {{ ref('stg_seasons') }}
),
referee as (
    select * from {{ ref('stg_referee') }}
),
manager_h as (
    select * from {{ ref('stg_managers') }}
),
manager_a as (
    select * from {{ ref('stg_managers') }}
),

match_stats as (
    -- We are aggregating some key stats from stg_match_stats to enrich our dimension
    select
        combo_id,
        coalesce(total_shots_homevalue, 0) as home_team_shots,
        coalesce(total_shots_awayvalue, 0) as away_team_shots,
        coalesce(shots_on_target_homevalue, 0) as home_team_shots_on_target,
        coalesce(shots_on_target_awayvalue, 0) as away_team_shots_on_target,
        coalesce(expected_goals_homevalue, 0) as home_team_expected_goals,
        coalesce(expected_goals_awayvalue, 0) as away_team_expected_goals,
        coalesce(ball_possession_homevalue, 0) as home_team_possession,
        coalesce(ball_possession_awayvalue, 0) as away_team_possession
    from {{ ref('stg_match_stats') }}
    where "period" = 'FULL-TIME'
)

select
    md.match_id,
    md.combo_id,
    md.match_date,
    md.home_team_id,
    md.away_team_id,
    md.referee_id,
    rf.name as referee_name,
    md.tournament_id,
    td.name as tournament_name,
    td.tier,
    md.season_id,
    sd.name as season_name,
    sd.year,
    md.home_team_goals,
    md.away_team_goals,
    (md.home_team_goals + md.away_team_goals) as total_goals,

    -- Adding a clear, human-readable match result
    case
        when md.winnercode = 1 then 'Home'
        when md.winnercode = 2 then 'Away'
        when md.winnercode = 3 then 'Draw'
        else 'Unknown'
    end as match_result,

    ms.home_team_shots,
    ms.away_team_shots,
    ms.home_team_shots_on_target,
    ms.away_team_shots_on_target,
    ms.home_team_expected_goals,
    ms.away_team_expected_goals,
    ms.home_team_possession,
    ms.away_team_possession,

    md.attendance,
    md.home_formation,
    md.away_formation,
    match_round,
    home_manager_id,
    mgh.name as home_manager_name,
    away_manager_id,
    mga.name as away_manager_name

from match_details md
left join match_stats ms
    on md.combo_id = ms.combo_id
left join tournament td
    on md.tournament_id = td.tournament_id
left join season sd
    on md.season_id = sd.id
left join referee rf
    on md.referee_id = rf.id
left join manager_h mgh
    on md.home_manager_id = mgh.id
left join manager_a mga
    on md.away_manager_id = mga.id

