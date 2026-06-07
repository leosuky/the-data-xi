-- depends_on: {{ ref('stg_odds_data') }}

{{
    config(
        description='Betting odds per match. One row per match.',
        materialized='incremental',
        unique_key='match_id',
        incremental_strategy='merge'   
    )
}}


with odds as (
    select
        *
    from {{ ref('stg_odds_data') }}
)

select
    match_id,
    combo_id,
    "full-time_1x2_1" as full_time_1_x_2_home,
    "full-time_1x2_x" as full_time_1_x_2_draw,
    "full-time_1x2_2" as full_time_1_x_2_away,
    "full-time_1x2_winning" as full_time_1_x_2_result,

    "full-time_double chance_1x" as double_chance_1x,
    "full-time_double chance_x2" as double_chance_x2,
    "full-time_double chance_12" as double_chance_12,
    "full-time_double chance_winning" as double_chance_result,

    "1st half_1x2_1" as first_half_1_x_2_home,
    "1st half_1x2_x" as first_half_1_x_2_draw,
    "1st half_1x2_2" as first_half_1_x_2_away,
    "1st half_1x2_winning" as first_half_1_x_2_result,

    "full-time_both teams to score_yes" as btts_yes,
    "full-time_both teams to score_no" as btts_no,
    "full-time_both teams to score_winning" as btts_result,

    "full-time_match goals_0.5_over" as over_0_5_goals,
    "full-time_match goals_0.5_under" as under_0_5_goals,
    "full-time_match goals_0.5_winning" as result_0_5_goals,

    "full-time_match goals_1.5_over" as over_1_5_goals,
    "full-time_match goals_1.5_under" as under_1_5_goals,
    "full-time_match goals_1.5_winning" as result_1_5_goals,

    "full-time_match goals_2.5_over" as over_2_5_goals,
    "full-time_match goals_2.5_under" as under_2_5_goals,
    "full-time_match goals_2.5_winning" as result_2_5_goals,

    "full-time_match goals_3.5_over" as over_3_5_goals,
    "full-time_match goals_3.5_under" as under_3_5_goals,
    "full-time_match goals_3.5_winning" as result_3_5_goals,

    "full-time_match goals_4.5_over" as over_4_5_goals,
    "full-time_match goals_4.5_under" as under_4_5_goals,
    "full-time_match goals_4.5_winning" as result_4_5_goals,

    "full-time_match goals_5.5_over" as over_5_5_goals,
    "full-time_match goals_5.5_under" as under_5_5_goals,
    "full-time_match goals_5.5_winning" as result_5_5_goals,

    "full-time_match goals_6.5_over" as over_6_5_goals,
    "full-time_match goals_6.5_under" as under_6_5_goals,
    "full-time_match goals_6.5_winning" as result_6_5_goals

from odds