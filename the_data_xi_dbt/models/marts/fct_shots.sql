{{
    config(
        description="Event-level shot data. One row per shot per match.",
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'   
    )
}}

with shots as (
    select * from {{ ref('stg_shots') }}
),
adv_shots as (
    select * from {{ ref('stg_advanced_shot_data') }}
)


select

    shots.id,
    advs.xg as xg,
    advs.psxg as psxg,
    advs.outcome as outcome,
    shots.shottype as shot_type,
    advs.distance as distance, -- in Yards
    shots.player_name as player_name,
    shots.player_id as player_id,
    advs.notes as notes,
    shots.time as minute_time,
    shots.addedtime as added_time,
    shots.timeseconds as time_in_seconds,
    shots.situation as situation,
    shots.ishome as is_home_team,
    advs.squad as team,
    shots.bodypart as body_part,
    advs."sca 1_player" as sca1_player,
    advs."sca 1_event" as sca1_event,
    advs."sca 2_player" as sca2_player,
    advs."sca 2_event" as sca2_event,
    shots.goaltype as goal_type,
    shots.draw as draw,
    shots.playercoordinates as player_coordinates,
    shots.goalmouthcoordinates as goal_mouth_coordinates,
    shots.goalmouthlocation as goal_mouth_location,
    shots.blockcoordinates as block_coordinates,
    shots.match_id as match_id,
    shots.combo_id as combo_id
    {# advs.id as dbt_id #}


from shots
left join adv_shots advs
    on shots.combo_id = advs.combo_id and shots.row_id = advs.row_id
    {# on shots.dbt_id = advs.id #}