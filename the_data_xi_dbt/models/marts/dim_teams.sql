-- depends_on: {{ ref('stg_teams') }}

{{
    config(
        description='Core dimension table for teams. One row per team.',
        materialized='incremental',
        unique_key='team_id',
        incremental_strategy='merge'   
    )
}}

with source_teams as (

    select
        team_id,
        name as team_name,
        country,
        stadium,
        stadium_capacity,
        city,
        longitude,
        latitude,
        founded_date
    from {{ ref('stg_teams') }}
)

select
    team_id,
    team_name,
    country,
    stadium,
    stadium_capacity,
    city,
    longitude,
    latitude,
    founded_date
from source_teams