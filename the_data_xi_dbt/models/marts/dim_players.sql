{{
    config(
        description='Core dimension table for players. One row per player.',
        materialized='incremental',
        unique_key='player_id',
        incremental_strategy='merge'   
    )
}}

with source_players as (

    select
        player_id,
        name as player_name,
        -- Correctly casting birth_date from text/timestamp to a date
        cast(birth_date as date) as birth_date,
        position,
        nationality
    from {{ ref('stg_player_data') }}
)

select
    player_id,
    player_name,
    birth_date,
    position,
    nationality
from source_players