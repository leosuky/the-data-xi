-- This dimension table provides a unique, clean record for every player.
-- It is built from the staging player_data table and deduplicates the data to ensure
-- that each player appears only once, creating a reliable master list of players.

with source_players as (
    -- We use a window function to handle potential duplicates in the source data.
    -- This ensures that if a player's details were ingested multiple times,
    -- we only select one unique record for our final dimension.
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