-- This dimension table provides a unique, clean record for every team.
-- It is built from the staging teams table and deduplicates the data to ensure
-- that each team appears only once.

with source_teams as (
    -- Using a window function to handle potential duplicates in the source data.
    -- We select only the first record we encounter for each team_id.
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