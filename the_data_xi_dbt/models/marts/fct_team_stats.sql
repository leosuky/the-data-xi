-- This is the final fact table, the core of our star schema.
-- It contains one row for each player's performance in a single match.
-- The complex joins are handled in the intermediate model, so this model's logic
-- is clean and focused on assembling the final analytical table.

with team_stats as (
    -- We select all the aggregated stats from our intermediate model.
    select * from {{ ref('int_team_stats_full') }}
)

select
    -- Generating a unique surrogate key for the fact table
    {{ dbt_utils.generate_surrogate_key(['id', 'home_team_id', 'away_team_id']) }} as team_performance_id,
    team_stats.*

from team_stats

