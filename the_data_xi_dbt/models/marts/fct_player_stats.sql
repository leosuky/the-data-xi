-- This is the final fact table, the core of our star schema.
-- It contains one row for each player's performance in a single match.
-- The complex joins are handled in the intermediate model, so this model's logic
-- is clean and focused on assembling the final analytical table.

with player_stats as (
    -- We select all the aggregated stats from our intermediate model.
    select * from {{ ref('int_player_stats_full') }}
)

select
    -- Generating a unique surrogate key for the fact table
    {{ dbt_utils.generate_surrogate_key(['match_id', 'player_id']) }} as player_performance_id,
    player_stats.*

from player_stats

