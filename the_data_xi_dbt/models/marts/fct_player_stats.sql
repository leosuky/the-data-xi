-- depends_on: {{ ref('int_player_stats_full') }}

{{
    config(
        description='Wide player performance table. One row per player per match.',
        unique_key='xx_id',
        incremental_strategy='merge'   
    )
}}

with player_stats as (
    -- We select all the aggregated stats from our intermediate model.
    select * from {{ ref('int_player_stats_full') }}
)

select
    
    {{
        dbt_utils.star(
            from=ref('int_player_stats_full'), 
            quote_identifiers=True, 
            except=[
                "gk_match_id",
                "goalkeeper_name",
                "combo_idx", 
                "match_idx", 
                "is_home_teamx", 
                "player", 
                "kit_number", 
                "xx_idx"
            ]
        )
    }}

from player_stats

