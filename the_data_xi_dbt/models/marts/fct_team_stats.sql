{{
    config(
        materialized='incremental',
        unique_key='xx_id',
        incremental_strategy='merge'   
    )
}}

with team_stats as (
    -- We select all the aggregated stats from our intermediate model.
    select * from {{ ref('int_team_stats_full') }}
)

select

    {{
        dbt_utils.star(
            from=ref('int_team_stats_full'), 
            quote_identifiers=True, 
            except=[
                "combo_id", 
                "id",
                "coverage",
                "awayscore_display",
                "homescore_display",
                "awayscore_current",
                "homescore_current",
                "correcthalftimeaiinsight",
                "correctaiinsight"
            ]
        )
    }}


from team_stats

