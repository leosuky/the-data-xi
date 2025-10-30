{{
    config(
        unique_key='lineup_id',
        incremental_strategy='merge',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'player_stats'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'player_stats') }}
),

final as (
    select
        *,
        md5(
            coalesce(match_id::text, '') || '-' ||
            coalesce(player_id::text)
        ) as lineup_id
    from source_data
)

-- 2. Final select ensures schema evolution
select *
from final

{# {% if is_incremental() %}
  -- Only insert matches not already present in target
  where match_id not in (select match_id from {{ this }})
{% endif %} #}