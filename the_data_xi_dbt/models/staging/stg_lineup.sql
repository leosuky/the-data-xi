{{
    config(
        alias='lineup',
        unique_key=['match_id', 'player_id'],
        incremental_strategy='append',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'lineup'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'lineup') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where match_id not in (select match_id from {{ this }})
{% endif %}