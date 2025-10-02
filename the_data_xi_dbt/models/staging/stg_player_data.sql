{{
    config(
        alias='player_data',
        unique_key='player_id',
        incremental_strategy='append',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'player_data'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'player_data') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where player_id not in (select player_id from {{ this }})
{% endif %}