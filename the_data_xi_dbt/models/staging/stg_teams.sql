{{
    config(
        unique_key='team_id',
        incremental_strategy='append',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'teams'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'teams') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where team_id not in (select team_id from {{ this }})
{% endif %}