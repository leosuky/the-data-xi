{{
    config(
        alias='odds_data',
        unique_key='match_id',
        incremental_strategy='append',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'odds_data'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'odds_data') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where match_id not in (select match_id from {{ this }})
{% endif %}