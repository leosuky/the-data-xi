{{
    config(
        unique_key='id',
        incremental_strategy='append',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'managers'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'managers') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where id not in (select id from {{ this }})
{% endif %}