{{
    config(
        unique_key='tournament_id',
        incremental_strategy='merge',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'tournaments'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'tournaments') }}
)

-- 2. Final select ensures schema evolution
select *
from source_data

{% if is_incremental() %}
  -- Only insert matches not already present in target
  where tournament_id not in (select tournament_id from {{ this }})
{% endif %}