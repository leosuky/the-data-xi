{{
    config(
        unique_key='id',
        incremental_strategy='merge',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'shots'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'shots') }}
),

final as (
    select
        *,
        md5(
            coalesce(match_id::text, '') || '-' ||
            coalesce(row_id::text)
        ) as dbt_id
    from source_data
)

-- 2. Final select ensures schema evolution
select *
from final

{# {% if is_incremental() %}
  -- Only insert matches not already present in target
  where id not in (select id from {{ this }})
{% endif %} #}