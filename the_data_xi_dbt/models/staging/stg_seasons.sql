{{
    config(
        unique_key='id',
        incremental_strategy='merge',
    )
}}

-- 1. Select from the raw matches source
with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'seasons'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'seasons') }}
),

deduplicated as (
    select
        source_data.*,
        row_number() over (
            partition by source_data.id
        ) as rn
    from source_data
)

select
    *
from deduplicated
where rn = 1

{% if is_incremental() %}
  -- Only insert matches not already present in target
  and id not in (select id from {{ this }})
{% endif %}