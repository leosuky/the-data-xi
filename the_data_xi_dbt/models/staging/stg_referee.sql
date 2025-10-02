{{
    config(
        alias='referee',
        unique_key='id',
        incremental_strategy='merge',
    )
}}

with source_data as (
    select
        {{ dbt_utils.star(from=source('the_data_xi_raw', 'referee'), quote_identifiers=True) }}
    from {{ source('the_data_xi_raw', 'referee') }}
),

deduplicated as (
    select
        source_data.*,
        row_number() over (
            partition by source_data.id
            order by source_data.games desc
        ) as rn
    from source_data
)

select
    *
from deduplicated
where rn = 1

{% if is_incremental() %}
  and games > (
        select coalesce(max(games), 0)
        from {{ this }} t
        where t.id = deduplicated.id
  )
{% endif %}
