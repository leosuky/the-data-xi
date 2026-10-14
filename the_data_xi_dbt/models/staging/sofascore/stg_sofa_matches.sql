/* trivial tier (flat): passthrough + typed casts.
   NB timestamps are ISO strings ('2025-08-16T14:00:00'), NOT unix epochs. */
{%- set cols = adapter.get_columns_in_relation(source('raw','sofa_matches')) | map(attribute='name') | list -%}

select
    {{ passthrough_columns(source('raw','sofa_matches'), 's',
         exclude=['id','ingested_at',
                  'homescore_current','homescore_display','homescore_period1',
                  'homescore_period2','homescore_normaltime',
                  'awayscore_current','awayscore_display','awayscore_period1',
                  'awayscore_period2','awayscore_normaltime',
                  'time_injurytime1','time_injurytime2','homeredcards',
                  'motm_rating','starttimestamp','time_currentperiodstarttimestamp',
                  'time_period1starttimestamp','time_period2starttimestamp']) }},
    {{ cast_col_or_null(cols, 's', 'homescore_current', 'int') }}      as homescore_current,
    {{ cast_col_or_null(cols, 's', 'homescore_display', 'int') }}      as homescore_display,
    {{ cast_col_or_null(cols, 's', 'homescore_period1', 'int') }}      as homescore_period1,
    {{ cast_col_or_null(cols, 's', 'homescore_period2', 'int') }}      as homescore_period2,
    {{ cast_col_or_null(cols, 's', 'homescore_normaltime', 'int') }}   as homescore_normaltime,
    {{ cast_col_or_null(cols, 's', 'awayscore_current', 'int') }}      as awayscore_current,
    {{ cast_col_or_null(cols, 's', 'awayscore_display', 'int') }}      as awayscore_display,
    {{ cast_col_or_null(cols, 's', 'awayscore_period1', 'int') }}      as awayscore_period1,
    {{ cast_col_or_null(cols, 's', 'awayscore_period2', 'int') }}      as awayscore_period2,
    {{ cast_col_or_null(cols, 's', 'awayscore_normaltime', 'int') }}   as awayscore_normaltime,
    {{ cast_col_or_null(cols, 's', 'time_injurytime1', 'int') }}       as time_injurytime1,
    {{ cast_col_or_null(cols, 's', 'time_injurytime2', 'int') }}       as time_injurytime2,
    {{ cast_col_or_null(cols, 's', 'homeredcards', 'int') }}           as homeredcards,
    {{ cast_col_or_null(cols, 's', 'motm_rating', 'numeric') }}        as motm_rating,
    {{ cast_col_or_null(cols, 's', 'starttimestamp', 'timestamp') }}                    as starttimestamp,
    {{ cast_col_or_null(cols, 's', 'time_currentperiodstarttimestamp', 'timestamp') }}  as time_currentperiodstarttimestamp,
    {{ cast_col_or_null(cols, 's', 'time_period1starttimestamp', 'timestamp') }}        as time_period1starttimestamp,
    {{ cast_col_or_null(cols, 's', 'time_period2starttimestamp', 'timestamp') }}        as time_period2starttimestamp
from {{ source('raw','sofa_matches') }} s
