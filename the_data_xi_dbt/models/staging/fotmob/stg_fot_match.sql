/* trivial tier (flat): passthrough + typed casts.
   match_date is ISO-with-Z ('2025-08-16T14:00:00.000Z') → timestamptz. */
select
    {{ passthrough_columns(source('raw','fot_match'), 's',
         exclude=['id','ingested_at','match_date','motm_rating','match_round']) }},
    nullif(s.match_date, '')::timestamptz  as match_date,
    nullif(s.motm_rating, '')::numeric     as motm_rating,
    nullif(s.match_round, '')::int         as match_round
from {{ source('raw','fot_match') }} s
