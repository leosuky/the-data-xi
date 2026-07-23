/* trivial tier (flat): passthrough + typed casts.
   NB `line` STAYS TEXT — it holds spread pairs like '-2.5/+2.5', not a number. */
select
    {{ passthrough_columns(source('raw','oddsp_odds'), 's',
         exclude=['id','ingested_at','oddspedia_match_id','odds_value']) }},
    nullif(s.oddspedia_match_id, '')::bigint  as oddspedia_match_id,
    nullif(s.odds_value, '')::numeric         as odds_value   -- decimal odds ('16')
from {{ source('raw','oddsp_odds') }} s
