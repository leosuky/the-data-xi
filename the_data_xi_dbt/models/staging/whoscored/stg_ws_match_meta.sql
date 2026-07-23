/* trivial tier (flat): passthrough + typed start_time.
   NB `score` / `ht_score` STAY TEXT — they hold '5-1'-style strings. */
select
    {{ passthrough_columns(source('raw','ws_match_meta'), 's',
         exclude=['id','ingested_at','start_time']) }},
    nullif(s.start_time, '')::timestamp as start_time
from {{ source('raw','ws_match_meta') }} s
