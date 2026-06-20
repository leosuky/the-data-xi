/* wide crosscheck table (home/away per period; no per-team grain -> no uid). */
select
    {{ cast_text_numeric(source('raw','sofa_match_stats'), keep_text=['combo_id','period'], drop=['id']) }}
from {{ source('raw','sofa_match_stats') }}
