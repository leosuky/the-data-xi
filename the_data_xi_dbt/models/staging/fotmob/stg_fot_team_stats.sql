/* wide crosscheck table (home/away per period; no per-team grain -> no uid). */
select
    {{ cast_text_numeric(source('raw','fot_team_stats'), keep_text=['combo_id','period'], drop=['id']) }}
from {{ source('raw','fot_team_stats') }}
