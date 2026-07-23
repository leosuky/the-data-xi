/* trivial tier (team-grain): text-numeric stat cols cast via introspection. */
select
    {{ cast_text_numeric(source('raw','ws_team_possession'),
                         keep_text=['combo_id'],
                         drop=['id', 'ingested_at']) }},
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_team_possession') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
