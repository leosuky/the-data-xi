/* trivial tier (team-grain): text-numeric stat cols cast via introspection. */
select
    {{ cast_text_numeric(source('raw','ws_team_defending'),
                         keep_text=['combo_id'],
                         drop=['id', 'ingested_at']) }},
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_team_defending') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
