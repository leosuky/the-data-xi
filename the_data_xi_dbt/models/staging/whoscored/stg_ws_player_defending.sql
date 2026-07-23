/* trivial tier (player-grain): text-numeric stat cols cast via introspection
   (per-third counts + pcts arrive as TEXT from the parser); labels kept as text. */
select
    {{ cast_text_numeric(source('raw','ws_player_defending'),
                         keep_text=['combo_id', 'player_name'],
                         drop=['id', 'ingested_at']) }},
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_player_defending') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
