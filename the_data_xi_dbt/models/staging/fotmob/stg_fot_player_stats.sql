/* dynamic per-row stats: season-varying numeric-TEXT cols cast via introspection;
   base text labels preserved via keep_text. */
select
    {{ cast_text_numeric(source('raw','fot_player_stats'), keep_text=['combo_id', 'player_name', 'team_name']) }},
    px.player_uid,
    tx.team_uid
from {{ source('raw','fot_player_stats') }} s
left join {{ ref('int_player_xwalk') }} px on px.fot_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.fot_team_id = s.team_id
