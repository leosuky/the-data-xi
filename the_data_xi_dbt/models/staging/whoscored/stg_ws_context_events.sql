/* trivial tier (player-grain): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','ws_context_events'), 's', exclude=['id','ingested_at']) }},
    coalesce(px.player_uid, s.player_id) as player_uid,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_context_events') }} s
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
