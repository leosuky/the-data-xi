/* trivial tier (player-grain): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','fot_shots'), 's', exclude=['id','ingested_at']) }},
    px.player_uid,
    tx.team_uid
from {{ source('raw','fot_shots') }} s
left join {{ ref('int_player_xwalk') }} px on px.fot_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.fot_team_id = s.team_id
