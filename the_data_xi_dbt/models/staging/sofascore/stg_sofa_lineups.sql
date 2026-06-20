/* trivial tier (player-grain): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','sofa_lineups'), 's', exclude=['id','ingested_at']) }},
    px.player_uid,
    tx.team_uid
from {{ source('raw','sofa_lineups') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
