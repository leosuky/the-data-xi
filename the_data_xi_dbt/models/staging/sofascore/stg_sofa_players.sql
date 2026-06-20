/* trivial tier (player-grain): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','sofa_players'), 's', exclude=['id','ingested_at']) }},
    px.player_uid
from {{ source('raw','sofa_players') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
