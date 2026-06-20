/* trivial tier (team-grain): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','ws_team_passing'), 's', exclude=['id','ingested_at']) }},
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_team_passing') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
