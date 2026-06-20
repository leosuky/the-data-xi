/* trivial tier (flat): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','sofa_teams'), 's', exclude=['id','ingested_at']) }},
    tx.team_uid
from {{ source('raw','sofa_teams') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
