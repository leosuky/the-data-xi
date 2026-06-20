/* trivial tier (flat): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','sofa_seasons'), 's', exclude=['id','ingested_at']) }}
from {{ source('raw','sofa_seasons') }} s
