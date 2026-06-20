/* trivial tier (flat): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','oddsp_odds'), 's', exclude=['id','ingested_at']) }}
from {{ source('raw','oddsp_odds') }} s
