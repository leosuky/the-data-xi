/* trivial tier (flat): 1:1 passthrough (introspected) + uids. */
select
    {{ passthrough_columns(source('raw','ws_match_meta'), 's', exclude=['id','ingested_at']) }}
from {{ source('raw','ws_match_meta') }} s
