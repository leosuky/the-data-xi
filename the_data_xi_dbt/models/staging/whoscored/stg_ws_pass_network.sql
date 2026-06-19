/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.passer_id,
    s.passer_name,
    s.receiver_id,
    s.receiver_name,
    s.pass_count,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_pass_network') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
