/* trivial tier (player-grain): 1:1 passthrough (introspected) + uids + team_row_id. */
with base as (
    select
        {{ passthrough_columns(source('raw','ws_shots'), 's', exclude=['id','ingested_at']) }},
        coalesce(px.player_uid, s.player_id) as player_uid,
        coalesce(tx.team_uid, s.team_id)     as team_uid
    from {{ source('raw','ws_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
    left join {{ ref('int_team_xwalk') }}   tx on tx.ws_team_id   = s.team_id
)
-- team-based shot index: WhoScored carries minute+second, so ordering is exact.
select *,
       row_number() over (partition by combo_id, team_uid
                          order by minute::int, second::int, row_id) as team_row_id
from base
