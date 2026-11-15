/* Data XI xG / xGOT per shot (parse_shot_xg) + team / player / keeper uids.
   A shot is (combo_id, team_uid, event_id): WhoScored numbers events per team. */
select
    {{ passthrough_columns(source('raw','ws_shot_xg'), 'x', exclude=['id','ingested_at']) }},
    coalesce(tx.team_uid,   x.team_id)   as team_uid,
    coalesce(px.player_uid, x.player_id) as player_uid,
    coalesce(kx.player_uid, x.keeper_id) as keeper_uid
from {{ source('raw','ws_shot_xg') }} x
left join {{ ref('int_team_xwalk') }}   tx on tx.ws_team_id   = x.team_id
left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = x.player_id
left join {{ ref('int_player_xwalk') }} kx on kx.ws_player_id = x.keeper_id
