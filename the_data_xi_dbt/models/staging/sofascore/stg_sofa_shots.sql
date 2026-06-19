/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.match_id,
    s.shot_id,
    s.player_id,
    s.player_name,
    s.ishome,
    s.shottype,
    s.situation,
    s.bodypart,
    s.goalmouthlocation,
    s.xg,
    s.xgot,
    s.incidenttype,
    s.goaltype,
    s.addedtime,
    px.player_uid
from {{ source('raw','sofa_shots') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
