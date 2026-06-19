/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.player_id,
    s.name,
    s.position,
    s.nationality,
    s.birth_date,
    px.player_uid
from {{ source('raw','sofa_players') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
