/* trivial tier (player-grain): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.match_id,
    s.team_id,
    s.is_home_team,
    s.player_id,
    s.name,
    s.type,
    s.reason,
    s.description,
    s.external_type,
    s.expected_end_date,
    px.player_uid,
    tx.team_uid
from {{ source('raw','sofa_missing_players') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
