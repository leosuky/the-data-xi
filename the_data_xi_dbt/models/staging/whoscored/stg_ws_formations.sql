/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.team_name,
    s.is_home_team,
    s.formation_index,
    s.formation_name,
    s.formation_id,
    s.start_minute,
    s.end_minute,
    s.captain_player_id,
    s.captain_name,
    s.sub_on_player_id,
    s.sub_off_player_id,
    s.player_ids,
    s.jersey_numbers,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_formations') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
