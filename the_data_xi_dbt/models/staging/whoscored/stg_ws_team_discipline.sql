/* trivial tier (team-grain): RAW already typed -> passthrough + uids. */
select
    s.whoscored_match_id,
    s.combo_id,
    s.team_id,
    s.fouls_committed,
    s.fouls_won,
    s.fouls_defensive,
    s.fouls_offensive,
    s.fouls_aerial,
    s.fouls_own_box,
    s.fouls_own_half,
    s.fouls_midfield,
    s.fouls_danger_area,
    s.fouls_first_half,
    s.fouls_second_half,
    s.fouls_late_game,
    s.yellow_cards,
    s.red_cards,
    s.second_yellow_cards,
    s.offsides_committed,
    s.offsides_caught,
    s.goals_disallowed_offside,
    s.dangerous_foul_rate_pct,
    s.foul_escalation_rate,
    s.foul_to_card_ratio,
    coalesce(tx.team_uid, s.team_id) as team_uid
from {{ source('raw','ws_team_discipline') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.ws_team_id = s.team_id
