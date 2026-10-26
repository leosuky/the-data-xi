{{ config(materialized='table', post_hook="create index if not exists {{ this.name }}_combo_idx on {{ this }} (combo_id)") }}
-- fct_defensive_actions: one row per defensive event (stg_ws_defensive_actions),
-- for defensive-action maps. x/y are WhoScored 0-100 from the acting team's
-- attacking direction (x = 0 own goal line, x = 100 opponent's).
-- stat = the per-match stat the event counts toward, so a map's dots add up to
-- fct_team_defending / fct_player_defending (verified per player-match):
--   tackles, interceptions, clearances, blocks (shot blocks), blocked_passes,
--   recoveries (ball recovery, keeper pickup, successful claim), aerials,
--   dribbled_past, fouls, blocked_crosses; NULL for saves, sweeps, errors and
--   unsuccessful claims.
select
    s.combo_id, s.event_id, s.team_uid, s.player_uid, s.player_name,
    s.period, s.minute, s.second, s.x, s.y, s.end_x, s.end_y,
    s.action, s.is_successful, s.is_header,
    case
        when s.action = 'tackle'                         then 'tackles'
        when s.action = 'interception'                   then 'interceptions'
        when s.action in ('clearance', 'punch')          then 'clearances'
        when s.action = 'shot_block'                     then 'blocks'
        when s.action = 'blocked_pass'                   then 'blocked_passes'
        when s.action in ('ball_recovery', 'keeper_pickup')
          or (s.action = 'claim' and s.is_successful)    then 'recoveries'
        when s.action = 'aerial'                         then 'aerials'
        when s.action = 'challenge'                      then 'dribbled_past'
        when s.action = 'foul'                           then 'fouls'
        when s.action = 'blocked_cross'                  then 'blocked_crosses'
    end                                                  as stat,
    case when s.x <= 33.3 then 'own_third'
         when s.x <= 66.6 then 'middle_third'
         else 'final_third' end                          as third,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_defensive_actions') }} s
left join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
