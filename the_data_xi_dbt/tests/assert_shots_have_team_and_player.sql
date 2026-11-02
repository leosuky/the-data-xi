-- Every fct_shots row needs a team, a player and a minute: a row without one
-- drops out of team totals and breaks match pages. Sofascore-only shots by a
-- player missing from the crosswalk used to lose all three (~130 shots).
-- severity=warn: it reports, it never blocks the marts.
{{ config(severity='warn') }}
select shot_uid, combo_id, team_uid, player_uid, minute,
       sofa_shot_id, fot_shot_id, ws_row_id
from {{ ref('fct_shots') }}
where team_uid is null or player_uid is null or minute is null
