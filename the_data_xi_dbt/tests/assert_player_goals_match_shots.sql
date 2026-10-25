-- A player's goals and shots in fct_player_shooting must equal his WhoScored
-- shot rows in fct_shots, own goals excluded (an own goal is neither a goal nor
-- a shot for the player who scored it). Catches own goals creeping back into
-- player totals, and lost or duplicated shot rows. WhoScored matches only.
-- severity=warn: it reports, it never blocks the marts.
{{ config(severity='warn') }}
with shots as (
    select combo_id, player_uid,
           sum(case when is_goal and not is_own_goal then 1 else 0 end) as goals,
           sum(case when not is_own_goal then 1 else 0 end)             as shots
    from {{ ref('fct_shots') }}
    where ws_row_id is not null
    group by combo_id, player_uid
)
select ps.combo_id, ps.player_uid,
       ps.goals, coalesce(s.goals, 0) as shot_goals,
       ps.total_shots, coalesce(s.shots, 0) as shot_rows
from {{ ref('fct_player_shooting') }} ps
left join shots s using (combo_id, player_uid)
where ps.goals is not null                      -- has WhoScored shooting
  and (ps.goals <> coalesce(s.goals, 0) or ps.total_shots <> coalesce(s.shots, 0))
