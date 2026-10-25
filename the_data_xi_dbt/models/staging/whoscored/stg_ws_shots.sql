/* trivial tier (player-grain): passthrough + typed row_id + uids + team_row_id. */
with base as (
    select
        {{ passthrough_columns(source('raw','ws_shots'), 's', exclude=['id','ingested_at','row_id']) }},
        nullif(s.row_id, '')::int as row_id,
        coalesce(px.player_uid, s.player_id) as player_uid,
        coalesce(tx.team_uid, s.team_id)     as team_uid
    from {{ source('raw','ws_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.ws_player_id = s.player_id
    left join {{ ref('int_team_xwalk') }}   tx on tx.ws_team_id   = s.team_id
)
-- WhoScored carries minute+second, so ordering is exact.
-- Own goals: the parser keeps them in this per-shot table with is_goal false
-- (an own goal is not the player's goal). Every other shot is exactly one of
-- goal / on target / blocked / off target, so a row that is none of them is an
-- own goal. is_goal_scored = the ball went in, either end. Rows loaded by the
-- older parser carry is_goal true for own goals; both shapes give the same
-- is_goal_scored (their own-goal flag then comes from context events).
select *,
       ( not coalesce(is_goal, false) and not coalesce(is_on_target, false)
         and not coalesce(is_blocked, false) and not coalesce(is_off_target, false)
       ) as is_own_goal,
       ( coalesce(is_goal, false)
         or ( not coalesce(is_on_target, false) and not coalesce(is_blocked, false)
              and not coalesce(is_off_target, false) )
       ) as is_goal_scored,
       row_number() over (partition by combo_id, team_uid
                          order by minute, second, row_id) as team_row_id
from base
