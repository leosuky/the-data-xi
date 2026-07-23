/* trivial tier (player-grain): passthrough + typed row_id + uids + team_row_id.
   NB `on_goal_shot` STAYS TEXT — the parser writes a Python dict repr
   ("{'x': 1.5, ...}") which is not valid JSON, so it can't be cast to jsonb. */
with base as (
    select
        {{ passthrough_columns(source('raw','fot_shots'), 's', exclude=['id','ingested_at','row_id']) }},
        nullif(s.row_id, '')::int as row_id,
        px.player_uid,
        tx.team_uid
    from {{ source('raw','fot_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.fot_player_id = s.player_id
    left join {{ ref('int_team_xwalk') }}   tx on tx.fot_team_id   = s.team_id
)
-- Fotmob lacks seconds and mis-orders cross-team within a minute, but shot_id is
-- monotonic WITHIN a team, so per-team ordering by shot_id is exact.
select *,
       row_number() over (partition by combo_id, team_uid
                          order by shot_id) as team_row_id
from base
