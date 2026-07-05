{{ config(materialized='table') }}
-- One row per shot, keyed (combo_id, team_uid, team_row_id). TEAM-BASED spine:
-- shots are numbered per team in staging (Fotmob mis-orders cross-team within a
-- minute because it lacks seconds; within a team its shot_id is monotonic). Each
-- provider is ordered by its own exact key, so the Nth shot of a team aligns
-- across providers. PROVIDER-AGNOSTIC: a shot exists if ANY provider logged it,
-- so Fotmob/Sofascore shots still appear for a WhoScored-absent match (ws_* NULL).
with shot_keys as (
    select combo_id, team_uid, team_row_id from {{ ref('stg_ws_shots') }}
    union
    select combo_id, team_uid, team_row_id from {{ ref('stg_fot_shots') }}
    union
    select combo_id, team_uid, team_row_id from {{ ref('stg_sofa_shots') }}
)
select
    k.combo_id || '-' || k.team_uid || '-' || k.team_row_id as shot_uid,
    k.combo_id, k.team_uid, k.team_row_id,
    coalesce(w.player_uid, f.player_uid, so.player_uid) as player_uid,
    -- is_goal: WhoScored when present, else Fotmob's event_type = 'Goal'
    coalesce(w.is_goal, f.event_type = 'Goal')          as is_goal,
    {{ passthrough_columns(ref('stg_ws_shots'), 'w',
         exclude=['combo_id','row_id','team_row_id','player_uid','team_uid','is_goal',
                  'x','y','goal_mouth_y','goal_mouth_z','xgot','expanded_minute']) }},
    -- WhoScored coordinates (prefixed)
    w.x as ws_x, w.y as ws_y, w.goal_mouth_y as ws_goal_mouth_y, w.goal_mouth_z as ws_goal_mouth_z,
    -- Match timing from Fotmob
    f.minute as expanded_minute, f.minute_added,
    -- Fotmob xg + coordinates + extras
    f.xg, f.xgot,
    f.x as fot_x, f.y as fot_y,
    f.goal_crossed_y as fot_goal_crossed_y, f.goal_crossed_z as fot_goal_crossed_z,
    f.blocked_x as fot_blocked_x, f.blocked_y as fot_blocked_y,
    f.shot_type as fot_shot_type, f.situation as fot_situation, f.keeper_id as fot_keeper_id,
    f.event_type as fot_event_type, f.player_name as fot_player_name,
    f.is_blocked as fot_is_blocked, f.is_on_target as fot_is_on_target,
    f.is_own_goal as fot_is_own_goal,
    -- Sofascore mapping + flags (prefixed)
    so.shot_id as sofa_shot_id, so.xg as sofa_xg, so.xgot as sofa_xgot,
    so.ishome as sofa_is_home, so.goaltype as sofa_goaltype,
    dm.season_uid, dm.competition_uid, dm.match_date
from shot_keys k
left join {{ ref('stg_ws_shots') }}   w  on w.combo_id  = k.combo_id and w.team_uid  = k.team_uid and w.team_row_id  = k.team_row_id
left join {{ ref('stg_fot_shots') }}  f  on f.combo_id  = k.combo_id and f.team_uid  = k.team_uid and f.team_row_id  = k.team_row_id
left join {{ ref('stg_sofa_shots') }} so on so.combo_id = k.combo_id and so.team_uid = k.team_uid and so.team_row_id = k.team_row_id
left join {{ ref('dim_match') }} dm on dm.combo_id = k.combo_id
