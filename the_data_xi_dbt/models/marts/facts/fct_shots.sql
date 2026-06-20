{{ config(materialized='table') }}
-- One row per shot, keyed (combo_id, row_id). WhoScored spine + Fotmob xg +
-- both providers' coordinates (prefixed) + Sofascore shot-id mapping (decision #4).
select
    w.combo_id || '-' || w.row_id as shot_uid,
    {{ passthrough_columns(ref('stg_ws_shots'), 'w',
         exclude=['x','y','goal_mouth_y','goal_mouth_z','xgot','expanded_minute']) }},
    -- WhoScored coordinates (prefixed)
    w.x as ws_x, w.y as ws_y, w.goal_mouth_y as ws_goal_mouth_y, w.goal_mouth_z as ws_goal_mouth_z,
    -- Match timing: expanded_minute comes from Fotmob (its `minute`), plus minute_added
    f.minute as expanded_minute, f.minute_added,
    -- Fotmob xg + coordinates (prefixed) + extras
    f.xg, f.xgot,
    f.x as fot_x, f.y as fot_y,
    f.goal_crossed_y as fot_goal_crossed_y, f.goal_crossed_z as fot_goal_crossed_z,
    f.blocked_x as fot_blocked_x, f.blocked_y as fot_blocked_y,
    f.shot_type as fot_shot_type, f.situation as fot_situation, f.keeper_id as fot_keeper_id,
    -- Sofascore mapping (reference only)
    so.shot_id as sofa_shot_id, so.xg as sofa_xg, so.xgot as sofa_xgot,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_ws_shots') }} w
left join {{ ref('stg_fot_shots') }}  f  on f.combo_id  = w.combo_id and f.row_id  = w.row_id
left join {{ ref('stg_sofa_shots') }} so on so.combo_id = w.combo_id and so.row_id = w.row_id
left join {{ ref('dim_match') }} dm on dm.combo_id = w.combo_id