-- Every fct_shots row must describe ONE real shot: where two providers are
-- joined on the same row, they must name the same player. A disagreement
-- between two real (WhoScored-anchored, positive) player ids means the
-- cross-provider matching glued different shots together - that fails.
-- A disagreement involving a negative (Sofascore-fallback) id is a crosswalk
-- GAP instead (same player, his Sofascore id just isn't linked yet) - those are
-- reported by assert_player_xwalk_gaps (warn), not here.
-- Refs fct_shots ONLY, so it runs with the marts step, not the staging step.
select shot_uid, combo_id, ws_player_uid, fot_player_uid, sofa_player_uid
from {{ ref('fct_shots') }}
where (ws_player_uid  > 0 and fot_player_uid  > 0 and ws_player_uid  <> fot_player_uid)
   or (fot_player_uid > 0 and sofa_player_uid > 0 and fot_player_uid <> sofa_player_uid)
   or (ws_player_uid  > 0 and sofa_player_uid > 0 and ws_player_uid  <> sofa_player_uid)
