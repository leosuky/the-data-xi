-- Every fct_shots row must describe ONE real shot: where two providers are
-- joined on the same row, they must name the same player. A disagreement means
-- the cross-provider matching glued different shots together (the bug the
-- content-based matching in fct_shots replaced).
-- Refs fct_shots ONLY, so it runs with the marts step, not the staging step.
select shot_uid, combo_id, ws_player_uid, fot_player_uid, sofa_player_uid
from {{ ref('fct_shots') }}
where (ws_player_uid  is not null and fot_player_uid  is not null and ws_player_uid  <> fot_player_uid)
   or (fot_player_uid is not null and sofa_player_uid is not null and fot_player_uid <> sofa_player_uid)
   or (ws_player_uid  is not null and sofa_player_uid is not null and ws_player_uid  <> sofa_player_uid)
