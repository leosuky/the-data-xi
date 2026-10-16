-- Shots where one provider names a WhoScored-anchored player and another names
-- a Sofascore-fallback (negative) player: the same person ended up with two
-- player_uids because his Sofascore id wasn't linked to his WhoScored id
-- (e.g. shirt number and name both differ in that match). Harmless for the
-- shot join, but his Sofascore-side stats sit under the wrong uid. Fix via the
-- player crosswalk / player_alias seed when it matters. severity=warn.
{{ config(severity='warn') }}
select shot_uid, combo_id, player_name, ws_player_uid, fot_player_uid, sofa_player_uid
from {{ ref('fct_shots') }}
where (ws_player_uid  is not null and fot_player_uid  is not null and ws_player_uid  <> fot_player_uid
       and least(ws_player_uid, fot_player_uid) < 0)
   or (fot_player_uid is not null and sofa_player_uid is not null and fot_player_uid <> sofa_player_uid
       and least(fot_player_uid, sofa_player_uid) < 0)
   or (ws_player_uid  is not null and sofa_player_uid is not null and ws_player_uid  <> sofa_player_uid
       and least(ws_player_uid, sofa_player_uid) < 0)
