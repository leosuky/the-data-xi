-- Every fct_shots row must describe ONE real shot: where two providers are
-- joined on the same row, they must name the same player. A disagreement means
-- the cross-provider matching glued different shots together (the bug the
-- content-based matching in fct_shots replaced).
select s.shot_uid, s.combo_id,
       w.player_uid as ws_player_uid, f.player_uid as fot_player_uid, so.player_uid as sofa_player_uid
from {{ ref('fct_shots') }} s
left join {{ ref('stg_ws_shots') }}   w  on w.combo_id  = s.combo_id and w.row_id   = s.ws_row_id
left join {{ ref('stg_fot_shots') }}  f  on f.combo_id  = s.combo_id and f.shot_id  = s.fot_shot_id
left join {{ ref('stg_sofa_shots') }} so on so.combo_id = s.combo_id and so.shot_id = s.sofa_shot_id
where (w.player_uid is not null and f.player_uid  is not null and w.player_uid <> f.player_uid)
   or (f.player_uid is not null and so.player_uid is not null and f.player_uid <> so.player_uid)
   or (w.player_uid is not null and so.player_uid is not null and w.player_uid <> so.player_uid)
