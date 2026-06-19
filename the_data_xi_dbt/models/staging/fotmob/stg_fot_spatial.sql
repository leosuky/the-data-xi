/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.fotmob_id,
    s.combo_id,
    s.momentum_data
from {{ source('raw','fot_spatial') }} s
