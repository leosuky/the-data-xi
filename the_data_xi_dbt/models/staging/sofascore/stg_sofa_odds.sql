/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.match_id,
    s.selection,
    s.odds_value
from {{ source('raw','sofa_odds') }} s
