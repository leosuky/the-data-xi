/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.referee_id,
    s.name,
    s.nationality,
    s.red_cards,
    s.yellow_cards,
    s.double_yellow_cards,
    s.games
from {{ source('raw','sofa_referees') }} s
