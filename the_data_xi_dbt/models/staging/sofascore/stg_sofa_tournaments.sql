/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.tournament_id,
    s.name,
    s.country,
    s.tier
from {{ source('raw','sofa_tournaments') }} s
