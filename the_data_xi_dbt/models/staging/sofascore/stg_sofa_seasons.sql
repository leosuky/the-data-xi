/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.season_id,
    s.name,
    s.year,
    s.tournament_id
from {{ source('raw','sofa_seasons') }} s
