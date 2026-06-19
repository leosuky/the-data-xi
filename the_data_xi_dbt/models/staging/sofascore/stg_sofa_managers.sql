/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.manager_id,
    s.name,
    s.slug,
    s.shortname
from {{ source('raw','sofa_managers') }} s
