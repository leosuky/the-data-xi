/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.team_id,
    s.name,
    s.stadium,
    s.stadium_capacity,
    s.country,
    s.city,
    s.latitude,
    s.longitude,
    s.founded_date,
    tx.team_uid
from {{ source('raw','sofa_teams') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
