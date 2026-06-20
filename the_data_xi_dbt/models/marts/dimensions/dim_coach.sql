{{ config(materialized='table') }}
-- One row per coach. Anchored on Sofascore manager_id; doubles as the
-- Fotmob-coach <-> Sofascore-manager crosswalk (fot_coach_id column).
with pairs as (
    select f.combo_id, f.ht_coach_id as fot_coach_id, sm.home_manager_id as sofa_manager_id
    from {{ ref('stg_fot_match') }} f
    join {{ ref('stg_sofa_matches') }} sm using (combo_id)
    union all
    select f.combo_id, f.at_coach_id, sm.away_manager_id
    from {{ ref('stg_fot_match') }} f
    join {{ ref('stg_sofa_matches') }} sm using (combo_id)
),
vote as (
    select sofa_manager_id, fot_coach_id,
           row_number() over (partition by sofa_manager_id order by count(*) desc) rn
    from pairs
    where sofa_manager_id is not null and fot_coach_id is not null
    group by sofa_manager_id, fot_coach_id
)
select
    m.manager_id as coach_uid,
    m.name, m.shortname, m.slug,
    v.fot_coach_id
from {{ ref('stg_sofa_managers') }} m
left join vote v on v.sofa_manager_id = m.manager_id and v.rn = 1
