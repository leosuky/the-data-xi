{{ config(materialized='table') }}
select
    tournament_id as competition_uid,
    name, country, tier
from {{ ref('stg_sofa_tournaments') }}
