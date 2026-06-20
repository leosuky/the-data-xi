{{ config(materialized='table') }}
select
    season_id as season_uid,
    name, year,
    tournament_id as competition_uid
from {{ ref('stg_sofa_seasons') }}
