{{ config(materialized='table') }}
-- One row per referee (Sofascore referee_id anchor). Career disciplinary rates.
-- NOTE: depends on the raw-side conditional upsert (only keep the row with the
-- highest `games`), since Sofascore ships cumulative career stats per match.
select
    referee_id as referee_uid,
    name, nationality,
    games, yellow_cards, red_cards, double_yellow_cards
from {{ ref('stg_sofa_referees') }}
