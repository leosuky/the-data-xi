{{ config(materialized='table') }}
-- Oddspedia decomposed odds (long). Subset of columns (decision #3).
select
    o.combo_id,
    o.market_name, o.period_name, o.line, o.outcome_name,
    o.odds_value, o.odds_direction,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_oddsp_odds') }} o
left join {{ ref('dim_match') }} dm on dm.combo_id = o.combo_id
