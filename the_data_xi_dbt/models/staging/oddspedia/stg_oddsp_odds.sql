/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.oddspedia_match_id,
    s.combo_id,
    s.market_name,
    s.market_group_id,
    s.market_slug,
    s.period_name,
    s.line,
    s.outcome_name,
    s.odds_value,
    s.odds_direction
from {{ source('raw','oddsp_odds') }} s
