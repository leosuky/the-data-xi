/* trivial tier (flat): passthrough.
   NB `odds_value` STAYS TEXT — Sofascore ships FRACTIONAL odds ('12/5'), which
   are not castable. odds_decimal converts them (num/den + 1) for maths. */
select
    {{ passthrough_columns(source('raw','sofa_odds'), 's', exclude=['id','ingested_at']) }},
    case
        when s.odds_value ~ '^[0-9]+/[0-9]+$'
        then round(split_part(s.odds_value, '/', 1)::numeric
                 / nullif(split_part(s.odds_value, '/', 2)::numeric, 0) + 1, 4)
    end as odds_decimal
from {{ source('raw','sofa_odds') }} s
