/* trivial tier (flat): raw sofa_odds is WIDE - one column per
   {period}_{market}_{selection}. Values are fractional-odds strings ('29/100')
   and text winning labels ('No','Over'), so this is a 1:1 passthrough, NOT a
   numeric cast. (Fractional->decimal conversion, if wanted, belongs in marts.) */
select
    {{ passthrough_columns(source('raw','sofa_odds'), 's', exclude=['id','ingested_at']) }}
from {{ source('raw','sofa_odds') }} s
