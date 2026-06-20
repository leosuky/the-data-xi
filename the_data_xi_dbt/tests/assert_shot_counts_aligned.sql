-- Shot-count reconciler. Flags matches where a provider's shot count disagrees
-- with WhoScored (only when that provider actually has shots for the match, so
-- a missing-provider match doesn't fire). severity=warn: surfaces misalignment
-- without halting the pipeline, since row_id alignment depends on equal counts.
{{ config(severity='warn') }}
with c as (
    select combo_id,
        count(*) filter (where src='ws')   as ws_n,
        count(*) filter (where src='fot')  as fot_n,
        count(*) filter (where src='sofa') as sofa_n
    from (
        select combo_id, 'ws'   as src from {{ ref('stg_ws_shots') }}
        union all select combo_id, 'fot'  from {{ ref('stg_fot_shots') }}
        union all select combo_id, 'sofa' from {{ ref('stg_sofa_shots') }}
    ) u
    group by combo_id
)
select * from c
where (fot_n  > 0 and ws_n <> fot_n)
   or (sofa_n > 0 and ws_n <> sofa_n)
