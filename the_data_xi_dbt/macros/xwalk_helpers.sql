{# Emit the exact->elimination->name resolution + voting CTEs for one provider.
   Expects an anchor CTE (default `ws`; cols: combo_id,is_home_team,shirt_number,ws_id,nname)
   and a CTE named <prov> (cols: combo_id,is_home_team,shirt_number,prov_id,nname)
   to already exist in the model. Produces vote_<prov>(ws_id,prov_id,n,rn) — or
   <anchor>_vote_<prov> when a non-default anchor is passed (so a model can
   resolve against a second anchor, e.g. Sofascore-only players, without CTE
   name clashes). The anchor's id column is always called ws_id. #}
{% macro xwalk_resolve_provider(prov, anchor='ws') %}
    {%- set pfx = '' if anchor == 'ws' else anchor ~ '_' -%}
    {{pfx}}exact_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from {{anchor}} w join {{prov}} p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
         and w.shirt_number=p.shirt_number and w.shirt_number is not null
    ),
    {{pfx}}ws_r_{{prov}} as (
        select * from {{anchor}} w where not exists (
          select 1 from {{pfx}}exact_{{prov}} e
          where e.combo_id=w.combo_id and e.is_home_team=w.is_home_team and e.ws_id=w.ws_id)
    ),
    {{pfx}}pv_r_{{prov}} as (
        select * from {{prov}} p where not exists (
          select 1 from {{pfx}}exact_{{prov}} e
          where e.combo_id=p.combo_id and e.is_home_team=p.is_home_team and e.prov_id=p.prov_id)
    ),
    {{pfx}}elim_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from (select *, count(*) over (partition by combo_id,is_home_team) cc from {{pfx}}ws_r_{{prov}}) w
        join (select *, count(*) over (partition by combo_id,is_home_team) cc from {{pfx}}pv_r_{{prov}}) p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
        where w.cc=1 and p.cc=1
    ),
    {{pfx}}ws_r2_{{prov}} as (
        select * from {{pfx}}ws_r_{{prov}} w where not exists (
          select 1 from {{pfx}}elim_{{prov}} e
          where e.combo_id=w.combo_id and e.is_home_team=w.is_home_team and e.ws_id=w.ws_id)
    ),
    {{pfx}}pv_r2_{{prov}} as (
        select * from {{pfx}}pv_r_{{prov}} p where not exists (
          select 1 from {{pfx}}elim_{{prov}} e
          where e.combo_id=p.combo_id and e.is_home_team=p.is_home_team and e.prov_id=p.prov_id)
    ),
    {{pfx}}name_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from {{pfx}}ws_r2_{{prov}} w join {{pfx}}pv_r2_{{prov}} p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
         and w.nname=p.nname and length(w.nname) > 0
    ),
    {{pfx}}obs_{{prov}} as (
        select ws_id, prov_id from {{pfx}}exact_{{prov}}
        union all select ws_id, prov_id from {{pfx}}elim_{{prov}}
        union all select ws_id, prov_id from {{pfx}}name_{{prov}}
    ),
    {{pfx}}vote_{{prov}} as (
        select ws_id, prov_id, count(*) as n,
               row_number() over (partition by ws_id order by count(*) desc, prov_id) rn
        from {{pfx}}obs_{{prov}} group by ws_id, prov_id
    )
{% endmacro %}

{# Team voting CTE for one provider. Expects CTEs `ws` and <prov>
   with (combo_id,is_home_team, ws_id|prov_id). #}
{% macro xwalk_vote_team(prov) %}
    vote_{{prov}} as (
        select w.ws_id, p.prov_id,
               row_number() over (partition by w.ws_id order by count(*) desc, p.prov_id) rn
        from ws w join {{prov}} p using (combo_id, is_home_team)
        group by w.ws_id, p.prov_id
    )
{% endmacro %}
