{# Emit the exact->elimination->name resolution + voting CTEs for one provider.
   Expects CTEs `ws` (cols: combo_id,is_home_team,shirt_number,ws_id,nname)
   and a CTE named <prov> (cols: combo_id,is_home_team,shirt_number,prov_id,nname)
   to already exist in the model. Produces vote_<prov>(ws_id,prov_id,rn). #}
{% macro xwalk_resolve_provider(prov) %}
    exact_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from ws w join {{prov}} p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
         and w.shirt_number=p.shirt_number and w.shirt_number is not null
    ),
    ws_r_{{prov}} as (
        select * from ws w where not exists (
          select 1 from exact_{{prov}} e
          where e.combo_id=w.combo_id and e.is_home_team=w.is_home_team and e.ws_id=w.ws_id)
    ),
    pv_r_{{prov}} as (
        select * from {{prov}} p where not exists (
          select 1 from exact_{{prov}} e
          where e.combo_id=p.combo_id and e.is_home_team=p.is_home_team and e.prov_id=p.prov_id)
    ),
    elim_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from (select *, count(*) over (partition by combo_id,is_home_team) cc from ws_r_{{prov}}) w
        join (select *, count(*) over (partition by combo_id,is_home_team) cc from pv_r_{{prov}}) p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
        where w.cc=1 and p.cc=1
    ),
    ws_r2_{{prov}} as (
        select * from ws_r_{{prov}} w where not exists (
          select 1 from elim_{{prov}} e
          where e.combo_id=w.combo_id and e.is_home_team=w.is_home_team and e.ws_id=w.ws_id)
    ),
    pv_r2_{{prov}} as (
        select * from pv_r_{{prov}} p where not exists (
          select 1 from elim_{{prov}} e
          where e.combo_id=p.combo_id and e.is_home_team=p.is_home_team and e.prov_id=p.prov_id)
    ),
    name_{{prov}} as (
        select w.combo_id, w.is_home_team, w.ws_id, p.prov_id
        from ws_r2_{{prov}} w join pv_r2_{{prov}} p
          on w.combo_id=p.combo_id and w.is_home_team=p.is_home_team
         and w.nname=p.nname and length(w.nname) > 0
    ),
    obs_{{prov}} as (
        select ws_id, prov_id from exact_{{prov}}
        union all select ws_id, prov_id from elim_{{prov}}
        union all select ws_id, prov_id from name_{{prov}}
    ),
    vote_{{prov}} as (
        select ws_id, prov_id,
               row_number() over (partition by ws_id order by count(*) desc, prov_id) rn
        from obs_{{prov}} group by ws_id, prov_id
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
