{{ config(materialized='table') }}
/*
  int_player_xwalk - ws-anchored player identity.
  ------------------------------------------------------------------
  player_uid = WhoScored player_id (always present, stable, debuggable),
  optionally overridden via the player_alias seed (COALESCE seam).
  sofa_player_id / fot_player_id are resolved ATTRIBUTES, not part of identity,
  so correcting a provider mapping never changes a player's uid.

  Resolution per (combo_id, is_home_team) match-side, then GLOBAL voting:
    tier 1  exact (shirt_number)
    tier 2  elimination (exactly one slot unmatched per side)
    tier 3  normalized name
  Most-frequent (ws_id, prov_id) pairing across all matches wins - volume
  outvotes the occasional swapped/mistyped shirt.

  Reads raw lineups via source() (NOT the stg_*_lineups views) on purpose:
  the stg lineup views attach player_uid, which would create a cycle.
*/

with ws as (
    select combo_id, is_home_team, shirt_number,
           player_id as ws_id,
           {{ normalize_name('player_name') }} as nname
    from {{ source('raw', 'ws_lineups') }}
),

-- -- provider lineups normalized to (combo_id, is_home_team, shirt, prov_id, nname)
sofa as (
    select sl.combo_id, sl.is_home_team, sl.shirt_number,
           sl.player_id as prov_id,
           {{ normalize_name('sp.name') }} as nname
    from {{ source('raw', 'sofa_lineups') }} sl
    left join {{ source('raw', 'sofa_players') }} sp on sp.player_id = sl.player_id
),
fot as (
    select combo_id, is_home_team, shirt_number,
           player_id as prov_id,
           {{ normalize_name('player_name') }} as nname
    from {{ source('raw', 'fot_lineups') }}
),

{# resolve one provider against the ws anchor -> winning (ws_id, prov_id) #}


{{ xwalk_resolve_provider('sofa') }},
{{ xwalk_resolve_provider('fot') }},

ws_universe as (select distinct ws_id from ws),

joined as (
    select u.ws_id as ws_player_id,
           s.prov_id as sofa_player_id,
           f.prov_id as fot_player_id
    from ws_universe u
    left join vote_sofa s on s.ws_id=u.ws_id and s.rn=1
    left join vote_fot  f on f.ws_id=u.ws_id and f.rn=1
)

select
    coalesce(a.canonical_ws_id, j.ws_player_id) as player_uid,
    j.ws_player_id,
    j.sofa_player_id,
    j.fot_player_id
from joined j
left join {{ ref('player_alias') }} a on a.bad_ws_id = j.ws_player_id
