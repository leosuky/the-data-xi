{{ config(materialized='table') }}
-- Slim per-(player,match) spine: identity, minutes, started, captain, and
-- position from ALL THREE providers. PROVIDER-AGNOSTIC spine — a player-match
-- exists if ANY provider has them, so Sofascore/Fotmob positions, ratings and
-- minutes still appear for a WhoScored-absent match (ws_position etc. NULL).
-- Identity (team_uid, name, home, starter, captain) is coalesced across
-- providers. Stores BOTH ratings prefixed. match_rating is a dynamic sofa stat
-- column, so reference it only if present.
--
-- squad_status: starter | sub_used | unused_sub | unavailable. Fotmob also lists
-- injured/suspended players (role 'unavailable', with unavailable_type/_reason);
-- they get a row here so availability history is queryable, but filter them out
-- of anything that should count only matchday squads.
-- minutes_played: Sofascore minutes; 0 for unused subs and unavailable players
-- (Sofascore leaves those NULL); NULL only if a starter's minutes are unknown.
-- shirt_number: the number worn in THIS match (WhoScored, else Fotmob, else
-- Sofascore lineups); NULL when no provider has it. Cast defensively: columns
-- auto-added by the loader arrive as TEXT.
{% set sps_cols = adapter.get_columns_in_relation(ref('stg_sofa_player_stats'))
                  | map(attribute='name') | list %}
{% set fps_cols = adapter.get_columns_in_relation(ref('stg_fot_player_stats'))
                  | map(attribute='name') | list %}
with ws as (
    select combo_id, player_uid, team_uid, player_name, is_home_team,
           is_starter, is_captain, position as ws_position,
           subbed_in_minute, subbed_out_minute,
           nullif(shirt_number::text, '')::numeric::int as shirt_number
    from {{ ref('stg_ws_lineups') }}
),
fot as (
    select combo_id, player_uid, team_uid, player_name,
           is_home_team, is_captain,
           position as fot_position, usual_position as fot_usual_position,
           position_id as fot_position_id, usual_position_id as fot_usual_position_id,
           rating as fot_rating,
           role as fot_role, unavailable_type, unavailable_reason,
           nullif(shirt_number::text, '')::numeric::int as shirt_number
    from {{ ref('stg_fot_lineups') }}
),
sofa as (
    select combo_id, player_uid, team_uid, is_home_team, is_starter, is_captain,
           position as sofa_position, minutes_played,
           nullif(shirt_number::text, '')::numeric::int as shirt_number
    from {{ ref('stg_sofa_lineups') }}
),
sofa_rating as (
    select combo_id, player_uid,
           {% if 'match_rating' in sps_cols %}match_rating{% else %}null::numeric{% endif %} as sofa_rating
    from {{ ref('stg_sofa_player_stats') }}
),
fot_stats as (
    -- Fotmob per-match player stats not available from WhoScored events
    select combo_id, player_uid,
           {% if 'expected_assists' in fps_cols %}max(expected_assists::numeric){% else %}null::numeric{% endif %} as expected_assists
    from {{ ref('stg_fot_player_stats') }}
    where player_uid is not null
    group by combo_id, player_uid
),
keys as (
    select combo_id, player_uid from ws   where player_uid is not null
    union
    select combo_id, player_uid from fot  where player_uid is not null
    union
    select combo_id, player_uid from sofa where player_uid is not null
)
select
    k.combo_id, k.player_uid,
    coalesce(w.team_uid, f.team_uid, s.team_uid) as team_uid,
    dm.season_uid, dm.competition_uid, dm.match_date,
    coalesce(w.player_name, f.player_name)              as player_name,
    coalesce(w.is_home_team, f.is_home_team, s.is_home_team) as is_home_team,
    case when f.fot_role = 'unavailable' then false
         else coalesce(w.is_starter, s.is_starter, f.fot_role = 'starter')
    end                                                 as is_starter,
    coalesce(w.is_captain, f.is_captain, s.is_captain)  as is_captain,
    coalesce(w.shirt_number, f.shirt_number, s.shirt_number) as shirt_number,
    case
        when f.fot_role = 'unavailable' then 'unavailable'
        when coalesce(w.is_starter, s.is_starter, f.fot_role = 'starter') then 'starter'
        when coalesce(s.minutes_played, 0) > 0 or w.subbed_in_minute is not null then 'sub_used'
        else 'unused_sub'
    end                                                 as squad_status,
    case
        when s.minutes_played is not null then s.minutes_played
        when f.fot_role = 'unavailable' then 0
        when coalesce(w.is_starter, s.is_starter, f.fot_role = 'starter') then null
        else 0
    end                                                 as minutes_played,
    f.unavailable_type, f.unavailable_reason,
    w.subbed_in_minute, w.subbed_out_minute,
    w.ws_position, s.sofa_position, f.fot_position, f.fot_usual_position,
    f.fot_position_id, f.fot_usual_position_id,
    f.fot_rating,
    sr.sofa_rating,
    fs.expected_assists                                 -- xA (Fotmob)
from keys k
left join ws          w  on w.combo_id  = k.combo_id and w.player_uid  = k.player_uid
left join fot         f  on f.combo_id  = k.combo_id and f.player_uid  = k.player_uid
left join sofa        s  on s.combo_id  = k.combo_id and s.player_uid  = k.player_uid
left join sofa_rating sr on sr.combo_id = k.combo_id and sr.player_uid = k.player_uid
left join fot_stats   fs on fs.combo_id = k.combo_id and fs.player_uid = k.player_uid
left join {{ ref('dim_match') }} dm on dm.combo_id = k.combo_id
