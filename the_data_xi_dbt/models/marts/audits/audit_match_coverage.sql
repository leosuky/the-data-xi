{{ config(materialized='view') }}
-- Per-match provider coverage — the post-run audit surface. One row per match
-- with booleans for what loaded, and a ws_status that separates the three cases:
--   whoscored      — real WhoScored event stream (has_ws_events = true)
--   fbref_backfill — no event stream, but WhoScored summary tables were
--                    backfilled from FBref (ws lineups present, flag false)
--   no_whoscored   — no WhoScored data at all; backfill candidate if FBref exists
--
-- Quick worklist of matches needing attention:
--   select combo_id, match_date from {{ this }} where ws_status = 'no_whoscored';
with ws_lineups as (select distinct combo_id from {{ ref('stg_ws_lineups') }}),
     fot        as (select distinct combo_id from {{ ref('stg_fot_match') }}),
     sofa       as (select distinct combo_id from {{ ref('stg_sofa_matches') }}),
     odds       as (select distinct combo_id from {{ ref('stg_oddsp_odds') }}),
     fot_shots  as (select distinct combo_id from {{ ref('stg_fot_shots') }})
select
    dm.combo_id,
    dm.season_uid, dm.competition_uid, dm.match_date,
    dm.home_team_uid, dm.away_team_uid,
    dm.has_ws_events,
    (wl.combo_id is not null) as has_ws_lineups,
    (ft.combo_id is not null) as has_fotmob,
    (sf.combo_id is not null) as has_sofascore,
    (od.combo_id is not null) as has_odds,
    (fs.combo_id is not null) as has_fot_shots,
    case
        when dm.has_ws_events            then 'whoscored'
        when wl.combo_id is not null     then 'fbref_backfill'
        else                                  'no_whoscored'
    end as ws_status
from {{ ref('dim_match') }} dm
left join ws_lineups wl on wl.combo_id = dm.combo_id
left join fot        ft on ft.combo_id = dm.combo_id
left join sofa       sf on sf.combo_id = dm.combo_id
left join odds       od on od.combo_id = dm.combo_id
left join fot_shots  fs on fs.combo_id = dm.combo_id
