{{ config(materialized='table') }}
-- Fotmob team summary, kept WIDE (home/away per period). home/away team_uid
-- attached for convenience (also available via dim_match).
select
    f.*,
    dm.home_team_uid, dm.away_team_uid,
    dm.season_uid, dm.competition_uid, dm.match_date
from {{ ref('stg_fot_team_stats') }} f
left join {{ ref('dim_match') }} dm on dm.combo_id = f.combo_id
