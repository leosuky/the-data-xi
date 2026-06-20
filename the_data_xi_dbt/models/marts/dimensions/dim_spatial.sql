{{ config(materialized='table') }}
-- One row per match: combined viz payloads (heatmaps, momentum, avg positions),
-- provider-prefixed. Nothing joins on it; it's a per-match spatial lookup store.
select
    s.combo_id,
    s.average_positions      as sofa_average_positions,
    s.home_heatmap           as sofa_home_heatmap,
    s.away_heatmap           as sofa_away_heatmap,
    s.full_player_heatmaps   as sofa_full_player_heatmaps,
    s.match_momentum_graph   as sofa_match_momentum_graph,
    s.commentary             as sofa_commentary,
    f.momentum_data          as fot_momentum_data
from {{ ref('stg_sofa_spatial') }} s
full outer join {{ ref('stg_fot_spatial') }} f on f.combo_id = s.combo_id
