/* trivial tier (flat): RAW already typed -> passthrough + uids. */
select
    s.combo_id,
    s.match_id,
    s.average_positions,
    s.commentary,
    s.match_momentum_graph,
    s.home_heatmap,
    s.away_heatmap,
    s.full_player_heatmaps
from {{ source('raw','sofa_spatial') }} s
