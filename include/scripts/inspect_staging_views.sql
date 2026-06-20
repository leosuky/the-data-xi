/* =========================================================================
   Staging Layer QA Script
   Target: Materialized Views in `staging` schema
   Prefix: stg_
   ========================================================================= */

-- -------------------------------------------------------------------------
-- WhoScored (WS)
-- -------------------------------------------------------------------------
SELECT * FROM staging.stg_ws_avg_positions LIMIT 10;
SELECT * FROM staging.stg_ws_context_events LIMIT 10;
SELECT * FROM staging.stg_ws_formations LIMIT 10;
SELECT * FROM staging.stg_ws_game_states LIMIT 10;
SELECT * FROM staging.stg_ws_lineups LIMIT 10;
SELECT * FROM staging.stg_ws_match_meta LIMIT 10;
SELECT * FROM staging.stg_ws_pass_map LIMIT 10;
SELECT * FROM staging.stg_ws_pass_network LIMIT 10;
SELECT * FROM staging.stg_ws_player_defending LIMIT 10;
SELECT * FROM staging.stg_ws_player_discipline LIMIT 10;
SELECT * FROM staging.stg_ws_player_goalkeep LIMIT 10;
SELECT * FROM staging.stg_ws_player_passing LIMIT 10;
SELECT * FROM staging.stg_ws_player_passing_adv LIMIT 10;
SELECT * FROM staging.stg_ws_player_possession LIMIT 10;
SELECT * FROM staging.stg_ws_player_shooting LIMIT 10;
SELECT * FROM staging.stg_ws_sequences LIMIT 10;
SELECT * FROM staging.stg_ws_shots LIMIT 10;
SELECT * FROM staging.stg_ws_team_defending LIMIT 10;
SELECT * FROM staging.stg_ws_team_discipline LIMIT 10;
SELECT * FROM staging.stg_ws_team_passing LIMIT 10;
SELECT * FROM staging.stg_ws_team_passing_adv LIMIT 10;
SELECT * FROM staging.stg_ws_team_poss_adv LIMIT 10;
SELECT * FROM staging.stg_ws_team_possession LIMIT 10;
SELECT * FROM staging.stg_ws_team_shooting LIMIT 10;
SELECT * FROM staging.stg_ws_team_state_agg LIMIT 10;
SELECT * FROM staging.stg_ws_team_states LIMIT 10;

-- -------------------------------------------------------------------------
-- Sofascore (SOFA)
-- -------------------------------------------------------------------------
SELECT * FROM staging.stg_sofa_lineups LIMIT 10;
SELECT * FROM staging.stg_sofa_managers LIMIT 10;
SELECT * FROM staging.stg_sofa_match_stats LIMIT 10;
SELECT * FROM staging.stg_sofa_matches LIMIT 10;
SELECT * FROM staging.stg_sofa_missing_players LIMIT 10;
SELECT * FROM staging.stg_sofa_odds LIMIT 10;
SELECT * FROM staging.stg_sofa_player_stats LIMIT 10;
SELECT * FROM staging.stg_sofa_players LIMIT 10;
SELECT * FROM staging.stg_sofa_referees LIMIT 10;
SELECT * FROM staging.stg_sofa_seasons LIMIT 10;
SELECT * FROM staging.stg_sofa_shots LIMIT 10;
SELECT * FROM staging.stg_sofa_spatial LIMIT 10;
SELECT * FROM staging.stg_sofa_teams LIMIT 10;
SELECT * FROM staging.stg_sofa_tournaments LIMIT 10;

-- -------------------------------------------------------------------------
-- Fotmob (FOT)
-- -------------------------------------------------------------------------
SELECT * FROM staging.stg_fot_lineups LIMIT 10;
SELECT * FROM staging.stg_fot_match LIMIT 10;
SELECT * FROM staging.stg_fot_player_stats LIMIT 10;
SELECT * FROM staging.stg_fot_shots LIMIT 10;
SELECT * FROM staging.stg_fot_spatial LIMIT 10;
SELECT * FROM staging.stg_fot_team_stats LIMIT 10;

-- -------------------------------------------------------------------------
-- Oddspedia (ODDSP)
-- -------------------------------------------------------------------------
SELECT * FROM staging.stg_oddsp_odds LIMIT 10;