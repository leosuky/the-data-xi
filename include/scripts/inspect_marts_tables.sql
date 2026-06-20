/* =========================================================================
   Marts Layer QA Script
   Target: Tables in `marts` schema
   ========================================================================= */

-- -------------------------------------------------------------------------
-- Dimensions (Entities & Context)
-- -------------------------------------------------------------------------
SELECT * FROM marts.dim_coach LIMIT 10;
SELECT * FROM marts.dim_competition LIMIT 10;
SELECT * FROM marts.dim_match LIMIT 10;
SELECT * FROM marts.dim_player LIMIT 10;
SELECT * FROM marts.dim_referee LIMIT 10;
SELECT * FROM marts.dim_season LIMIT 10;
SELECT * FROM marts.dim_spatial LIMIT 10;
SELECT * FROM marts.dim_team LIMIT 10;

-- -------------------------------------------------------------------------
-- Facts (Metrics & Events)
-- -------------------------------------------------------------------------
SELECT * FROM marts.fct_odds LIMIT 10;
SELECT * FROM marts.fct_player_defending LIMIT 10;
SELECT * FROM marts.fct_player_discipline LIMIT 10;
SELECT * FROM marts.fct_player_goalkeeping LIMIT 10;
SELECT * FROM marts.fct_player_match LIMIT 10;
SELECT * FROM marts.fct_player_passing LIMIT 10;
SELECT * FROM marts.fct_player_possession LIMIT 10;
SELECT * FROM marts.fct_player_shooting LIMIT 10;
SELECT * FROM marts.fct_shots LIMIT 10;
SELECT * FROM marts.fct_team_defending LIMIT 10;
SELECT * FROM marts.fct_team_discipline LIMIT 10;
SELECT * FROM marts.fct_team_match LIMIT 10;
SELECT * FROM marts.fct_team_match_state LIMIT 10;
SELECT * FROM marts.fct_team_passing LIMIT 10;
SELECT * FROM marts.fct_team_possession LIMIT 10;
SELECT * FROM marts.fct_team_shooting LIMIT 10;