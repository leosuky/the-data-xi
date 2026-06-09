-- ============================================================================
-- DataXI RAW Schema - Table Inspection Queries
-- ============================================================================

-- Set the schema path if you haven't already in your session
-- SET search_path TO raw;

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  WHOSCORED (ws_)                                                           ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- Match metadata & Lineups
SELECT * FROM raw.ws_match_meta LIMIT 10;
SELECT * FROM raw.ws_lineups where combo_id = '2022-10-29NapSas';
SELECT * FROM raw.ws_formations LIMIT 10;

-- Team-level Stats
SELECT * FROM raw.ws_team_passing LIMIT 10;
SELECT * FROM raw.ws_team_passing_adv LIMIT 10;
SELECT * FROM raw.ws_team_defending LIMIT 10;
-- more data for defending win percentages by zone.
SELECT * FROM raw.ws_team_discipline LIMIT 10;
SELECT * FROM raw.ws_team_possession LIMIT 10;
SELECT * FROM raw.ws_team_poss_adv LIMIT 10;
-- rework turnover pct

-- Player-level Stats
SELECT * FROM raw.ws_player_passing where combo_id = '2022-10-29NapSas';
SELECT * FROM raw.ws_player_passing_adv where combo_id = '2022-10-29NapSas';
SELECT * FROM raw.ws_player_defending where combo_id = '2022-10-29NapSas';
-- discrepancy in duels.

SELECT * FROM raw.ws_player_goalkeep where combo_id = '2022-10-29NapSas';
SELECT * FROM raw.ws_player_discipline where combo_id = '2022-10-29NapSas';
SELECT * FROM raw.ws_player_possession where combo_id = '2022-10-29NapSas';
-- dribbles lost discrepancy


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  FOTMOB (fot_)                                                             ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- SELECT * FROM raw.fot_match LIMIT 10;
-- SELECT * FROM raw.fot_lineups LIMIT 10;
-- SELECT * FROM raw.fot_player_stats LIMIT 10;
-- SELECT * FROM raw.fot_team_stats LIMIT 10;
-- SELECT * FROM raw.fot_shots LIMIT 10;
-- SELECT * FROM raw.fot_spatial LIMIT 10;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  SOFASCORE (sofa_)                                                         ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- Core Entities
-- SELECT * FROM raw.sofa_tournaments LIMIT 10;
-- SELECT * FROM raw.sofa_seasons LIMIT 10;
-- SELECT * FROM raw.sofa_teams LIMIT 10;
-- SELECT * FROM raw.sofa_players LIMIT 10;
-- SELECT * FROM raw.sofa_referees LIMIT 10;
-- SELECT * FROM raw.sofa_managers LIMIT 10;

-- Match Data & Lineups
-- SELECT * FROM raw.sofa_matches LIMIT 10;
-- SELECT * FROM raw.sofa_lineups LIMIT 10;
-- SELECT * FROM raw.sofa_missing_players LIMIT 10;

-- Match Events & Stats
-- SELECT * FROM raw.sofa_player_stats LIMIT 10;
-- SELECT * FROM raw.sofa_match_stats LIMIT 10;
-- SELECT * FROM raw.sofa_shots LIMIT 10;
-- SELECT * FROM raw.sofa_odds LIMIT 10;
-- SELECT * FROM raw.sofa_spatial LIMIT 10;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  ODDSPEDIA (oddsp_)                                                        ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- SELECT * FROM raw.oddsp_odds LIMIT 10;