-- Opta alignment: new raw columns. Safe to re-run. Run BEFORE deploying the parsers:
-- the loader silently drops columns the table doesn't have.
-- docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi < include/scripts/migrate_opta_columns.sql
BEGIN;
ALTER TABLE raw.ws_player_carries
    ADD COLUMN IF NOT EXISTS progressive_distance_m NUMERIC(7,1),
    ADD COLUMN IF NOT EXISTS shot_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS goal_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS chance_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS assist_ending_carries INTEGER;
ALTER TABLE raw.ws_team_carries
    ADD COLUMN IF NOT EXISTS progressive_distance_m NUMERIC(8,1),
    ADD COLUMN IF NOT EXISTS shot_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS goal_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS chance_ending_carries INTEGER,
    ADD COLUMN IF NOT EXISTS assist_ending_carries INTEGER;
ALTER TABLE raw.ws_team_poss_adv
    ADD COLUMN IF NOT EXISTS ppda_passes INTEGER,
    ADD COLUMN IF NOT EXISTS ppda_actions INTEGER,
    ADD COLUMN IF NOT EXISTS high_turnover_goals INTEGER,
    ADD COLUMN IF NOT EXISTS build_up_goals INTEGER,
    ADD COLUMN IF NOT EXISTS direct_attack_goals INTEGER;
ALTER TABLE raw.ws_shots
    ADD COLUMN IF NOT EXISTS is_penalty BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_direct_freekick BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_set_piece BOOLEAN;
ALTER TABLE raw.ws_team_shooting
    ADD COLUMN IF NOT EXISTS shots_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS goals_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS shots_on_target_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS shots_direct_freekick INTEGER,
    ADD COLUMN IF NOT EXISTS goals_direct_freekick INTEGER,
    ADD COLUMN IF NOT EXISTS shots_set_piece INTEGER,
    ADD COLUMN IF NOT EXISTS goals_set_piece INTEGER,
    ADD COLUMN IF NOT EXISTS goals_by_subs INTEGER;
ALTER TABLE raw.ws_player_shooting
    ADD COLUMN IF NOT EXISTS shots_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS goals_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS shots_on_target_penalty INTEGER,
    ADD COLUMN IF NOT EXISTS shots_direct_freekick INTEGER,
    ADD COLUMN IF NOT EXISTS goals_direct_freekick INTEGER,
    ADD COLUMN IF NOT EXISTS shots_set_piece INTEGER,
    ADD COLUMN IF NOT EXISTS goals_set_piece INTEGER;
ALTER TABLE raw.ws_team_passing_adv
    ADD COLUMN IF NOT EXISTS pass_cone_forward_pct NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pass_cone_backward_pct NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pass_cone_left_pct NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pass_cone_right_pct NUMERIC(5,2);
ALTER TABLE raw.ws_player_passing_adv
    ADD COLUMN IF NOT EXISTS chances_created_open_play INTEGER,
    ADD COLUMN IF NOT EXISTS assists_open_play INTEGER,
    ADD COLUMN IF NOT EXISTS crosses_open_play_attempted INTEGER,
    ADD COLUMN IF NOT EXISTS crosses_open_play_accurate INTEGER;
ALTER TABLE raw.ws_team_discipline
    ADD COLUMN IF NOT EXISTS penalties_won INTEGER,
    ADD COLUMN IF NOT EXISTS penalties_conceded INTEGER,
    ADD COLUMN IF NOT EXISTS staff_yellow_cards INTEGER,
    ADD COLUMN IF NOT EXISTS staff_red_cards INTEGER;
ALTER TABLE raw.ws_player_discipline
    ADD COLUMN IF NOT EXISTS penalties_won INTEGER,
    ADD COLUMN IF NOT EXISTS penalties_conceded INTEGER;
COMMIT;
