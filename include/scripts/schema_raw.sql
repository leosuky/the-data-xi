-- ============================================================================
-- DataXII RAW Schema
-- ============================================================================
-- All data from all providers lands here as-is, prefixed by provider.
-- No foreign keys, no cross-provider joins — that happens in staging.
-- Every table has: combo_id (TEXT NOT NULL) for cross-provider linking.
--
-- Providers:
--   ws_     = WhoScored  (event-derived stats via custom parsers)
--   fot_    = Fotmob     (match details, lineups, shots, momentum)
--   sofa_   = Sofascore  (lineups, player stats, shotmaps, spatial)
--   oddsp_  = Oddspedia  (pre-match odds across 17 markets)
--
-- Idempotency:
--   Team/player stat tables use (combo_id, team_id) or (combo_id, player_id)
--   as conflict keys for upsert.
--   Event tables (sequences, pass_map, shots) use DELETE + INSERT per combo_id.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
SET search_path TO raw;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  WHOSCORED (ws_)                                                         ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ── Match metadata ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_match_meta (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    home_team               TEXT,
    home_team_id            INTEGER,
    away_team               TEXT,
    away_team_id            INTEGER,
    score                   TEXT,
    ht_score                TEXT,
    venue                   TEXT,
    attendance              INTEGER,
    referee                 TEXT,
    start_time              TEXT,
    UNIQUE (combo_id)
);

-- ── Lineups ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_lineups (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    player_id               INTEGER NOT NULL,
    player_name             TEXT,
    team_id                 INTEGER,
    team_name               TEXT,
    is_home_team            BOOLEAN,
    shirt_number            INTEGER,
    position                TEXT,
    is_starter              BOOLEAN,
    is_captain              BOOLEAN,
    is_man_of_the_match     BOOLEAN,
    age                     INTEGER,
    height_cm               INTEGER,
    weight_kg               INTEGER,
    subbed_in_minute        INTEGER,
    subbed_in_for           INTEGER,
    subbed_out_minute       INTEGER,
    subbed_out_for          INTEGER,
    UNIQUE (combo_id, player_id)
);

-- ── Formation changes (unique to WhoScored) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_formations (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    team_id                 INTEGER,
    team_name               TEXT,
    is_home_team            BOOLEAN,
    formation_index         INTEGER,
    formation_name          TEXT,
    formation_id            INTEGER,
    start_minute            INTEGER,
    end_minute              INTEGER,
    captain_player_id       INTEGER,
    captain_name            TEXT,
    sub_on_player_id        INTEGER,
    sub_off_player_id       INTEGER,
    player_ids              JSONB,
    jersey_numbers          JSONB,
    UNIQUE (combo_id, team_id, formation_index)
);

-- ── Team-level passing ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_passing (
    id                              BIGSERIAL PRIMARY KEY,
    whoscored_match_id              INTEGER NOT NULL,
    combo_id                        TEXT NOT NULL,
    team_id                         INTEGER NOT NULL,
    passes_total                    INTEGER,
    passes_accurate                 INTEGER,
    passes_inaccurate               INTEGER,
    pass_accuracy_pct               NUMERIC(5,2),
    longballs_attempted             INTEGER,
    longballs_accurate              INTEGER,
    longball_accuracy_pct           NUMERIC(5,2),
    throughballs                    INTEGER,
    head_passes                     INTEGER,
    layoffs                         INTEGER,
    passes_short                    INTEGER,
    passes_medium                   INTEGER,
    passes_long_band                INTEGER,
    freekick_passes                 INTEGER,
    indirect_fk_passes              INTEGER,
    goalkick_passes                 INTEGER,
    crosses_attempted               INTEGER,
    crosses_accurate                INTEGER,
    cross_accuracy_pct              NUMERIC(5,2),
    corner_deliveries               INTEGER,
    corner_deliveries_accurate      INTEGER,
    throwins_attempted              INTEGER,
    throwins_accurate               INTEGER,
    keeper_throws_attempted         INTEGER,
    keeper_throws_accurate          INTEGER,
    blocked_passes                  INTEGER,
    key_passes                      INTEGER,
    assists                         INTEGER,
    big_chances_created             INTEGER,
    passes_entering_final_third     INTEGER,
    passes_in_final_third           INTEGER,
    passes_in_final_third_accurate  INTEGER,
    UNIQUE (combo_id, team_id)
);

-- ── Team-level advanced passing ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_passing_adv (
    id                                  BIGSERIAL PRIMARY KEY,
    whoscored_match_id                  INTEGER NOT NULL,
    combo_id                            TEXT NOT NULL,
    team_id                             INTEGER NOT NULL,
    progressive_passes                  INTEGER,
    passes_entering_final_third         INTEGER,
    passes_in_final_third               INTEGER,
    passes_in_final_third_accurate      INTEGER,
    passes_in_final_third_open_play     INTEGER,
    passes_in_final_third_op_accurate   INTEGER,
    penalty_area_passes                 INTEGER,
    switches_of_play                    INTEGER,
    avg_pass_length                     NUMERIC(6,2),
    avg_pass_length_euclid              NUMERIC(6,2),
    avg_vertical_gain                   NUMERIC(6,2),
    forward_pass_pct                    NUMERIC(5,2),
    backward_pass_pct                   NUMERIC(5,2),
    lateral_pass_pct                    NUMERIC(5,2),
    directness_index                    NUMERIC(8,6),
    xt_total                            NUMERIC(8,4),
    xt_positive                         NUMERIC(8,4),
    xt_per_pass                         NUMERIC(8,6),
    key_passes                          INTEGER,
    key_passes_open_play                INTEGER,
    assists                             INTEGER,
    assists_open_play                   INTEGER,
    chances_created                     INTEGER,
    chances_created_open_play           INTEGER,
    big_chances_created                 INTEGER,
    big_chances_created_open_play       INTEGER,
    crosses_open_play_attempted         INTEGER,
    crosses_open_play_accurate          INTEGER,
    corner_deliveries_open_play         INTEGER,
    ppda                                NUMERIC(6,2),
    field_tilt_pct                      NUMERIC(5,2),
    avg_release_seconds                 NUMERIC(5,2),
    quick_pass_pct                      NUMERIC(5,2),
    slow_pass_pct                       NUMERIC(5,2),
    UNIQUE (combo_id, team_id)
);

-- ── Team-level defending ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_defending (
    id                              BIGSERIAL PRIMARY KEY,
    whoscored_match_id              INTEGER NOT NULL,
    combo_id                        TEXT NOT NULL,
    team_id                         INTEGER NOT NULL,
    tackles_total                   INTEGER,
    tackles_won                     INTEGER,
    tackles_lost                    INTEGER,
    dribbled_past                   INTEGER,
    tackle_success_pct              NUMERIC(5,2),
    tackles_defensive               INTEGER,
    tackles_offensive               INTEGER,
    tackles_own_third               INTEGER,
    tackles_middle_third            INTEGER,
    tackles_final_third             INTEGER,
    interceptions                   INTEGER,
    interceptions_headed            INTEGER,
    interceptions_last_man          INTEGER,
    interceptions_own_third         INTEGER,
    interceptions_middle_third      INTEGER,
    interceptions_final_third       INTEGER,
    clearances                      INTEGER,
    clearances_headed               INTEGER,
    clearances_blocked_cross        INTEGER,
    clearances_in_own_box           INTEGER,
    avg_clearance_distance_m        NUMERIC(6,2),
    aerials_total                   INTEGER,
    aerials_won                     INTEGER,
    aerials_lost                    INTEGER,
    aerial_win_pct                  NUMERIC(5,2),
    aerials_defensive               INTEGER,
    aerials_offensive               INTEGER,
    aerial_win_pct_defensive        NUMERIC(5,2),
    aerial_win_pct_offensive        NUMERIC(5,2),
    aerials_own_third               INTEGER,
    aerials_middle_third            INTEGER,
    aerials_final_third             INTEGER,
    ball_recoveries                 INTEGER,
    recoveries_own_third            INTEGER,
    recoveries_middle_third         INTEGER,
    recoveries_final_third          INTEGER,
    blocked_passes                  INTEGER,
    blocked_shots                   INTEGER,
    fouls_conceded                  INTEGER,
    dispossessed                    INTEGER,
    shield_ball_opp                 INTEGER,
    offsides_caught                 INTEGER,
    errors_leading_to_shot          INTEGER,
    errors_leading_to_goal          INTEGER,
    errors_total                    INTEGER,
    defensive_actions_total         INTEGER,
    press_actions_own_third         INTEGER,
    press_actions_middle_third      INTEGER,
    press_actions_final_third       INTEGER,
    avg_defensive_line_height       NUMERIC(5,2),
    ground_duels                    INTEGER,
    ground_duels_won                INTEGER,
    ground_duels_lost               INTEGER,
    ground_duel_win_pct             NUMERIC(5,2),
    total_duels                     INTEGER,
    total_duels_won                 INTEGER,
    total_duels_lost                INTEGER,
    duel_win_pct                    NUMERIC(5,2),
    UNIQUE (combo_id, team_id)
);

-- ── Team-level discipline ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_discipline (
    id                          BIGSERIAL PRIMARY KEY,
    whoscored_match_id          INTEGER NOT NULL,
    combo_id                    TEXT NOT NULL,
    team_id                     INTEGER NOT NULL,
    fouls_committed             INTEGER,
    fouls_won                   INTEGER,
    fouls_defensive             INTEGER,
    fouls_offensive             INTEGER,
    fouls_aerial                INTEGER,
    fouls_own_box               INTEGER,
    fouls_own_half              INTEGER,
    fouls_midfield              INTEGER,
    fouls_danger_area           INTEGER,
    fouls_first_half            INTEGER,
    fouls_second_half           INTEGER,
    fouls_late_game             INTEGER,
    yellow_cards                INTEGER,
    red_cards                   INTEGER,
    second_yellow_cards         INTEGER,
    offsides_committed          INTEGER,
    offsides_caught             INTEGER,
    goals_disallowed_offside    INTEGER,
    dangerous_foul_rate_pct     NUMERIC(5,2),
    foul_escalation_rate        NUMERIC(5,2),
    foul_to_card_ratio          NUMERIC(5,2),
    UNIQUE (combo_id, team_id)
);

-- ── Team-level possession ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_possession (
    id                          BIGSERIAL PRIMARY KEY,
    whoscored_match_id          INTEGER NOT NULL,
    combo_id                    TEXT NOT NULL,
    team_id                     INTEGER NOT NULL,
    possession_pct              NUMERIC(5,2),
    possession_minutes          NUMERIC(5,1),
    touches_total               INTEGER,
    touches_own_third           INTEGER,
    touches_middle_third        INTEGER,
    touches_final_third         INTEGER,
    touches_in_box              INTEGER,
    avg_touch_x                 NUMERIC(5,2),
    dribbles_attempted          INTEGER,
    dribbles_won                INTEGER,
    dribbles_lost               INTEGER,
    dribble_success_pct         NUMERIC(5,2),
    dribbles_offensive          INTEGER,
    dribbles_defensive          INTEGER,
    dribbles_overrun            INTEGER,
    dispossessed                INTEGER,
    dispossessed_offensive      INTEGER,
    dispossessed_defensive      INTEGER,
    shield_ball_opp             INTEGER,
    passing_rate                NUMERIC(6,2),
    corners_won                 INTEGER,
    corners_miss_left           INTEGER,
    corners_miss_right          INTEGER,
    UNIQUE (combo_id, team_id)
);

-- ── Team-level advanced possession ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_team_poss_adv (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    passing_rate            NUMERIC,
    passes_accurate         INTEGER,
    possession_minutes      NUMERIC,
    field_tilt_pct          NUMERIC,
    field_tilt_passes_pct   NUMERIC,
    final_third_touches     INTEGER,
    ppda                    NUMERIC,
    sequences_total         INTEGER,
    open_play_sequences     INTEGER,
    avg_sequence_time_s     NUMERIC,
    avg_sequence_length     NUMERIC,
    passes_per_sequence     NUMERIC,
    start_distance_m        NUMERIC,
    direct_speed_m_s        NUMERIC,
    ten_plus_pass_sequences INTEGER,
    build_up_attacks        INTEGER,
    direct_attacks          INTEGER,
    high_turnovers          INTEGER,
    high_turnover_shots     INTEGER,
    shot_sequences          INTEGER,
    shot_seq_pct            NUMERIC,
    avg_shot_seq_length     NUMERIC,
    avg_shot_seq_duration_s NUMERIC,
    avg_shot_seq_passes     NUMERIC,
    attack_sequences        INTEGER,
    avg_attack_duration_s   NUMERIC,
    avg_attack_passes       NUMERIC,
    possessions_total       INTEGER,
    avg_possession_duration_s NUMERIC,
    avg_sequences_per_possession NUMERIC,
    counter_attack_sequences INTEGER,
    counter_attack_shot_pct NUMERIC,
    open_play_turnover_pct  NUMERIC,
    possession_loss_pct     NUMERIC,
    set_piece_end_pct       NUMERIC,
    UNIQUE (combo_id, team_id)
);

-- ── Player-level passing ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_player_passing (
    id                              BIGSERIAL PRIMARY KEY,
    whoscored_match_id              INTEGER NOT NULL,
    combo_id                        TEXT NOT NULL,
    team_id                         INTEGER,
    player_id                       INTEGER NOT NULL,
    player_name                     TEXT,
    passes_total                    INTEGER,
    passes_accurate                 INTEGER,
    passes_inaccurate               INTEGER,
    pass_accuracy_pct               NUMERIC(5,2),
    longballs_attempted             INTEGER,
    longballs_accurate              INTEGER,
    longball_accuracy_pct           NUMERIC(5,2),
    throughballs                    INTEGER,
    head_passes                     INTEGER,
    layoffs                         INTEGER,
    crosses_attempted               INTEGER,
    crosses_accurate                INTEGER,
    key_passes                      INTEGER,
    assists                         INTEGER,
    big_chances_created             INTEGER,
    passes_entering_final_third     INTEGER,
    passes_in_final_third           INTEGER,
    passes_in_final_third_accurate  INTEGER,
    UNIQUE (combo_id, player_id)
);

-- ── Player-level advanced passing ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_player_passing_adv (
    id                          BIGSERIAL PRIMARY KEY,
    whoscored_match_id          INTEGER NOT NULL,
    combo_id                    TEXT NOT NULL,
    team_id                     INTEGER,
    player_id                   INTEGER NOT NULL,
    player_name                 TEXT,
    progressive_passes          INTEGER,
    passes_entering_final_third INTEGER,
    penalty_area_passes         INTEGER,
    avg_pass_length             NUMERIC(6,2),
    avg_vertical_gain           NUMERIC(6,2),
    forward_pass_pct            NUMERIC(5,2),
    backward_pass_pct           NUMERIC(5,2),
    xt_total                    NUMERIC(8,4),
    xt_positive                 NUMERIC(8,4),
    xt_per_pass                 NUMERIC(8,6),
    key_passes                  INTEGER,
    assists                     INTEGER,
    chances_created             INTEGER,
    avg_hold_seconds            NUMERIC(5,2),
    UNIQUE (combo_id, player_id)
);

-- ── Player-level defending ───────────────────────────────────────────────────
-- Same 60 columns as team_defending but with player_id/player_name added.
-- Uses same column names — column introspection handles the match.

CREATE TABLE IF NOT EXISTS ws_player_defending (
    id                              BIGSERIAL PRIMARY KEY,
    whoscored_match_id              INTEGER NOT NULL,
    combo_id                        TEXT NOT NULL,
    team_id                         INTEGER,
    player_id                       INTEGER NOT NULL,
    player_name                     TEXT,
    tackles_total                   INTEGER,
    tackles_won                     INTEGER,
    tackles_lost                    INTEGER,
    dribbled_past                   INTEGER,
    tackle_success_pct              NUMERIC(5,2),
    tackles_defensive               INTEGER,
    tackles_offensive               INTEGER,
    tackles_own_third               INTEGER,
    tackles_middle_third            INTEGER,
    tackles_final_third             INTEGER,
    interceptions                   INTEGER,
    interceptions_headed            INTEGER,
    interceptions_last_man          INTEGER,
    interceptions_own_third         INTEGER,
    interceptions_middle_third      INTEGER,
    interceptions_final_third       INTEGER,
    clearances                      INTEGER,
    clearances_headed               INTEGER,
    clearances_blocked_cross        INTEGER,
    clearances_in_own_box           INTEGER,
    avg_clearance_distance_m        NUMERIC(6,2),
    aerials_total                   INTEGER,
    aerials_won                     INTEGER,
    aerials_lost                    INTEGER,
    aerial_win_pct                  NUMERIC(5,2),
    aerials_defensive               INTEGER,
    aerials_offensive               INTEGER,
    aerial_win_pct_defensive        NUMERIC(5,2),
    aerial_win_pct_offensive        NUMERIC(5,2),
    aerials_own_third               INTEGER,
    aerials_middle_third            INTEGER,
    aerials_final_third             INTEGER,
    ball_recoveries                 INTEGER,
    recoveries_own_third            INTEGER,
    recoveries_middle_third         INTEGER,
    recoveries_final_third          INTEGER,
    blocked_passes                  INTEGER,
    blocked_shots                   INTEGER,
    fouls_conceded                  INTEGER,
    dispossessed                    INTEGER,
    shield_ball_opp                 INTEGER,
    offsides_caught                 INTEGER,
    errors_leading_to_shot          INTEGER,
    errors_leading_to_goal          INTEGER,
    errors_total                    INTEGER,
    defensive_actions_total         INTEGER,
    press_actions_own_third         INTEGER,
    press_actions_middle_third      INTEGER,
    press_actions_final_third       INTEGER,
    avg_defensive_line_height       NUMERIC(5,2),
    ground_duels                    INTEGER,
    ground_duels_won                INTEGER,
    ground_duels_lost               INTEGER,
    ground_duel_win_pct             NUMERIC(5,2),
    total_duels                     INTEGER,
    total_duels_won                 INTEGER,
    total_duels_lost                INTEGER,
    duel_win_pct                    NUMERIC(5,2),
    UNIQUE (combo_id, player_id)
);

-- ── Player-level goalkeeping ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_player_goalkeep (
    id                          BIGSERIAL PRIMARY KEY,
    whoscored_match_id          INTEGER NOT NULL,
    combo_id                    TEXT NOT NULL,
    team_id                     INTEGER,
    player_id                   INTEGER NOT NULL,
    player_name                 TEXT,
    saves                       INTEGER,
    goals_conceded              INTEGER,
    shots_on_target_faced       INTEGER,
    save_pct                    NUMERIC(5,2),
    saves_standing              INTEGER,
    saves_diving                INTEGER,
    saves_in_box                INTEGER,
    saves_out_of_box            INTEGER,
    saves_collected             INTEGER,
    saves_parried_safe          INTEGER,
    saves_parried_danger        INTEGER,
    saves_with_head             INTEGER,
    saves_other_body_part       INTEGER,
    keeper_pickups              INTEGER,
    pickups_routine             INTEGER,
    pickups_after_save          INTEGER,
    high_claims                 INTEGER,
    high_claims_won             INTEGER,
    high_claim_win_pct          NUMERIC(5,2),
    sweeper_actions             INTEGER,
    avg_sweeper_x               NUMERIC(5,2),
    punches                     INTEGER,
    punches_accurate            INTEGER,
    avg_punch_distance_m        NUMERIC(6,2),
    distributions_total         INTEGER,
    passes_total                INTEGER,
    passes_accurate             INTEGER,
    distribution_accuracy_pct   NUMERIC(5,2),
    goal_kicks                  INTEGER,
    keeper_throws               INTEGER,
    longballs                   INTEGER,
    longballs_accurate          INTEGER,
    longball_accuracy_pct       NUMERIC(5,2),
    short_passes                INTEGER,
    freekick_passes             INTEGER,
    avg_distribution_length_m   NUMERIC(6,2),
    long_kick_pct               NUMERIC(5,2),
    short_kick_pct              NUMERIC(5,2),
    touches_outside_box         INTEGER,
    passes_outside_box          INTEGER,
    clearances                  INTEGER,
    progressive_passes          INTEGER,
    UNIQUE (combo_id, player_id)
);

-- ── Player-level discipline ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_player_discipline (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    team_id                 INTEGER,
    player_id               INTEGER NOT NULL,
    player_name             TEXT,
    fouls_committed         INTEGER,
    fouls_won               INTEGER,
    fouls_defensive         INTEGER,
    fouls_offensive         INTEGER,
    fouls_aerial            INTEGER,
    fouls_own_box           INTEGER,
    fouls_own_half          INTEGER,
    fouls_midfield          INTEGER,
    fouls_danger_area       INTEGER,
    fouls_first_half        INTEGER,
    fouls_second_half       INTEGER,
    fouls_late_game         INTEGER,
    yellow_cards            INTEGER,
    red_cards               INTEGER,
    second_yellow_cards     INTEGER,
    offsides_committed      INTEGER,
    UNIQUE (combo_id, player_id)
);

-- ── Player-level possession ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_player_possession (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    team_id                 INTEGER,
    player_id               INTEGER NOT NULL,
    player_name             TEXT,
    touches                 INTEGER,
    touches_own_third       INTEGER,
    touches_middle_third    INTEGER,
    touches_final_third     INTEGER,
    touches_in_box          INTEGER,
    avg_touch_x             NUMERIC(5,2),
    dribbles_attempted      INTEGER,
    dribbles_won            INTEGER,
    dribbles_lost           INTEGER,
    dribble_success_pct     NUMERIC(5,2),
    dispossessed            INTEGER,
    UNIQUE (combo_id, player_id)
);

-- ── Possession sequences ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_sequences (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    possession_id           INTEGER,
    length                  INTEGER,
    duration_s              INTEGER,
    start_x                 NUMERIC,
    start_y                 NUMERIC,
    end_x                   NUMERIC,
    end_y                   NUMERIC,
    n_passes                INTEGER,
    is_open_play            BOOLEAN,
    is_shot                 BOOLEAN,
    is_ten_plus_pass        BOOLEAN,
    is_build_up_attack      BOOLEAN,
    is_direct_attack        BOOLEAN,
    is_high_turnover        BOOLEAN,
    outcome                 TEXT,
    event_id_start          INTEGER,
    event_id_end            INTEGER,
    player_id_start         INTEGER,
    player_id_end           INTEGER,
    player_ids              JSONB
);

-- ══════════════════════════════════════════════════════════════════════════
-- ║  WhoScored — SHOOTING (parse_shooting)                                   ║
-- ══════════════════════════════════════════════════════════════════════════
-- ws_team_shooting / ws_player_shooting: per-match aggregates incl. SCA.
-- ws_shots: one row per shot (event-level; delete+insert per combo_id).

CREATE TABLE IF NOT EXISTS ws_team_shooting (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    player_id               INTEGER,
    player_name             TEXT,
    total_shots             INTEGER,
    goals                   INTEGER,
    shots_on_target         INTEGER,
    shots_blocked           INTEGER,
    shots_off_target        INTEGER,
    shots_on_target_pct     NUMERIC,
    conversion_rate_pct     NUMERIC,
    goals_per_shot_on_target_pct NUMERIC,
    big_chances             INTEGER,
    big_chances_scored      INTEGER,
    big_chance_conversion_pct NUMERIC,
    one_on_ones             INTEGER,
    shots_in_box            INTEGER,
    shots_out_of_box        INTEGER,
    shots_six_yard_box      INTEGER,
    in_box_conversion_pct   NUMERIC,
    avg_shot_distance_m     NUMERIC,
    shots_right_foot        INTEGER,
    shots_left_foot         INTEGER,
    shots_header            INTEGER,
    shots_volley            INTEGER,
    shots_first_touch       INTEGER,
    shots_from_corner       INTEGER,
    shots_from_freekick     INTEGER,
    shots_fast_break        INTEGER,
    shots_assisted          INTEGER,
    shots_individual_play   INTEGER,
    shots_open_play         INTEGER,
    goals_open_play         INTEGER,
    shots_on_target_open_play INTEGER,
    placement_top_corner    INTEGER,
    placement_low_corner    INTEGER,
    placement_centre_frame  INTEGER,
    sca_total               INTEGER,
    sca_pass_live           INTEGER,
    sca_pass_dead           INTEGER,
    sca_take_on             INTEGER,
    sca_shot                INTEGER,
    sca_fouled              INTEGER,
    sca_defense             INTEGER,
    shots_with_sca          INTEGER,
    UNIQUE (combo_id, team_id)
);

CREATE TABLE IF NOT EXISTS ws_player_shooting (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    player_id               INTEGER,
    player_name             TEXT,
    total_shots             INTEGER,
    goals                   INTEGER,
    shots_on_target         INTEGER,
    shots_blocked           INTEGER,
    shots_off_target        INTEGER,
    shots_on_target_pct     NUMERIC,
    conversion_rate_pct     NUMERIC,
    goals_per_shot_on_target_pct NUMERIC,
    big_chances             INTEGER,
    big_chances_scored      INTEGER,
    big_chance_conversion_pct NUMERIC,
    one_on_ones             INTEGER,
    shots_in_box            INTEGER,
    shots_out_of_box        INTEGER,
    shots_six_yard_box      INTEGER,
    in_box_conversion_pct   NUMERIC,
    avg_shot_distance_m     NUMERIC,
    shots_right_foot        INTEGER,
    shots_left_foot         INTEGER,
    shots_header            INTEGER,
    shots_volley            INTEGER,
    shots_first_touch       INTEGER,
    shots_from_corner       INTEGER,
    shots_from_freekick     INTEGER,
    shots_fast_break        INTEGER,
    shots_assisted          INTEGER,
    shots_individual_play   INTEGER,
    shots_open_play         INTEGER,
    goals_open_play         INTEGER,
    shots_on_target_open_play INTEGER,
    placement_top_corner    INTEGER,
    placement_low_corner    INTEGER,
    placement_centre_frame  INTEGER,
    sca                     INTEGER,
    sca_pass_live           INTEGER,
    sca_pass_dead           INTEGER,
    sca_take_on             INTEGER,
    sca_shot                INTEGER,
    sca_fouled              INTEGER,
    sca_defense             INTEGER,
    shots                   INTEGER,
    UNIQUE (combo_id, player_id)
);

CREATE TABLE IF NOT EXISTS ws_shots (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    event_id                INTEGER,
    team_id                 INTEGER,
    player_id               INTEGER,
    player_name             TEXT,
    minute                  INTEGER,
    second                  INTEGER,
    expanded_minute         INTEGER,
    period                  TEXT,
    x                       NUMERIC,
    y                       NUMERIC,
    is_goal                 BOOLEAN,
    is_on_target            BOOLEAN,
    is_blocked              BOOLEAN,
    is_off_target           BOOLEAN,
    is_open_play            BOOLEAN,
    location                TEXT,
    in_box                  BOOLEAN,
    six_yard_box            BOOLEAN,
    small_box               BOOLEAN,
    distance_m              NUMERIC,
    angle_deg               NUMERIC,
    body_part               TEXT,
    is_header               BOOLEAN,
    is_right_foot           BOOLEAN,
    is_left_foot            BOOLEAN,
    is_volley               BOOLEAN,
    is_first_touch          BOOLEAN,
    pattern                 TEXT,
    is_from_corner          BOOLEAN,
    is_from_freekick        BOOLEAN,
    is_fast_break           BOOLEAN,
    is_assisted             BOOLEAN,
    is_individual_play      BOOLEAN,
    is_big_chance           BOOLEAN,
    is_one_on_one           BOOLEAN,
    goal_mouth_y            NUMERIC,
    goal_mouth_z            NUMERIC,
    placement               TEXT,
    related_event_id        INTEGER,
    xgot                    NUMERIC,
    sca_1_player_id         INTEGER,
    sca_1_player_name       TEXT,
    sca_1_type              TEXT,
    sca_2_player_id         INTEGER,
    sca_2_player_name       TEXT,
    sca_2_type              TEXT
);

-- ══════════════════════════════════════════════════════════════════════════
-- ║  WhoScored — GAME STATE (parse_game_state)                               ║
-- ══════════════════════════════════════════════════════════════════════════
-- ws_game_states: match-level interval spine (one row per state).
-- ws_team_states: spine x2, one row per interval per team (perspective).
-- ws_context_events: goals/cards/subs/formation changes, tagged with state.
-- ws_team_state_agg: per (team, period, score_bucket, man_state) aggregates.
-- All four regenerate per match (delete+insert per combo_id).

CREATE TABLE IF NOT EXISTS ws_game_states (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    state_id                INTEGER,
    period                  INTEGER,
    start_s                 INTEGER,
    end_s                   INTEGER,
    duration_s              INTEGER,
    home_score              INTEGER,
    away_score              INTEGER,
    home_players            INTEGER,
    away_players            INTEGER,
    trigger                 TEXT,
    home_score_diff         INTEGER,
    man_diff_home           INTEGER,
    start_minute            INTEGER,
    start_second            INTEGER,
    start_display           TEXT,
    end_minute              INTEGER,
    end_second              INTEGER,
    end_display             TEXT
);

CREATE TABLE IF NOT EXISTS ws_team_states (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    state_id                INTEGER,
    team_id                 INTEGER,
    opponent_id             INTEGER,
    is_home                 BOOLEAN,
    period                  INTEGER,
    start_s                 INTEGER,
    end_s                   INTEGER,
    duration_s              INTEGER,
    start_minute            INTEGER,
    end_minute              INTEGER,
    start_display           TEXT,
    end_display             TEXT,
    score_for               INTEGER,
    score_against           INTEGER,
    score_diff              INTEGER,
    score_bucket            TEXT,
    man_diff                INTEGER,
    man_state               TEXT,
    trigger                 TEXT
);

CREATE TABLE IF NOT EXISTS ws_context_events (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    state_id                INTEGER,
    score_diff_at           INTEGER,
    man_state_at            TEXT,
    period                  INTEGER,
    minute                  INTEGER,
    second                  INTEGER,
    minute_display          TEXT,
    time_s                  INTEGER,
    event_type              TEXT,
    player_id               INTEGER,
    player_name             TEXT,
    player_in_id            INTEGER,
    player_in_name          TEXT,
    player_out_id           INTEGER,
    player_out_name         TEXT,
    detail                  TEXT
);

CREATE TABLE IF NOT EXISTS ws_team_state_agg (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    team_id                 INTEGER,
    period                  INTEGER,
    score_bucket            TEXT,
    man_state               TEXT,
    state_ids               JSONB,
    minutes                 NUMERIC,
    possession_pct          NUMERIC,
    touches                 INTEGER,
    final_third_touches     INTEGER,
    field_tilt_pct          NUMERIC,
    box_touches             INTEGER,
    shots                   INTEGER,
    shots_on_target         INTEGER,
    goals                   INTEGER,
    key_passes              INTEGER,
    key_passes_open_play    INTEGER,
    assists                 INTEGER,
    big_chances_created     INTEGER,
    big_chances_created_open_play INTEGER,
    big_chances             INTEGER,
    big_chances_scored      INTEGER,
    xt_total                NUMERIC,
    xt_positive             NUMERIC,
    passes                  INTEGER,
    passes_completed        INTEGER,
    pass_accuracy_pct       NUMERIC,
    crosses                 INTEGER,
    corners                 INTEGER,
    fouls_committed         INTEGER,
    fouls_won               INTEGER,
    offsides                INTEGER,
    yellow_cards            INTEGER,
    red_cards               INTEGER,
    subs_made               INTEGER,
    take_ons                INTEGER,
    take_ons_won            INTEGER,
    tackles                 INTEGER,
    interceptions           INTEGER,
    clearances              INTEGER,
    aerials_won             INTEGER,
    aerials_total           INTEGER,
    defensive_actions       INTEGER,
    ppda                    NUMERIC
);


-- ── Pass map (individual pass events) ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_pass_map (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    event_id                INTEGER,
    player_id               INTEGER,
    player_name             TEXT,
    team_id                 INTEGER,
    minute                  INTEGER,
    second                  INTEGER,
    expanded_minute         INTEGER,
    period                  TEXT,
    x                       NUMERIC(5,1),
    y                       NUMERIC(5,1),
    end_x                   NUMERIC(5,1),
    end_y                   NUMERIC(5,1),
    length                  NUMERIC(6,1),
    accurate                BOOLEAN,
    v_gain                  NUMERIC(6,2),
    dist_to_goal_start      NUMERIC(6,2),
    dist_to_goal_end        NUMERIC(6,2),
    is_progressive          BOOLEAN,
    is_final_third          BOOLEAN,
    is_penalty_area         BOOLEAN,
    is_switch               BOOLEAN,
    is_open_play            BOOLEAN,
    xt_start                NUMERIC(8,5),
    xt_end                  NUMERIC(8,5),
    xt_added                NUMERIC(8,5),
    receiver_id             INTEGER,
    receiver_name           TEXT
);

-- ── Pass network (aggregated passer→receiver edges) ──────────────────────────

CREATE TABLE IF NOT EXISTS ws_pass_network (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    team_id                 INTEGER,
    passer_id               INTEGER,
    passer_name             TEXT,
    receiver_id             INTEGER,
    receiver_name           TEXT,
    pass_count              INTEGER,
    UNIQUE (combo_id, passer_id, receiver_id)
);

-- ── Average positions ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ws_avg_positions (
    id                      BIGSERIAL PRIMARY KEY,
    whoscored_match_id      INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    team_id                 INTEGER,
    player_id               INTEGER NOT NULL,
    player_name             TEXT,
    avg_x                   NUMERIC(5,2),
    avg_y                   NUMERIC(5,2),
    touch_count             INTEGER,
    UNIQUE (combo_id, player_id)
);


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  FOTMOB (fot_)                                                           ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ── Match metadata ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fot_match (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    match_name              TEXT,
    match_round             TEXT,
    league_id               INTEGER,
    league_name             TEXT,
    league_round_name       TEXT,
    country_code            TEXT,
    home_team_id            INTEGER,
    home_team_name          TEXT,
    home_score              INTEGER,
    away_team_id            INTEGER,
    away_team_name          TEXT,
    away_score              INTEGER,
    match_date              TEXT,
    stadium_name            TEXT,
    stadium_city            TEXT,
    stadium_country         TEXT,
    stadium_capacity        INTEGER,
    attendance              INTEGER,
    referee_name            TEXT,
    referee_country         TEXT,
    motm_id                 INTEGER,
    motm_name               TEXT,
    motm_rating             TEXT,
    ht_coach_id             INTEGER,
    ht_coach_name           TEXT,
    at_coach_id             INTEGER,
    at_coach_name           TEXT,
    home_color              TEXT,
    away_color              TEXT,
    home_color_dark         TEXT,
    away_color_dark         TEXT,
    home_font_color         TEXT,
    away_font_color         TEXT,
    home_font_dark          TEXT,
    away_font_dark          TEXT,
    has_shot_data           BOOLEAN DEFAULT FALSE,
    has_momentum_data       BOOLEAN DEFAULT FALSE,
    UNIQUE (combo_id)
);

-- ── Lineups ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fot_lineups (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    player_id               INTEGER,
    player_name             TEXT,
    team_id                 INTEGER,
    team_name               TEXT,
    is_home_team            BOOLEAN,
    role                    TEXT,        -- 'starter', 'sub', 'unavailable'
    shirt_number            INTEGER,
    position_id             INTEGER,
    position                TEXT,        -- RCM, CAM, LWB, ST, etc.
    usual_position_id       INTEGER,
    usual_position          TEXT,        -- GK, DEF, MID, FWD
    is_captain              BOOLEAN,
    age                     INTEGER,
    country                 TEXT,
    country_code            TEXT,
    rating                  NUMERIC(3,1),
    formation               TEXT,
    UNIQUE (combo_id, player_id)
);

-- ── Coaches (folded into fot_match as ht_/at_coach_* — table removed) ────────
-- Coach data now lives on fot_match (ht_coach_id, ht_coach_name, at_coach_id,
-- at_coach_name) to avoid one redundant row per coach per fixture.

-- ── Player stats (wide table — one row per player) ──────────────────────────
-- Columns are dynamically generated from Fotmob stat groups (attack, defense,
-- duels, top stats). Column introspection handles any new stats that appear.

CREATE TABLE IF NOT EXISTS fot_player_stats (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    player_id               INTEGER,
    player_name             TEXT,
    team_id                 INTEGER,
    team_name               TEXT,
    is_goalkeeper            BOOLEAN,
    shirt_number            INTEGER,
    position_id             INTEGER,
    -- Dynamic stat columns added via column introspection at load time.
    -- Examples: goals, assists, rating, minutes_played, shots_on_target,
    -- accurate_passes, tackles_won, saves, expected_goals, etc.
    UNIQUE (combo_id, player_id)
);

-- ── Team stats (one row per stat per period) ─────────────────────────────────

-- Wide format: one row per (combo_id, period); two columns per stat
-- ({stat_key}_home / {stat_key}_away), auto-added at load time. Redundant
-- per-stat metadata (group, stat_title, stat_format, highlighted) is dropped.
CREATE TABLE IF NOT EXISTS fot_team_stats (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    period                  TEXT         -- 'All', '1st', '2nd'
);

-- ── Shots (shotmap with xG/xGOT) ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fot_shots (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    shot_id                 BIGINT,
    event_type              TEXT,
    team_id                 INTEGER,
    player_id               INTEGER,
    player_name             TEXT,
    x                       NUMERIC(6,3),
    y                       NUMERIC(6,3),
    minute                  INTEGER,
    minute_added            INTEGER,
    period                  TEXT,
    is_blocked              BOOLEAN,
    is_on_target            BOOLEAN,
    is_own_goal             BOOLEAN,
    is_from_inside_box      BOOLEAN,
    is_saved_off_line       BOOLEAN,
    blocked_x               NUMERIC(6,3),
    blocked_y               NUMERIC(6,3),
    goal_crossed_y          NUMERIC(6,3),
    goal_crossed_z          NUMERIC(6,3),
    xg                      NUMERIC(5,4),
    xgot                    NUMERIC(5,4),
    shot_type               TEXT,
    situation               TEXT,
    keeper_id               INTEGER,
    on_goal_shot            TEXT
);

-- ── Spatial (momentum as JSONB blob) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fot_spatial (
    id                      BIGSERIAL PRIMARY KEY,
    fotmob_id               INTEGER NOT NULL,
    combo_id                TEXT NOT NULL,
    momentum_data           JSONB,
    UNIQUE (combo_id)
);


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  SOFASCORE (sofa_)                                                       ║
-- ║  Tables match read_json_data.py output. Column introspection at load     ║
-- ║  time handles any extra/missing columns across seasons.                   ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- Column names below mirror read_json_data.py output exactly. Wide tables
-- (sofa_player_stats and any season-varying columns on sofa_matches/sofa_shots)
-- get their extra columns auto-added as TEXT by the loader's upsert() at load
-- time. Note: read_json_data emits its natural primary key as `id`; the loader
-- remaps that to the natural key column (match_id / season_id / referee_id /
-- manager_id / shot_id) so it never collides with the BIGSERIAL surrogate.

CREATE TABLE IF NOT EXISTS sofa_tournaments (
    id                      BIGSERIAL PRIMARY KEY,
    tournament_id           INTEGER,
    name                    TEXT,
    country                 TEXT,
    tier                    TEXT,
    ingested_at             TEXT,
    UNIQUE (tournament_id)
);

CREATE TABLE IF NOT EXISTS sofa_seasons (
    id                      BIGSERIAL PRIMARY KEY,
    season_id               INTEGER,
    name                    TEXT,
    year                    TEXT,
    tournament_id           INTEGER,
    ingested_at             TEXT,
    -- editor / seasoncoverageinfo auto-added as TEXT by loader
    UNIQUE (season_id)
);

CREATE TABLE IF NOT EXISTS sofa_teams (
    id                      BIGSERIAL PRIMARY KEY,
    team_id                 INTEGER,
    name                    TEXT,
    stadium                 TEXT,
    stadium_capacity        INTEGER,
    country                 TEXT,
    city                    TEXT,
    latitude                TEXT,
    longitude               TEXT,
    founded_date            TEXT,
    ingested_at             TEXT,
    UNIQUE (team_id)
);

CREATE TABLE IF NOT EXISTS sofa_players (
    id                      BIGSERIAL PRIMARY KEY,
    player_id               INTEGER,
    name                    TEXT,
    position                TEXT,
    nationality             TEXT,
    birth_date              TEXT,
    ingested_at             TEXT,
    UNIQUE (player_id)
);

CREATE TABLE IF NOT EXISTS sofa_referees (
    id                      BIGSERIAL PRIMARY KEY,
    referee_id              INTEGER,
    name                    TEXT,
    nationality             TEXT,
    red_cards               INTEGER,
    yellow_cards            INTEGER,
    double_yellow_cards     INTEGER,
    games                   INTEGER,
    ingested_at             TEXT,
    UNIQUE (referee_id)
);

CREATE TABLE IF NOT EXISTS sofa_managers (
    id                      BIGSERIAL PRIMARY KEY,
    manager_id              INTEGER,
    name                    TEXT,
    slug                    TEXT,
    "shortname"             TEXT,
    ingested_at             TEXT,
    UNIQUE (manager_id)
);

CREATE TABLE IF NOT EXISTS sofa_matches (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    tournament_id           INTEGER,
    season_id               INTEGER,
    venue                   TEXT,
    referee_id              INTEGER,
    home_team_id            INTEGER,
    away_team_id            INTEGER,
    home_manager_id         INTEGER,
    away_manager_id         INTEGER,
    home_formation          TEXT,
    away_formation          TEXT,
    attendance              INTEGER,
    "winnercode"            INTEGER,
    "starttimestamp"        TEXT,
    motm_rating             TEXT,
    motm_player_name        TEXT,
    motm_player_id          INTEGER,
    has_shotmap             BOOLEAN DEFAULT FALSE,
    has_xg                  BOOLEAN DEFAULT FALSE,
    ingested_at             TEXT,
    -- Wide score/time columns (homescore_current, awayscore_period1,
    -- time_injurytime1, roundinfo_round, …) auto-added as TEXT by loader.
    UNIQUE (combo_id)
);

CREATE TABLE IF NOT EXISTS sofa_lineups (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    team_id                 INTEGER,
    is_home_team            BOOLEAN,
    player_id               INTEGER,
    is_starter              BOOLEAN,
    is_captain              BOOLEAN,
    minutes_played          INTEGER,
    shirt_number            INTEGER,
    position                TEXT,
    ingested_at             TEXT
);

CREATE TABLE IF NOT EXISTS sofa_missing_players (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    team_id                 INTEGER,
    is_home_team            BOOLEAN,
    player_id               INTEGER,
    name                    TEXT,
    type                    TEXT,
    reason                  TEXT,
    description             TEXT,
    external_type           TEXT,
    expected_end_date       TEXT,
    ingested_at             TEXT
);

CREATE TABLE IF NOT EXISTS sofa_player_stats (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    team_id                 INTEGER,
    is_home_team            BOOLEAN,
    player_id               INTEGER,
    position                TEXT
    -- 40+ per-match stat columns (match_rating, totalpass, accuratepass,
    -- expectedgoals, totaltackle, saves, …) auto-added as TEXT by loader.
);

-- Wide format: one row per period (FULL-TIME / FIRST-HALF / SECOND-HALF),
-- one column per stat ({stat}_homevalue / {stat}_awayvalue, plus _hometotal /
-- _awaytotal where Sofascore reports an "x of y"). Stat columns auto-added.
CREATE TABLE IF NOT EXISTS sofa_match_stats (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    period                  TEXT,
    ingested_at             TEXT
);

CREATE TABLE IF NOT EXISTS sofa_shots (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    shot_id                 BIGINT,
    player_id               INTEGER,
    player_name             TEXT,
    "ishome"                BOOLEAN,
    "shottype"              TEXT,
    situation               TEXT,
    "bodypart"              TEXT,
    "goalmouthlocation"     TEXT,
    xg                      NUMERIC(5,4),
    xgot                    NUMERIC(5,4),
    "incidenttype"          TEXT,
    "goaltype"              TEXT,
    "addedtime"             INTEGER,
    ingested_at             TEXT
    -- Coordinate blobs (playercoordinates, goalmouthcoordinates,
    -- blockcoordinates) and timing columns auto-added as TEXT by loader.
);

-- Long format: one row per odds selection (full selection label kept verbatim).
-- oddsp_odds remains the decomposed primary odds source.
CREATE TABLE IF NOT EXISTS sofa_odds (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    selection               TEXT,
    odds_value              TEXT,
    ingested_at             TEXT
);

CREATE TABLE IF NOT EXISTS sofa_spatial (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    match_id                INTEGER,
    average_positions       JSONB,
    commentary              JSONB,
    match_momentum_graph    JSONB,
    home_heatmap            JSONB,
    away_heatmap            JSONB,
    full_player_heatmaps    JSONB,
    ingested_at             TEXT
);


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  ODDSPEDIA (oddsp_)                                                      ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS oddsp_odds (
    id                      BIGSERIAL PRIMARY KEY,
    oddspedia_match_id      TEXT NOT NULL,
    combo_id                TEXT NOT NULL,
    market_name             TEXT,
    market_group_id         INTEGER,
    market_slug             TEXT,
    period_name             TEXT,
    line                    TEXT,
    outcome_name            TEXT,
    odds_value              TEXT,
    odds_direction          INTEGER
);