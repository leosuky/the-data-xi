-- Data XI xG / xGOT: the raw table for parse_shot_xg. Safe to re-run. Run BEFORE
-- deploying the parser (the loader cannot insert into a table that doesn't exist).
-- docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi < include/scripts/migrate_shot_xg.sql
BEGIN;
CREATE TABLE IF NOT EXISTS raw.ws_shot_xg (
    id                      BIGSERIAL PRIMARY KEY,
    combo_id                TEXT NOT NULL,
    whoscored_match_id      INTEGER,
    event_id                INTEGER,
    team_id                 INTEGER,
    player_id               INTEGER,
    period                  INTEGER,
    minute                  INTEGER,
    second                  INTEGER,
    is_goal                 BOOLEAN,
    is_penalty              BOOLEAN,
    is_own_goal             BOOLEAN,
    is_on_target            BOOLEAN,
    keeper_id               INTEGER,
    dxi_xg                  NUMERIC(5,4),
    dxi_xg_l1               NUMERIC(5,4),
    dxi_xgot                NUMERIC(5,4),
    dxi_xgot_l1             NUMERIC(5,4),
    model_version           TEXT,
    UNIQUE (combo_id, team_id, event_id)
);
COMMIT;
