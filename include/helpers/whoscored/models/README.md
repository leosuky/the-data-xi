# Data XI xG and xGOT

*Added Nov 2026. Research: `xg_model_2026-11-12.md` and `xgot_model_2026-11-13.md`
(+ `_implementation.md`), server `~/work/reports/`; working files and refit
scripts in `~/work/xg_research/` and `~/work/xgot_research/`.*

## What it is

| | Definition | Penalties | Own goals |
|---|---|---|---|
| `dxi_xg` | Chance of scoring from where and how the shot was taken, and how the chance was made (the shot event and its linked assist only) | 0.79 | NULL |
| `dxi_xgot` | Chance a shot **on target** goes in, from where it was heading in the goal; 0 for blocked, post, miss | placement model | NULL |
| `*_l1` | The same from the logistic fallbacks (numpy only) | | |

On target = `Goal`, or `SavedShot` without `Blocked`. Keeper = the defending
keeper at the moment of the shot (start, substitutions, red cards).

**How good** (held-out 2024/25–2026/27, top five):

- xG is a little less sharp than the providers' shot by shot (log-loss +.011),
  as good at predicting a team's future goals, level with goals in every season.
- xGOT is a little behind the providers' current model (+.005), far better than
  the provider xGOT stored for seasons before 2024/25 (−.038).
- The boosted models are the main values; the logistic xGOT is clearly worse
  (+.018), so `lightgbm` is a pipeline dependency.

## Where it lives

| Layer | Object | Columns |
|---|---|---|
| raw | `ws_shot_xg` | one row per shot; key `(combo_id, team_id, event_id)` |
| staging | `stg_ws_shot_xg` | + `team_uid`, `player_uid`, `keeper_uid` |
| marts | `fct_shots` | `dxi_xg`, `dxi_xg_l1`, `dxi_xgot`, `dxi_xgot_l1`, `dxi_is_on_target`, `dxi_keeper_uid`, `dxi_model_version` (WhoScored shots) |
| | `fct_player_shooting` | `dxi_xg`, `dxi_npxg`, `dxi_xgot`, `dxi_npxgot` |
| | `fct_team_shooting` | the same, for and `_against` |
| | `fct_player_goalkeeping` | `dxi_sot_faced`, `dxi_xgot_faced`, `dxi_goals_conceded`, `dxi_xgot_var`, `dxi_goals_prevented`, penalties separately |
| | `agg_player_season` | `dxi_np_shots`, `dxi_npxg`, `dxi_npxgot`, `placement_added` (+ `_se`) |
| | `agg_keeper_season` | `goals_prevented` (+ `_se`, `_shrunk`), centred per competition-season |

**Join on `(combo_id, team, event_id)`** — WhoScored numbers events per team;
`(combo_id, event_id)` is shared by ~1% of shots.

## Reading keepers and finishers

- One season of goals prevented is ~80% chance (next-season correlation +.13).
  Show the ± (about ±4 goals a season) and prefer multi-season, shrunk numbers.
- Placement added (xGOT − xG) is ~88% chance per season: a description of how a
  player placed his shots, not a skill rating.
- Both are centred on the competition-season: our xGOT runs ~7% low outside the
  top five and any xGOT's level moves a few points between seasons.
- Penalty saving: no measurable skill.

## Rules the parser keeps

Each cost a wrong result in the research (full list in the parser docstring):
nothing from the shot's timestamp, nothing from neighbouring events, no
big-chance / fast-break / first-touch / volley / one-on-one flags, no pass end
point to shot distance, and never a blocked shot's goal-mouth position.

## Deploy and backfill

```bash
cd ~/the-data-xi && git pull
# 1. the raw table (before the parser runs)
docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi < include/scripts/migrate_shot_xg.sql
# 2. rebuild the image: requirements.txt (lightgbm) and packages.txt (libgomp1) changed
astro dev restart
# 3. parser check against the research (in the scheduler; needs the raw files)
S=$(docker ps --format '{{.Names}}' | grep -m1 scheduler)
docker exec -it $S bash -c "cd /usr/local/airflow && python -m pytest -q tests/helpers/test_parse_shot_xg.py"
# 4. backfill every competition and season (tmux; resumable)
docker exec -it $S python /usr/local/airflow/include/scripts/reload_parsers.py --parsers shot_xg
# 5. rebuild the marts: trigger the_data_xi_dbt, or by hand in the scheduler
#    cd /usr/local/airflow/the_data_xi_dbt && dbt build -s stg_ws_shot_xg+
```

## Checks worth running

- Per competition-season, `sum(dxi_xg) / goals` on non-penalty shots between
  .90 and 1.12; `sum(dxi_xgot) / goals` on target .93–1.07 in the top five
  (.85–1.10 elsewhere); share of shots on target .30–.36.
- **Drift**: mean `dxi_xg` per non-penalty shot by month and competition — a
  move of more than 4% against the previous three months means WhoScored changed
  how it records something (that is how the Sep 2025 pass end point change was
  found).

## Yearly refit

After each season, on the server: xG `x1_extract_ws.py` → `x2_features.py` →
`x6_export.py` (`~/work/xg_research/scripts`), then xGOT `g1_base.py` →
`g7_export.py` (`~/work/xgot_research/scripts`). Copy the model files into
`include/helpers/whoscored/models/` (meta files as `xg_meta.json` and
`xgot_meta.json`), bump `MODEL_VERSION` in `parse_shot_xg.py`, and re-run the
parser for the **new season only** — old seasons keep the version they were
scored with; rescoring history is a deliberate act.
