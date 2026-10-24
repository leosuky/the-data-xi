# Command cheat sheet

The commands we use day to day, grouped by where you run them.

- **PowerShell** = your Windows PC. Every server command goes through the
  `dataxi` SSH alias (set in `~/.ssh/config`).
- **Server** = the Oracle VM, after `ssh dataxi`.

Where things live on the server:

| Path | What |
|---|---|
| `~/the-data-xi` | this repo (pull-only — change code on your PC, then push) |
| `~/the-data-xi/data` | raw scraped match files (mounted in Airflow as `/usr/local/airflow/data`) |
| `~/work` | server Claude's workspace (`CLAUDE.md`, `.claude/commands/`, `prematch/`) |
| `~/work/reports` | `gameweek_*.md` reports, `picks_log.csv`, `odds_snapshots.csv` |
| `~/backups` | nightly warehouse dumps (`latest.dump`) |

---

## 1. Files between your PC and the server (PowerShell)

`scp <from> <to>`. Server paths start with `dataxi:`. Quote Windows paths.

```powershell
# Download one report
scp "dataxi:~/work/reports/gameweek_2026-10-20_Champions_League.md" "$env:USERPROFILE\Downloads\reports\"

# Download all reports from a given day (quote the wildcard so the server expands it)
scp "dataxi:~/work/reports/gameweek_2026-10-20_*.md" "$env:USERPROFILE\Downloads\reports\"

# Download the picks log
scp "dataxi:~/work/reports/picks_log.csv" "$env:USERPROFILE\Downloads\reports\"

# List what reports exist before downloading
ssh dataxi "ls -lt ~/work/reports | head -20"

# Upload a file (e.g. a new skill or SQL script) to the server
scp "$env:USERPROFILE\Downloads\reports\server_setup\gameweek-picks-v6.md" dataxi:~/work/

# Download the latest warehouse backup
scp dataxi:~/backups/latest.dump "$env:USERPROFILE\Downloads\"
```

Upload scraped match data (only new/changed files, via a manifest):

```powershell
python include\scripts\sync_to_server.py --dry-run   # see what would be sent
python include\scripts\sync_to_server.py             # send it
```

Edit the `COMPETITIONS` / `SEASONS` block at the top of the script to limit it.

---

## 2. Connecting and the server Claude session

```powershell
ssh dataxi                 # log in
```

On the server — Claude runs inside tmux so it survives disconnects:

```bash
tmux ls                    # list sessions
tmux attach -t <name>      # re-attach (e.g. tmux attach -t claude)
# detach without stopping it: Ctrl+B, then D
```

You can also reach the same session from the Claude app on your phone via
Remote Control — no SSH needed.

---

## 3. Deploying code changes (server)

Code is changed on your PC, pushed to GitHub, then pulled on the server:

```bash
cd ~/the-data-xi && git pull
```

dbt model / test changes need no restart: trigger the `the_data_xi_dbt` DAG in
the Airflow UI. Changes to DAG files or `include/helpers/` are picked up by
Airflow on the next DAG parse (about a minute); restart only if they aren't:

```bash
cd ~/the-data-xi && astro dev restart
```

Airflow UI from your PC: forward the port, then open http://localhost:8080

```powershell
ssh -L 8080:localhost:8080 dataxi
```

---

## 4. Warehouse (server)

```bash
# psql shell as the owner
docker exec -it the_data_xi_postgres psql -U the_data_xi -d the_data_xi

# one-off query
docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi -c "select count(*) from marts.fct_shots;"

# run a SQL file from the repo
docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi -f - < include/scripts/inspect_marts_tables.sql

# delete one match from every raw table, then re-run dbt
docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi \
  -v combo=2026-10-10RayAth -f - < include/scripts/purge_match.sql
```

Useful psql commands: `\dn` schemas · `\dt marts.*` tables · `\d marts.fct_shots`
columns · `\x` expanded rows · `\q` quit.

Backups run nightly from cron (`include/scripts/backup_warehouse.sh`). Restore
into an empty warehouse:

```bash
docker exec -i the_data_xi_postgres pg_restore -U the_data_xi -d the_data_xi \
  --clean --if-exists --no-owner < dataxi_<timestamp>.dump
```

---

## 5. dbt by hand (inside the Airflow scheduler container)

Normally the `the_data_xi_dbt` DAG does this. To run one model yourself:

```bash
docker ps --format '{{.Names}}' | grep scheduler       # find the container
docker exec -it <scheduler-container> bash
cd /usr/local/airflow/the_data_xi_dbt
dbt build -s fct_shots          # one model + its tests
dbt run -s marts                # a whole folder
dbt test -s marts
```

Timings of the last run: `target/run_results.json` (overwritten by each dbt
command) and `logs/dbt.log`.

---

## 6. Server Claude commands (type these in its session)

| Command | What it does |
|---|---|
| `/gameweek-picks <Competitions> [dates]` | v6 picks: form screen → context → price gate. Overs and AH only. Report to `~/work/reports/gameweek_<date>_<comps>.md`. e.g. `/gameweek-picks Champions_League` |
| `/settle-picks` | fills results, profit, closing odds and CLV in `picks_log.csv` |
| `took <selection> @<odds> <book>` | log a bet you actually placed |

---

## 7. Cron on the server

```bash
crontab -l        # see scheduled jobs (backups, closing-odds snapshots)
```

---

## 8. Re-parse every season after a parser change

`reload_parsers.py` does what the `the_data_xi_selective_reload` DAG does,
but over every competition and season in one run (4 matches at a time,
resumable, failures listed at the end). Run it in tmux; it takes hours for
the full warehouse.

```bash
S=$(docker ps --format '{{.Names}}' | grep -m1 scheduler)

# what would run (counts per competition/season), loads nothing
docker exec -it $S python /usr/local/airflow/include/scripts/reload_parsers.py \
  --parsers passing,passing_advanced,discipline,game_state --dry-run

# the real thing (same command without --dry-run); re-run it to resume
docker exec -it $S python /usr/local/airflow/include/scripts/reload_parsers.py \
  --parsers passing,passing_advanced,discipline,game_state
```

Narrow it with `--competitions Premier_League` or `--seasons 2025_2026`.
When it finishes, trigger `the_data_xi_dbt` once to rebuild the marts.
