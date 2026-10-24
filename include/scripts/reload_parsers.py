"""
reload_parsers.py
=================
Re-run chosen providers/parsers over every competition and season in one go,
instead of triggering `the_data_xi_selective_reload` once per season by hand.

It calls exactly what the DAG calls (discovery + `load_match`), so every write
is the same delete-then-insert scoped to (table, combo_id): only the chosen
parsers' tables are rewritten, nothing else in a match is touched.

Run it inside the Airflow scheduler container (it has the code, the data and
the warehouse connection), in tmux so it survives a disconnect:

    S=$(docker ps --format '{{.Names}}' | grep -m1 scheduler)
    docker exec -it $S python /usr/local/airflow/include/scripts/reload_parsers.py \
        --parsers passing,passing_advanced,discipline,game_state

Options
    --providers     comma list (default: whoscored)
    --parsers       comma list of WhoScored units (default: all)
    --competitions  comma list of competition folders (default: all)
    --seasons       comma list of season folders (default: all)
    --workers       parallel matches (default: 4, same as the DAG)
    --dry-run       only count matches per competition/season, load nothing
    --fresh         ignore earlier progress and redo every match

Resumable: each finished combo_id is appended to a progress file, so re-running
the same command skips what's already done. Failures never stop the run; they
are listed at the end (and in the progress folder) to retry with the DAG's
`combo_ids` box.

Afterwards, rebuild the marts once: trigger `the_data_xi_dbt` (or
`dbt build -s marts` in the scheduler container).
"""

import argparse
import collections
import os
import sys
import time
from multiprocessing import Pool

sys.path.insert(0, "/usr/local/airflow")  # so `include.` imports work outside Airflow

from include.helpers.common.discovery import discover_unloaded_matches, parse_csv_param  # noqa: E402

FIXTURES_DIR = "/usr/local/airflow/data"  # {competition}/{season}/{combo_id}
POSTGRES_CONN = "the_data_xi_postgres"
TARGET_SCHEMA = "raw"
PROGRESS_DIR = "/tmp/reload_progress"

_conn = None
_opts = None


def _connect():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=POSTGRES_CONN).get_conn()


def _init_worker(opts):
    """One warehouse connection per worker process, reused for its matches."""
    global _conn, _opts
    _opts = opts
    _conn = _connect()


def _reload_one(match: dict) -> tuple[str, str | None]:
    from include.helpers.loader import load_match

    try:
        load_match(
            match_descriptor=match,
            conn=_conn,
            schema=TARGET_SCHEMA,
            providers=_opts["providers"],
            parsers=_opts["parsers"],
        )
        _conn.commit()
        return match["combo_id"], None
    except Exception as e:  # keep going; report at the end
        _conn.rollback()
        return match["combo_id"], f"{type(e).__name__}: {e}"


def _label(m: dict) -> str:
    parts = os.path.normpath(m["match_dir"]).split(os.sep)
    comp = m.get("competition") or (parts[-3] if len(parts) >= 3 else "?")
    season = m.get("season") or (parts[-2] if len(parts) >= 2 else "?")
    return f"{comp}/{season}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--providers", default="whoscored")
    ap.add_argument("--parsers", default=None)
    ap.add_argument("--competitions", default=None)
    ap.add_argument("--seasons", default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--data-dir", default=FIXTURES_DIR)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fresh", action="store_true")
    a = ap.parse_args()

    opts = {"providers": parse_csv_param(a.providers), "parsers": parse_csv_param(a.parsers)}
    matches = discover_unloaded_matches(
        fixtures_dir=a.data_dir,
        conn=None,  # unused with force_reload=True
        schema=TARGET_SCHEMA,
        force_reload=True,
        competitions=parse_csv_param(a.competitions),
        seasons=parse_csv_param(a.seasons),
    )
    per_season = collections.Counter(_label(m) for m in matches)
    print(f"{len(matches)} matches in {len(per_season)} competition-seasons")
    for k in sorted(per_season):
        print(f"  {k:40s} {per_season[k]:5d}")
    if a.dry_run or not matches:
        return

    tag = "_".join(opts["providers"] or ["all"]) + "__" + "_".join(opts["parsers"] or ["all"])
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    done_path = os.path.join(PROGRESS_DIR, f"{tag}.done")
    fail_path = os.path.join(PROGRESS_DIR, f"{tag}.failed")
    if a.fresh:
        for p in (done_path, fail_path):
            if os.path.exists(p):
                os.remove(p)
    done = set(open(done_path).read().split()) if os.path.exists(done_path) else set()
    todo = [m for m in matches if m["combo_id"] not in done]
    print(f"{len(done)} already done earlier, {len(todo)} to go "
          f"(providers={opts['providers']}, parsers={opts['parsers'] or 'all'}, "
          f"workers={a.workers})")
    print(f"progress: {done_path}")

    failures: list[tuple[str, str]] = []
    start, n = time.time(), 0
    with open(done_path, "a") as done_f, open(fail_path, "a") as fail_f, Pool(
        a.workers, initializer=_init_worker, initargs=(opts,)
    ) as pool:
        for combo_id, err in pool.imap_unordered(_reload_one, todo, chunksize=4):
            n += 1
            if err:
                failures.append((combo_id, err))
                fail_f.write(f"{combo_id}\t{err}\n")
                fail_f.flush()
            else:
                done_f.write(combo_id + "\n")
                done_f.flush()
            if n % 100 == 0 or n == len(todo):
                rate = n / (time.time() - start)
                eta = (len(todo) - n) / rate / 60 if rate else 0
                print(f"  {n}/{len(todo)}  {rate:.1f} matches/s  "
                      f"failed {len(failures)}  ~{eta:.0f} min left", flush=True)

    print(f"\nFinished: {n - len(failures)} reloaded, {len(failures)} failed "
          f"in {(time.time() - start) / 60:.0f} min")
    if failures:
        print("Failed (retry with the DAG's combo_ids box):")
        print("  " + ", ".join(c for c, _ in failures))
    print("Next: rebuild marts (trigger the_data_xi_dbt).")


if __name__ == "__main__":
    main()
