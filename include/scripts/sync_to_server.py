#!/usr/bin/env python3
"""
sync_to_server.py
=================
Standalone tool. Pushes scraped match data from this machine to the server's
raw-data folder, sending only files that are new or changed since the last run.

The scraper's `Done` folder is already in the layout the loader expects:

    {SOURCE_DIR}/
    └── {competition}/                  e.g. Premier_League
        └── {season}/                   e.g. 2026_2027
            └── {combo_id}/             e.g. 2026-08-21ArsCov
                ├── whoscored/  fotmob/  sofascore/  oddspedia/

and is mirrored 1:1 into {REMOTE_DIR} on the server (mounted into Airflow as
/usr/local/airflow/data).

How it works:
    - A manifest ({SOURCE_DIR}/.sync_manifest.json) records the size + mtime of
      every file already sent. Anything new, or whose size/mtime changed
      (e.g. a re-scrape), is sent on the next run.
    - Files are streamed as a gzipped tar over the existing `ssh` config
      (Host alias SSH_HOST) — no rsync, no extra installs.
    - One batch per competition/season; the manifest is saved after each batch
      succeeds, so a dropped connection only costs the batch in flight.

Usage:
    Edit the CONFIG block, then run:

        python sync_to_server.py            # send everything new/changed
        python sync_to_server.py --dry-run  # report only, send nothing
"""

from __future__ import annotations

import json
import subprocess
import sys
import tarfile
from collections import defaultdict
from pathlib import Path

# ════════════════════════════════════════════════════════════════════════════════
#  CONFIG — edit these, then run:  python sync_to_server.py
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_DIR  = r"C:\Users\Leosuky\Desktop\Code\Scraper\Done"   # local scraper output
# SOURCE_DIR = r"D:\Consolidated_fixtures\data"
SSH_HOST    = "dataxi"                                         # Host alias in ~/.ssh/config
REMOTE_DIR  = "~/the-data-xi/data"                             # server raw-data folder

COMPETITIONS = None        # e.g. ["Premier_League", "UCL"]; None = all
SEASONS      = None        # e.g. ["2026_2027"]; None = all

# COMPETITIONS = ["Ligue_1"]
# SEASONS = [
#     "2018_2019",
#     "2019_2020",
#     # "2020_2021",
#     # "2021_2022",
#     # "2022_2023",
#     # "2023_2024",
#     # "2024_2025",
#     # "2025_2026",
# ]

# european club coefficient ratings.

# ════════════════════════════════════════════════════════════════════════════════

MANIFEST_NAME = ".sync_manifest.json"


def load_manifest(path: Path) -> dict[str, list[int]]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_manifest(path: Path, manifest: dict[str, list[int]]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)                         # atomic: never leaves a half-written manifest


def find_changed(source: Path, manifest: dict[str, list[int]]) -> dict[str, list[tuple[Path, str, list[int]]]]:
    """Return {"competition/season": [(abs_path, rel_posix, [size, mtime]), ...]} for new/changed files."""
    batches: dict[str, list[tuple[Path, str, list[int]]]] = defaultdict(list)
    for f in source.rglob("*"):
        if not f.is_file():
            continue
        rel = f.relative_to(source)
        parts = rel.parts
        if len(parts) < 4 or rel.name.startswith(".sync_manifest"):
            continue                          # skip stray files outside comp/season/combo/
        if COMPETITIONS and parts[0] not in COMPETITIONS:
            continue
        if SEASONS and parts[1] not in SEASONS:
            continue
        st = f.stat()
        sig = [st.st_size, int(st.st_mtime)]
        key = rel.as_posix()
        if manifest.get(key) != sig:
            batches[f"{parts[0]}/{parts[1]}"].append((f, key, sig))
    return batches


def send_batch(files: list[tuple[Path, str, list[int]]]) -> bool:
    """Stream files as tar.gz into `ssh HOST tar -x` on the server. True on success."""
    remote_cmd = f"mkdir -p {REMOTE_DIR} && tar -xzf - -C {REMOTE_DIR}"
    proc = subprocess.Popen(["ssh", SSH_HOST, remote_cmd], stdin=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=proc.stdin, mode="w|gz") as tar:
            for path, rel, _ in files:
                tar.add(path, arcname=rel, recursive=False)
        proc.stdin.close()
    except (BrokenPipeError, OSError) as e:
        print(f"    ! stream failed: {e}")
        proc.kill()
        proc.wait()
        return False
    return proc.wait() == 0


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    source = Path(SOURCE_DIR)
    if not source.is_dir():
        print(f"SOURCE_DIR not found: {source}")
        return 1

    manifest_path = source / MANIFEST_NAME
    manifest = load_manifest(manifest_path)
    batches = find_changed(source, manifest)

    if not batches:
        print("Nothing new to send.")
        return 0

    total_files = sum(len(b) for b in batches.values())
    total_mb = sum(sig[0] for b in batches.values() for _, _, sig in b) / 1e6
    print(f"{total_files} new/changed files ({total_mb:,.1f} MB before compression)")

    failed = 0
    for key in sorted(batches):
        files = batches[key]
        mb = sum(sig[0] for _, _, sig in files) / 1e6
        combos = len({rel.split("/")[2] for _, rel, _ in files})
        print(f"  {key}: {len(files)} files, {combos} matches, {mb:,.1f} MB", end="")
        if dry_run:
            print("  [dry-run]")
            continue
        print(" ...", flush=True)
        if send_batch(files):
            for _, rel, sig in files:
                manifest[rel] = sig
            save_manifest(manifest_path, manifest)
            print("    ok")
        else:
            failed += 1
            print("    FAILED — will retry on next run")

    if failed:
        print(f"\n{failed} batch(es) failed. Re-run to retry; already-sent batches are skipped.")
        return 1
    print("\nDone." if not dry_run else "\nDry run — nothing sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
