#!/usr/bin/env python3
"""
consolidate_fixtures.py
=======================
Standalone pre-pipeline tool. Consolidates match data that currently lives in
THREE separate on-disk locations into the single folder layout the loader's
discovery step expects.

Source locations (3 independent roots, each a competition/season/gameweek tree):
    1. WhoScored          — JSON per match (whoscored_<id>.json).
    2. Oddspedia+Fotmob   — JSON per match; the Fotmob file is prefixed `fotmob_`,
                            everything else is Oddspedia.
    3. Sofascore+FBref    — JSON files are Sofascore, CSV files are FBref.

Gameweek folders are ignored — matches are keyed on (season, combo_id) only.
Season folder names differ between locations (e.g. `2018_2019` vs `Season_18_19`)
and are normalised to a canonical `YYYY_YYYY`.

Output layout (one season folder = one pipeline `fixtures_dir`):
    {output}/
    └── {season}/                       e.g. 2018_2019
        └── {combo_id}/                 e.g. 2018-08-10ManLei
            ├── whoscored/  whoscored_<id>.json   (required)
            ├── fotmob/     fotmob_<id>.json       (optional)
            ├── sofascore/  <sf_id>_<type>.json    (optional)
            ├── oddspedia/  <op_id>_odds_<mkt>.json(optional)
            └── fbref/      *.csv                  (stored, not loaded)

Safety gate: before consolidating a season, the combo_id sets across the three
locations must match exactly. Any mismatch is logged in full and the run aborts
(nothing is copied) unless --allow-partial is passed.

Usage:
    Edit the CONFIG block near the top of this file (source roots, output dir,
    which seasons, copy/move, dry-run), then run:

        python consolidate_fixtures.py
"""

from __future__ import annotations

import logging
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

log = logging.getLogger("consolidate")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG — edit these, then run:  python consolidate_fixtures.py
# ═══════════════════════════════════════════════════════════════════════════════

# The three source roots (use raw strings on Windows, e.g. r"C:\data\Premier_League")
WHOSCORED_ROOT  = r""                    # location 1 — WhoScored JSON
ODDSFOTMOB_ROOT = r""                    # location 2 — Oddspedia + fotmob_*.json
SOFASCORE_ROOT  = r""                    # location 3 — Sofascore JSON + FBref CSV

OUTPUT_DIR      = r"./data/fixtures"     # where the consolidated tree is written

# Which seasons to process:
#   ALL_SEASONS = True   → every season found on disk (SEASONS ignored)
#   ALL_SEASONS = False  → only the canonical seasons listed in SEASONS
ALL_SEASONS     = False
SEASONS         = ["2018_2019"]          # e.g. ["2018_2019", "2019_2020", ...]

COMPETITION     = None                   # e.g. "Premier_League" to filter; None = all
MOVE            = False                  # True = move files, False = copy (safer)
DRY_RUN         = False                  # True = report only, write nothing
ALLOW_PARTIAL   = False                  # True = process the intersection instead of
                                         #        aborting when locations don't match
VERBOSE         = False                  # True = debug logging

# ═══════════════════════════════════════════════════════════════════════════════

# ── Patterns ──────────────────────────────────────────────────────────────────

# combo_id: YYYY-MM-DD + team codes, e.g. 2018-08-10ManLei
COMBO_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[A-Za-z]+$")

# season folder variants → canonical YYYY_YYYY
_SEASON_PATTERNS = [
    re.compile(r"(?P<y1>\d{4})[_\-/](?P<y2>\d{4})"),                 # 2018_2019 / 2018-2019
    re.compile(r"(?P<y1>\d{4})[_\-/](?P<y2>\d{2})\b"),              # 2018_19
    re.compile(r"(?:season[_\s\-]*)?(?P<y1>\d{2})[_\-/](?P<y2>\d{2})", re.I),  # Season_18_19 / 18_19
]

# strict discovery-side filename checks
WS_FILE_RE  = re.compile(r"^whoscored_\d+\.json$", re.I)
FM_FILE_RE  = re.compile(r"^fotmob_\d+\.json$", re.I)


def _expand_year(y: str) -> int:
    """'2018' -> 2018 ; '18' -> 2018."""
    return int(y) if len(y) == 4 else 2000 + int(y)


def normalize_season(text: str) -> str | None:
    """Map a folder name to canonical `YYYY_YYYY`, or None if not season-like."""
    for pat in _SEASON_PATTERNS:
        m = pat.search(text)
        if m:
            y1, y2 = _expand_year(m.group("y1")), _expand_year(m.group("y2"))
            if y2 == y1 + 1:                      # consecutive years = a real season
                return f"{y1}_{y2}"
    return None


def season_from_combo(combo_id: str) -> str | None:
    """Fallback: derive season from the combo's match date (Aug–Jul split)."""
    m = COMBO_RE.match(combo_id)
    if not m:
        return None
    year, month = int(m.group(1)), int(m.group(2))
    start = year if month >= 7 else year - 1     # July+ belongs to the new season
    return f"{start}_{start + 1}"


# ── Discovery of source combo folders ─────────────────────────────────────────

def find_combo_dirs(root: Path, competition: str | None) -> dict[tuple[str, str], Path]:
    """
    Walk a source root and return {(season, combo_id): combo_dir}.

    Season is taken from the first season-like path component; if none is found
    it falls back to the combo's match date. Gameweek folders are ignored.
    """
    found: dict[tuple[str, str], Path] = {}
    if not root.is_dir():
        log.error("source root does not exist: %s", root)
        return found

    for d in root.rglob("*"):
        if not d.is_dir():
            continue
        if not COMBO_RE.match(d.name):
            continue

        rel_parts = d.relative_to(root).parts

        # optional competition filter on the top-level folder
        if competition and not any(competition.lower() in p.lower() for p in rel_parts):
            continue

        # season: first season-like component, else derive from the combo date
        season = next((s for p in rel_parts if (s := normalize_season(p))), None)
        if season is None:
            season = season_from_combo(d.name)
        if season is None:
            log.warning("could not determine season for %s — skipping", d)
            continue

        key = (season, d.name)
        if key in found and found[key] != d:
            log.warning("duplicate %s under %s — keeping %s, ignoring %s",
                        key, root, found[key], d)
            continue
        found[key] = d
    return found


# ── Validation gate ───────────────────────────────────────────────────────────

def validate(seasons, ws_map, of_map, sf_map, allow_partial: bool) -> bool:
    """
    For each requested season, the combo_id sets across the three locations must
    match exactly. Logs every discrepancy. Returns True if safe to proceed.
    """
    ok = True
    for season in sorted(seasons):
        ws = {c for (s, c) in ws_map if s == season}
        of = {c for (s, c) in of_map if s == season}
        sf = {c for (s, c) in sf_map if s == season}

        if ws == of == sf:
            log.info("[%s] OK — %d matches aligned across all 3 locations", season, len(ws))
            continue

        ok = False
        universe = ws | of | sf
        log.error("[%s] MISMATCH — %d distinct combos across locations:", season, len(universe))
        log.error("    whoscored=%d  oddspedia/fotmob=%d  sofascore/fbref=%d",
                  len(ws), len(of), len(sf))
        for label, missing in (
            ("missing in whoscored",        universe - ws),
            ("missing in oddspedia/fotmob", universe - of),
            ("missing in sofascore/fbref",  universe - sf),
        ):
            if missing:
                log.error("    %s (%d): %s", label, len(missing),
                          ", ".join(sorted(missing)))
    if not ok and allow_partial:
        log.warning("validation failed but --allow-partial set: proceeding with the "
                    "INTERSECTION of combos present in all three locations.")
        return True
    return ok


# ── Copy / move ───────────────────────────────────────────────────────────────

def _place(src: Path, dest_dir: Path, move: bool, dry: bool, counts, kind: str):
    if dry:
        log.debug("    would %s %s -> %s", "move" if move else "copy", src.name, dest_dir)
        counts[kind] += 1
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    (shutil.move if move else shutil.copy2)(str(src), str(dest))
    counts[kind] += 1


def consolidate_match(combo_id, ws_dir, of_dir, sf_dir, out_combo, move, dry, counts):
    """Copy one match's files from the 3 source dirs into the output layout."""
    # 1. WhoScored (required) → whoscored/
    if ws_dir:
        for f in sorted(p for p in ws_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"):
            name = f.name if WS_FILE_RE.match(f.name) else None
            if name is None:
                digits = re.search(r"(\d+)", f.stem)
                if not digits:
                    log.warning("[%s] whoscored file %s has no numeric id; discovery "
                                "will not pick it up", combo_id, f.name)
                    target_name = f.name
                else:
                    target_name = f"whoscored_{digits.group(1)}.json"
                    log.info("[%s] renaming %s -> %s for discovery", combo_id, f.name, target_name)
            else:
                target_name = name
            dest_dir = out_combo / "whoscored"
            if not dry:
                dest_dir.mkdir(parents=True, exist_ok=True)
                (shutil.move if move else shutil.copy2)(str(f), str(dest_dir / target_name))
            counts["whoscored"] += 1

    # 2. Oddspedia + Fotmob → fotmob/ (fotmob_ prefix) and oddspedia/ (rest)
    if of_dir:
        for f in sorted(p for p in of_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"):
            if f.name.lower().startswith("fotmob_"):
                if not FM_FILE_RE.match(f.name):
                    log.warning("[%s] fotmob file %s isn't fotmob_<digits>.json; discovery "
                                "will skip it", combo_id, f.name)
                _place(f, out_combo / "fotmob", move, dry, counts, "fotmob")
            else:
                _place(f, out_combo / "oddspedia", move, dry, counts, "oddspedia")

    # 3. Sofascore (json) + FBref (csv)
    if sf_dir:
        for f in sorted(p for p in sf_dir.iterdir() if p.is_file()):
            ext = f.suffix.lower()
            if ext == ".json":
                _place(f, out_combo / "sofascore", move, dry, counts, "sofascore")
            elif ext == ".csv":
                _place(f, out_combo / "fbref", move, dry, counts, "fbref")


# ── Orchestration ─────────────────────────────────────────────────────────────

def run(args) -> int:
    ws_map = find_combo_dirs(Path(args.whoscored),  args.competition)
    of_map = find_combo_dirs(Path(args.oddsfotmob), args.competition)
    sf_map = find_combo_dirs(Path(args.sofascore),  args.competition)

    all_seasons = {s for (s, _) in (*ws_map, *of_map, *sf_map)}
    if not all_seasons:
        log.error("no combo folders found in any location — check the paths")
        return 2

    if args.all:
        seasons = all_seasons
    else:
        seasons = set(args.season or [])
        unknown = seasons - all_seasons
        if unknown:
            log.warning("requested seasons not found on disk: %s", ", ".join(sorted(unknown)))
        seasons &= all_seasons
    if not seasons:
        log.error("no matching seasons to process (found on disk: %s)",
                  ", ".join(sorted(all_seasons)))
        return 2

    log.info("processing seasons: %s", ", ".join(sorted(seasons)))

    # gate — validate ALL requested seasons before copying anything
    if not validate(seasons, ws_map, of_map, sf_map, args.allow_partial):
        log.error("aborting: combo_id mismatch across locations (nothing copied). "
                  "Fix the source data or pass --allow-partial.")
        return 1

    out_root = Path(args.output)
    totals = defaultdict(int)
    for season in sorted(seasons):
        combos = sorted({c for (s, c) in ws_map if s == season})
        if args.allow_partial:                       # intersection only
            of_c = {c for (s, c) in of_map if s == season}
            sf_c = {c for (s, c) in sf_map if s == season}
            combos = sorted(set(combos) & of_c & sf_c)

        log.info("[%s] consolidating %d matches%s", season, len(combos),
                 " (dry-run)" if args.dry_run else "")
        season_counts = defaultdict(int)
        for combo in combos:
            out_combo = out_root / season / combo
            consolidate_match(
                combo,
                ws_map.get((season, combo)),
                of_map.get((season, combo)),
                sf_map.get((season, combo)),
                out_combo, args.move, args.dry_run, season_counts,
            )
        log.info("[%s] done — %s", season,
                 ", ".join(f"{k}:{v}" for k, v in sorted(season_counts.items())) or "no files")
        for k, v in season_counts.items():
            totals[k] += v

    verb = "would copy/move" if args.dry_run else ("moved" if args.move else "copied")
    log.info("FINISHED. %s files — %s", verb,
             ", ".join(f"{k}:{v}" for k, v in sorted(totals.items())) or "none")
    return 0


def main():
    logging.basicConfig(
        level=logging.DEBUG if VERBOSE else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    missing = [n for n, v in (("WHOSCORED_ROOT", WHOSCORED_ROOT),
                              ("ODDSFOTMOB_ROOT", ODDSFOTMOB_ROOT),
                              ("SOFASCORE_ROOT", SOFASCORE_ROOT),
                              ("OUTPUT_DIR", OUTPUT_DIR)) if not str(v).strip()]
    if missing:
        log.error("set these CONFIG variables before running: %s", ", ".join(missing))
        return 2
    if not ALL_SEASONS and not SEASONS:
        log.error("set ALL_SEASONS = True, or list seasons in SEASONS")
        return 2

    args = SimpleNamespace(
        whoscored=WHOSCORED_ROOT,
        oddsfotmob=ODDSFOTMOB_ROOT,
        sofascore=SOFASCORE_ROOT,
        output=OUTPUT_DIR,
        all=ALL_SEASONS,
        season=None if ALL_SEASONS else list(SEASONS),
        competition=COMPETITION,
        move=MOVE,
        dry_run=DRY_RUN,
        allow_partial=ALLOW_PARTIAL,
    )
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
