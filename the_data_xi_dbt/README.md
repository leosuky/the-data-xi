# The Data XI - dbt staging layer

Staging is the ONLY place where (a) the RAW type/format debt is paid and (b)
cross-provider identity is resolved. No marts, dims, or facts live here.

## Layer rules
- `stg_*` = **views** (zero storage; RAW is never duplicated).
- `int_*_xwalk` = **tables**, full-refreshed each run so identity corrections
  propagate everywhere.
- Provider prefixes are kept (`stg_sofa_*`, `stg_fot_*`, `stg_ws_*`).
- Every `stg_*` attaches `player_uid` / `team_uid` (decision A=ii).

## Identity (ws-anchored)
`player_uid = ws_player_id`, `team_uid = ws_team_id` - stored as-is (no hash),
because WhoScored is required on every match and the ids are stable & debuggable.
`sofa_*`/`fot_*` ids are resolved ATTRIBUTES (via the crosswalks), never part of
identity - so correcting a mapping never orphans a fact row.

The `player_alias` / `team_alias` seeds are the ONLY override seam
(`COALESCE(alias.canonical_ws_id, ws_id)`), for the rare case WhoScored carries
one person under two ids. Empty by default.

### Crosswalk resolution (per (combo_id, is_home_team) match-side, then global vote)
1. exact `(combo_id, is_home_team, shirt_number)`
2. elimination (exactly one slot unmatched per side)
3. normalized name (`unaccent` recommended for accented names)

Most-frequent pairing across all ~10,500 matches wins; volume outvotes a
swapped/mistyped shirt. **Crosswalks read RAW via `source()`, not the stg lineup
views** - the stg views attach uids, which would otherwise create a cycle.

## The three stg tiers
- **trivial** (most `ws_*`): RAW already typed -> rename + attach uids.
  Pattern = `stg_ws_player_shooting.sql`.
- **dynamic-cast** (`sofa_player_stats`, `fot_player_stats`): dozens of
  season-varying TEXT cols -> `cast_text_numeric()` introspects & casts.
  Pattern = `stg_sofa_player_stats.sql`.
- **format-parse** (`fot_team_stats`): Fotmob display strings ('12/20 (60%)')
  -> leading number + raw kept in `<col>_display`. Pattern = `stg_fot_team_stats.sql`.
  Wide `*_team_stats`/`*_match_stats` are home/away per-period (no team_id), so
  they stay FLAT - no uid join, crosscheck on `combo_id`.

## Coverage (47 RAW tables -> 47 stg models)
Organized by provider under models/staging/:
  whoscored/ (26)  sofascore/ (14)  fotmob/ (6)  oddspedia/ (1)
plus crosswalks/ (int_player_xwalk, int_team_xwalk).

Generated uniformly by tier:
- team-grain (`*_team_*`)  -> team_uid only
- player-grain            -> player_uid + team_uid
- dynamic (`*_player_stats`) -> cast_text_numeric introspection
- wide-flat (`*_match_stats`,`fot_team_stats`) -> flat cast, no uid (home/away)
- entity/odds (no player/team_id) -> flat passthrough

`id` and `ingested_at` are dropped at staging; provider ids kept alongside uids.

## Status
Built & tested via dbt (run + tests green): both crosswalks, both lineup spines,
one example per tier. The remaining trivial `stg_ws_*` views are mechanical
repeats of the trivial pattern - enumerate columns or use `dbt_utils.star`.
