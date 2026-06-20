"""
parse_goalkeeping.py
--------------------
Parses WhoScored matchCentreData into goalkeeper-level stats.
Returns one row per GK per match. Team-level = the GK's own stats.

EVENT TYPES USED
----------------
Save          (displayName='Save')
    All saves carry outcome=Successful. Key qualifiers:
    QID 178  = StandingSave
    QID 179  = DivingSave
    QID 177  = Collected (caught cleanly)
    QID 173  = ParriedSafe (parried away from danger)
    QID 174  = ParriedDanger (parried into danger)
    QID 11115= KeeperSaveInTheBox (save made inside box)
    QID 11117= KeeperSaveObox (save outside box)
    QID 15   = Head
    QID 21   = OtherBodyPart

KeeperPickup  (displayName='KeeperPickup')
    Keeper claims ball — from crosses, through balls, loose balls.
    No standard qualifiers. x/y coordinate = where ball was claimed.
    Subset with StandingSave qualifier (QID 178) = claimed after a parry/rebound.

KeeperSweeper (displayName='KeeperSweeper')
    Keeper comes off line to intercept a through ball / counter.
    x coordinate = how far off the line the keeper ventured.

Punch         (displayName='Punch')
    Carries Length, PassEndX/Y, Angle — maps where punches land.
    Outcome = Successful (cleared danger) or Unsuccessful.

Pass          (displayName='Pass', by GK playerId)
    All GK distributions. Qualifiers:
    QID 124 = GoalKick
    QID 123 = KeeperThrow
    QID 1   = Longball
    QID 5   = FreekickTaken
    All carry PassEndX/Y, Length for distance/direction analysis.

Goal          (by opponent team)
    Goals conceded. Count opponent Goal events.

GOAL KICK DETECTION
-------------------
WhoScored sometimes omits the GoalKick qualifier (QID 124) on passes taken
from deep in the keeper's own goal area. We use a two-pronged approach:
  1. Pass with QID 124 (explicitly tagged)
  2. Pass with x ≤ 5.0 AND no KeeperThrow/FreekickTaken/Indirect FK qualifier
     (catches ~2 untagged goal kicks per game)
Verified: Vicario 5 goal kicks ✅ matches Sofascore.

HIGH CLAIMS
-----------
Sofascore "high claims" = GK winning an aerial challenge from a cross (Aerial events).
This is NOT the same as KeeperPickup (which counts all ball claims of any type).
GKs rarely have aerial events (most games = 0), so high_claims = GK Aerial events.
KeeperPickup is tracked separately as a distinct metric.

LONG DISTRIBUTION ACCURACY
---------------------------
Sofascore shows "2/9" for long balls = 2 accurate out of 9 attempts (22.22% accuracy).
This is longball ACCURACY, not the % of distribution that is long.
Both metrics are tracked:
  longball_accuracy_pct  = accurate longballs / total longballs (matches Sofascore)
  long_kick_pct          = longballs / total passes (style index: how direct is the GK)

UNITS
-----
Distance: metres (Length qualifier). Coordinates: WhoScored 0-100 scale.
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

PENALTY_BOX_X  = 17.0    # 18-yard box boundary in WhoScored coords
GOAL_KICK_X    = 5.0     # x ≤ this = ball is in/at the goal area (goal kicks)
# Qualifier IDs that mark a GK pass as a KeeperThrow (excluded from passes/longballs)
QID_KEEPER_THROW = 123


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has(event, name: str) -> bool:
    return any(q['type']['displayName'] == name
               for q in event.get('qualifiers', []))

def _has_qid(event, qid: int) -> bool:
    return any(q.get('type', {}).get('value') == qid
               for q in event.get('qualifiers', []))

def _get(event, name: str):
    for q in event.get('qualifiers', []):
        if q['type']['displayName'] == name:
            return q.get('value')
    return None

def _flt(v, default=0.0) -> float:
    try: return float(v)
    except: return default

def _acc(event) -> bool:
    return event.get('outcomeType', {}).get('displayName') == 'Successful'

def _dist_to_goal(x: float, y: float) -> float:
    """Euclidean distance to centre of opponent goal (100, 50). Units: metres."""
    return math.sqrt(((100 - x) * 1.05) ** 2 + ((50 - y) * 0.68) ** 2)


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_goalkeeping(data: dict, whoscored_match_id: int) -> dict:
    """
    Parse all goalkeeper stats from WhoScored matchCentreData.

    Returns:
        {
            'keepers': [row_per_gk]   — one row per keeper
        }
    All stats are at keeper (player) level. For team-level use, join via team_id.
    """
    events   = data.get('events', [])
    home_id  = data['home']['teamId']
    away_id  = data['away']['teamId']
    names    = data.get('playerIdNameDictionary', {})

    # Collect all GKs: {player_id: (team_id, side, player_info)}
    gk_map = {}
    for side in ('home', 'away'):
        team_id = data[side]['teamId']
        opp_id  = away_id if side == 'home' else home_id
        for p in data[side].get('players', []):
            if p.get('position') == 'GK':
                gk_map[p['playerId']] = {
                    'team_id': team_id,
                    'opp_id':  opp_id,
                    'side':    side,
                }

    keeper_rows = []

    for gk_id, meta in gk_map.items():
        team_id = meta['team_id']
        opp_id  = meta['opp_id']
        gk_name = names.get(str(gk_id))

        # ── Filter events ─────────────────────────────────────────────────────
        gk_evs     = [e for e in events if e.get('playerId') == gk_id]
        opp_goals  = [e for e in events
                      if e.get('teamId') == opp_id
                      and e['type']['displayName'] == 'Goal']

        saves      = [e for e in gk_evs if e['type']['displayName'] == 'Save']
        pickups    = [e for e in gk_evs if e['type']['displayName'] == 'KeeperPickup']
        sweepers   = [e for e in gk_evs if e['type']['displayName'] == 'KeeperSweeper']
        punches    = [e for e in gk_evs if e['type']['displayName'] == 'Punch']
        passes     = [e for e in gk_evs if e['type']['displayName'] == 'Pass']
        aerials    = [e for e in gk_evs if e['type']['displayName'] == 'Aerial']
        clearances = [e for e in gk_evs if e['type']['displayName'] == 'Clearance']

        # ── 1. Shot stopping ──────────────────────────────────────────────────
        # Exclude FromShotOffTarget saves from the total — Fotmob/Opta does not
        # count these as official saves (keeper touches ball already going wide).
        # However, style qualifiers (diving, standing etc.) ARE counted from all
        # save events including FromShotOffTarget, as they track keeper technique.
        saves_proper    = [s for s in saves if not _has(s, 'FromShotOffTarget')]
        n_saves         = len(saves_proper)
        n_goals         = len(opp_goals)
        sot_faced       = n_saves + n_goals
        save_pct        = round(n_saves / sot_faced * 100, 2) if sot_faced else None

        # Style qualifiers from ALL save events (including FromShotOffTarget)
        standing        = sum(1 for s in saves if _has_qid(s, 178))
        diving          = sum(1 for s in saves if _has_qid(s, 179))
        in_box          = sum(1 for s in saves_proper if _has_qid(s, 11115))
        out_of_box      = sum(1 for s in saves_proper if _has_qid(s, 11117))
        collected       = sum(1 for s in saves if _has(s, 'Collected'))
        parried_safe    = sum(1 for s in saves if _has(s, 'ParriedSafe'))
        parried_danger  = sum(1 for s in saves if _has(s, 'ParriedDanger'))
        head_saves      = sum(1 for s in saves if _has(s, 'Head'))
        other_body      = sum(1 for s in saves if _has_qid(s, 21))

        # ── 2. Aerial & sweeping ──────────────────────────────────────────────
        n_pickups          = len(pickups)
        pickups_after_save = sum(1 for p in pickups if _has_qid(p, 178))
        pickups_routine    = n_pickups - pickups_after_save

        n_sweepers   = len(sweepers)
        sweep_xs     = [_flt(e.get('x', 0)) for e in sweepers]
        avg_sweep_x  = round(sum(sweep_xs) / len(sweep_xs), 2) if sweep_xs else None

        n_punches    = len(punches)
        punch_acc    = sum(1 for p in punches if _acc(p))
        punch_dists  = [_flt(_get(p, 'Length')) for p in punches if _get(p, 'Length')]
        avg_punch_dist = round(sum(punch_dists) / len(punch_dists), 2) if punch_dists else None

        # High claims = GK Aerial events (Sofascore definition — NOT KeeperPickup)
        hc_total    = len(aerials)
        hc_won      = sum(1 for a in aerials if _acc(a))
        hc_win_pct  = round(hc_won / hc_total * 100, 2) if hc_total else None

        # ── 3. Distribution ───────────────────────────────────────────────────
        # KeeperThrows tracked separately and EXCLUDED from passes_total / longballs.
        # Fotmob reports Throws as a distinct stat, not in "Accurate passes" or
        # "Accurate long balls". Verified exact across 4 keepers in 2 games. ✅
        # Also: one GK throw per game is often tagged Longball in WS — excluded here.
        keeper_throws   = sum(1 for p in passes if _has_qid(p, QID_KEEPER_THROW))
        passes_no_throw = [p for p in passes if not _has_qid(p, QID_KEEPER_THROW)]

        n_passes    = len(passes_no_throw)
        passes_acc  = sum(1 for p in passes_no_throw if _acc(p))
        dist_acc_pct = round(passes_acc / n_passes * 100, 2) if n_passes else None

        # Goal kicks: ALL passes with QID 124 for the GK's team.
        # Modern teams use outfield defenders to take short goal kicks — WhoScored
        # correctly attributes those passes to whoever strikes the ball, but
        # Fotmob/Opta counts all team goal kicks as the GK's stat.
        # Verified: Ellborg WS=7 own passes → team=12 ✅ matches Fotmob.
        goal_kicks     = sum(1 for e in events
                             if e.get('teamId') == team_id
                             and e['type']['displayName'] == 'Pass'
                             and _has_qid(e, 124))
        freekick_passes= sum(1 for p in passes_no_throw if _has_qid(p, 5))

        lb_list  = [p for p in passes_no_throw if _has(p, 'Longball')]
        lb_n     = len(lb_list)
        lb_acc   = sum(1 for p in lb_list if _acc(p))
        lb_acc_pct = round(lb_acc / lb_n * 100, 2) if lb_n else None
        short_passes = n_passes - lb_n

        total_dist   = n_passes + n_punches + keeper_throws
        dist_lengths = [_flt(_get(p, 'Length')) for p in passes_no_throw if _get(p, 'Length')]
        avg_dist_length = round(sum(dist_lengths) / len(dist_lengths), 2) if dist_lengths else None
        long_kick_pct  = round(lb_n / n_passes * 100, 2) if n_passes else None
        short_kick_pct = round(short_passes / n_passes * 100, 2) if n_passes else None

        # ── 4. Outfield contribution ──────────────────────────────────────────
        outfield_touches = sum(1 for e in gk_evs if _flt(e.get('x', 0)) > PENALTY_BOX_X)
        outfield_passes  = sum(1 for p in passes_no_throw if _flt(p.get('x', 0)) > PENALTY_BOX_X)
        n_clearances     = len(clearances)

        prog_passes = 0
        for p in passes:
            if not _acc(p): continue
            x  = _flt(p.get('x', 0)); y  = _flt(p.get('y', 50))
            ex = _flt(_get(p, 'PassEndX') or 0); ey = _flt(_get(p, 'PassEndY') or 50)
            if ex == 0: continue
            if x > 33.3 and _dist_to_goal(ex, ey) < 0.75 * _dist_to_goal(x, y):
                prog_passes += 1

        # ── Build row ─────────────────────────────────────────────────────────
        keeper_rows.append({
            'whoscored_match_id':         whoscored_match_id,
            'team_id':                    team_id,
            'player_id':                  gk_id,
            'player_name':                gk_name,
            # 1. Shot stopping
            'saves':                      n_saves,
            'goals_conceded':             n_goals,
            'shots_on_target_faced':      sot_faced,
            'save_pct':                   save_pct,
            'saves_standing':             standing,
            'saves_diving':               diving,
            'saves_in_box':               in_box,
            'saves_out_of_box':           out_of_box,
            'saves_collected':            collected,
            'saves_parried_safe':         parried_safe,
            'saves_parried_danger':       parried_danger,
            'saves_with_head':            head_saves,
            'saves_other_body_part':      other_body,
            # 2. Aerial & sweeping
            'keeper_pickups':             n_pickups,
            'pickups_routine':            pickups_routine,
            'pickups_after_save':         pickups_after_save,
            'high_claims':                hc_total,
            'high_claims_won':            hc_won,
            'high_claim_win_pct':         hc_win_pct,
            'sweeper_actions':            n_sweepers,
            'avg_sweeper_x':              avg_sweep_x,
            'punches':                    n_punches,
            'punches_accurate':           punch_acc,
            'avg_punch_distance_m':       avg_punch_dist,
            # 3. Distribution
            'distributions_total':        total_dist,
            'passes_total':               n_passes,
            'passes_accurate':            passes_acc,
            'distribution_accuracy_pct':  dist_acc_pct,
            'goal_kicks':                 goal_kicks,
            'keeper_throws':              keeper_throws,
            'longballs':                  lb_n,
            'longballs_accurate':         lb_acc,
            'longball_accuracy_pct':      lb_acc_pct,
            'short_passes':               short_passes,
            'freekick_passes':            freekick_passes,
            'avg_distribution_length_m':  avg_dist_length,
            'long_kick_pct':              long_kick_pct,
            'short_kick_pct':             short_kick_pct,
            # 4. Outfield
            'touches_outside_box':        outfield_touches,
            'passes_outside_box':         outfield_passes,
            'clearances':                 n_clearances,
            'progressive_passes':         prog_passes,
            # 5. Advanced
            'xgot':                       None,
            'psxg_minus_ga':              None,
        })

    return {'keepers': keeper_rows}


if __name__ == "__main__":
    import json
    # path = './1903468.json'
    path = './whoscored_data/new_sun.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_goalkeeping(file, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, ensure_ascii=False)
