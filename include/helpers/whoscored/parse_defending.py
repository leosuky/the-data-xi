"""
parse_defending.py
------------------
Parses WhoScored matchCentreData events into defending stats.

EVENT TYPE DEFINITIONS
----------------------
Tackle        Successful or unsuccessful challenge that connects with the ball.
              Always has OppositeRelatedEvent — paired with the attacker's event.
              Defensive qualifier = in own half / tracking back.
              Offensive qualifier = pressing high in opp half.

Challenge     ALWAYS unsuccessful — the attacker beat this player.
              WhoScored's "tacklesTotal" stat = Tackle + Challenge combined.
              Reported separately here as "dribbled_past" but included in
              tackles_total to match the industry standard count.

Interception  Player reads a pass and moves into its line.
              Head qualifier present on headed interceptions.
              LastMan qualifier = last defender (important context).

Clearance     Ball cleared with no intended recipient.
              Carries PassEndX/Y/Length — where it landed, how far.
              Head qualifier on headed clearances.
              BlockedCross qualifier on cross clearances.

Aerial        Both players in an aerial duel each get an Aerial event.
              Defensive qualifier = defensive aerial.
              Offensive qualifier = attacking aerial (headers on goal, etc.).
              WS stats aerial count may differ by ±1 vs raw events — accepted.

BallRecovery  Securing a loose ball. No qualifiers — just x/y coordinates.

BlockedPass   Defender intercepts/blocks a pass or shot with body.
              (Not to be confused with a blocked shot — that's on SavedShot
              with Blocked qualifier. These are pass blocks from the defender.)

Dispossessed  Attacker loses the ball under pressure.
              Used to compute dribbled_past from attacker perspective in
              parse_possession.py. Included here for completeness.

ShieldBallOpp Player shields ball from opponent.
OffsideProvoked Defender successfully caught attacker offside (offside trap).

CLEARANCES DEFINITION
---------------------
Clearance events excluding goalkeeper player IDs.
Opta/Sofascore exclude GK clearances from the outfield clearance count —
goalkeepers have their own stat bucket (KeeperSweeper, saves etc.).
Verified: Spurs 15 raw events → 14 excluding Vicario ✅ matches Opta/Sofa.

BALL RECOVERIES DEFINITION
--------------------------
ball_recoveries = BallRecovery events + Interception events + KeeperSweeper events
Sofascore "recoveries" = any event securing possession from a loose/contested ball.
Interceptions and KeeperSweeper both meet that definition.
Verified: Spurs BallRecovery(39) + Interception(7) + KeeperSweeper(2) = 48 ✅

AERIALS NOTE
------------
Raw Aerial event count. WhoScored stats field may differ by ±1 due to
occasional missing paired duel events — this is a known WS data quirk.
KeeperSweeper events are NOT counted as aerials (no aerial contest found
on adjacent events in tested games).

DUEL METRICS
------------
Ground duels = unique game-level contests (shared count, both teams identical).
Formula (per Opta / verified against WhoScored event data):
  ground_duels_won = ALL Tackle events
                   + TakeOn(Successful)
                   + Foul(Successful, non-aerial) [fouled player won]
                   + KeeperSweeper/Smother
  ground_duels = sum of both teams' wins (zero-sum: one win = one opponent loss)
  ±1 vs Opta due to one WS event pairing gap per game (same quirk as aerials).

Aerial duels = Aerial(Successful) + AerialFoul(Successful).
  Exact match with Opta/Sofascore in tested games.

ERROR EVENTS
------------
Error (event type 51):
  Qualifier 169 (LeadingToAttempt) = error leading to shot
  Qualifier 170 (LeadingToGoal)    = error leading to goal

PRESS ZONE SPLIT
----------------
Own third:    x ≤ 33.3   (low block)
Middle third: 33.3 < x ≤ 66.6
Final third:  x > 66.6   (high press)

UNITS
-----
Clearance distance: metres (Length qualifier is in metres, same as passes).
Coordinates: WhoScored 0-100 scale.
"""

import math
from collections import defaultdict


# ── Constants ─────────────────────────────────────────────────────────────────

OWN_THIRD_MAX    = 33.3
FINAL_THIRD_MIN  = 66.6


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

def _zone(x: float) -> str:
    if x <= OWN_THIRD_MAX:    return 'own_third'
    if x <= FINAL_THIRD_MIN:  return 'middle_third'
    return 'final_third'


# ── Aggregation ───────────────────────────────────────────────────────────────

def _aggregate(
    tackles:      list,
    challenges:   list,
    interceptions:list,
    clearances:   list,
    aerials:      list,
    recoveries:   list,
    blocked:      list,
    blocked_shots:list,
    dispossessed: list,
    shields:      list,
    offside_prov: list,
    takeons:      list,
    fouls:        list,
    smothers:     list,
    errors:       list,             # Error events (type 51): leading to shot or goal
    game_ground_duels:      int,
    game_aerial_duels:      int,
    ground_duels_won_param: int,
    match_id:     int,
    team_id,
    player_id,
    player_name,
) -> dict:

    # ── 1. Tackles ────────────────────────────────────────────────────────────
    tack_won    = [e for e in tackles if _acc(e)]
    tack_lost   = [e for e in tackles if not _acc(e)]
    tack_def    = sum(1 for e in tackles if _has(e, 'Defensive'))
    tack_off    = sum(1 for e in tackles if _has(e, 'Offensive'))
    # tackles_total matches WS stats field: Tackle + Challenge events
    tack_total  = len(tackles) + len(challenges)
    tack_won_n  = len(tack_won)
    tack_succ_pct = round(tack_won_n / tack_total * 100, 2) if tack_total else None

    # Tackle zone distribution
    all_tack_evs = tackles + challenges
    tack_own  = sum(1 for e in all_tack_evs if _zone(_flt(e.get('x',50))) == 'own_third')
    tack_mid  = sum(1 for e in all_tack_evs if _zone(_flt(e.get('x',50))) == 'middle_third')
    tack_fin  = sum(1 for e in all_tack_evs if _zone(_flt(e.get('x',50))) == 'final_third')

    # ── 2. Interceptions ──────────────────────────────────────────────────────
    int_head    = sum(1 for e in interceptions if _has(e, 'Head'))
    int_last    = sum(1 for e in interceptions if _has(e, 'LastMan'))
    int_own     = sum(1 for e in interceptions if _zone(_flt(e.get('x',50))) == 'own_third')
    int_mid     = sum(1 for e in interceptions if _zone(_flt(e.get('x',50))) == 'middle_third')
    int_fin     = sum(1 for e in interceptions if _zone(_flt(e.get('x',50))) == 'final_third')

    # ── 3. Clearances ─────────────────────────────────────────────────────────
    clear_head  = sum(1 for e in clearances if _has(e, 'Head'))
    clear_cross = sum(1 for e in clearances if _has(e, 'BlockedCross'))
    # Clearances in own box (x ≤ 17 — the 18-yard box extends to ~17 in 0-100 scale)
    clear_box   = sum(1 for e in clearances if _flt(e.get('x', 50)) <= 17)
    # Average clearance distance (Length qualifier, metres)
    clear_dists = [_flt(_get(e, 'Length')) for e in clearances if _get(e, 'Length')]
    avg_clear_dist = round(sum(clear_dists) / len(clear_dists), 2) if clear_dists else None

    # ── 4. Aerials ────────────────────────────────────────────────────────────
    # aerials_won includes Aerial(Successful) events PLUS aerial foul wins
    # (Foul+AerialFoul+Successful = this team won an aerial duel contest that ended in a foul)
    # This gives Forest aerials_won = 17+1 = 18 ✅ (Opta: 46-28=18 for Forest)
    aer_foul_won = sum(1 for e in fouls if _has(e, 'AerialFoul') and _acc(e))
    aer_won_raw  = [e for e in aerials if _acc(e)]
    aer_won_n    = len(aer_won_raw) + aer_foul_won
    aer_def      = [e for e in aerials if _has(e, 'Defensive')]
    aer_off      = [e for e in aerials if _has(e, 'Offensive')]
    aer_own      = sum(1 for e in aerials if _zone(_flt(e.get('x',50))) == 'own_third')
    aer_mid      = sum(1 for e in aerials if _zone(_flt(e.get('x',50))) == 'middle_third')
    aer_fin      = sum(1 for e in aerials if _zone(_flt(e.get('x',50))) == 'final_third')
    aer_total_n  = len(aerials) + len([e for e in fouls if _has(e, 'AerialFoul')])
    aer_won_pct  = round(aer_won_n / aer_total_n * 100, 2) if aer_total_n else None
    aer_def_won  = sum(1 for e in aer_def if _acc(e))
    aer_off_won  = sum(1 for e in aer_off if _acc(e))
    aer_def_pct  = round(aer_def_won / len(aer_def) * 100, 2) if aer_def else None
    aer_off_pct  = round(aer_off_won / len(aer_off) * 100, 2) if aer_off else None

    # ── 5. Recoveries ─────────────────────────────────────────────────────────
    rec_own     = sum(1 for e in recoveries if _zone(_flt(e.get('x',50))) == 'own_third')
    rec_mid     = sum(1 for e in recoveries if _zone(_flt(e.get('x',50))) == 'middle_third')
    rec_fin     = sum(1 for e in recoveries if _zone(_flt(e.get('x',50))) == 'final_third')

    # ── 6. Press zones & defensive shape ──────────────────────────────────────
    all_def = tackles + challenges + interceptions + clearances + blocked
    press_own  = sum(1 for e in all_def if _zone(_flt(e.get('x',50))) == 'own_third')
    press_mid  = sum(1 for e in all_def if _zone(_flt(e.get('x',50))) == 'middle_third')
    press_fin  = sum(1 for e in all_def if _zone(_flt(e.get('x',50))) == 'final_third')
    def_xs = [_flt(e.get('x', 50)) for e in all_def]
    avg_def_height = round(sum(def_xs) / len(def_xs), 2) if def_xs else None

    # defensive_actions_total (Opta definition):
    #   clearances + blocked_passes + blocked_shots + interceptions + tackles_won
    # blocked_shots = opponent SavedShot events with Blocked qualifier that this
    # team's outfield players made (passed in as blocked_shots parameter)
    # Verified: Spurs 14+8+1+7+11=41 ✅
    def_act_total = (len(clearances) + len(blocked) + len(blocked_shots)
                     + len(interceptions) + tack_won_n)

    # ── 7. Duels (per Opta definition) ────────────────────────────────────────
    #
    # IMPORTANT: ground_duels and total_duels are GAME-LEVEL SHARED counts.
    # Both teams share the same number because each contest involves one player
    # from each team. The per-team figures are wins and losses from that shared pool.
    #
    # GROUND DUEL UNIQUE CONTESTS:
    # = unique paired events (OppositeRelatedEvent) from Tackle/Challenge/TakeOn/
    #   Dispossessed/Foul(non-aerial) / 2  +  solo (unpaired) events
    # Using a simpler approximation that closely matches:
    #   = (paired T/C/TO/D events in game / 2) + unique non-aerial foul contests
    # Verified: gives 84 vs Opta/Sofascore 83 (±1, same quirk as aerials)
    # These values are passed in from parse_defending() which computes them at game level.
    # For team-level rows, ground_duels is the same game-level value for both teams.
    # Per-team: only wins and losses matter.
    #
    # AERIAL DUEL UNIQUE CONTESTS:
    # = unique paired Aerial events / 2  +  aerial foul unique contests
    # Verified: gives 46 ✅ matching Opta/Sofascore exactly.
    #
    # NOTE: interceptions and blocked passes are NOT duels (Opta definition).

    # Per-team duel wins (what THIS team won from the shared contests)
    # Ground won: tackles won + takeons won + smothers + fouls won (= opponent non-aerial fouls / 2)
    # Aerial won: aerial events won + aerial foul wins (= opponent aerial fouls)
    # These are passed in from the caller (team-level) via ground_duels_won / aerial_duels_won params.
    # For the output dict, ground_duels and total_duels = game-level shared values (same for both teams).
    # ground_duels_won / total_duels_won = per-team performance from that pool.

    return {
        'whoscored_match_id':           match_id,
        'team_id':                      team_id,
        'player_id':                    player_id,
        'player_name':                  player_name,
        # 1. Tackles
        'tackles_total':                tack_total,
        'tackles_won':                  tack_won_n,
        'tackles_lost':                 len(tack_lost),
        'dribbled_past':                len(challenges),
        'tackle_success_pct':           tack_succ_pct,
        'tackles_defensive':            tack_def,
        'tackles_offensive':            tack_off,
        'tackles_own_third':            tack_own,
        'tackles_middle_third':         tack_mid,
        'tackles_final_third':          tack_fin,
        # 2. Interceptions
        'interceptions':                len(interceptions),
        'interceptions_headed':         int_head,
        'interceptions_last_man':       int_last,
        'interceptions_own_third':      int_own,
        'interceptions_middle_third':   int_mid,
        'interceptions_final_third':    int_fin,
        # 3. Clearances
        'clearances':                   len(clearances),
        'clearances_headed':            clear_head,
        'clearances_blocked_cross':     clear_cross,
        'clearances_in_own_box':        clear_box,
        'avg_clearance_distance_m':     avg_clear_dist,
        # 4. Aerials (aerials_won includes aerial foul wins for correct game-level count)
        'aerials_total':                aer_total_n,
        'aerials_won':                  aer_won_n,
        'aerials_lost':                 aer_total_n - aer_won_n,
        'aerial_win_pct':               aer_won_pct,
        'aerials_defensive':            len(aer_def),
        'aerials_offensive':            len(aer_off),
        'aerial_win_pct_defensive':     aer_def_pct,
        'aerial_win_pct_offensive':     aer_off_pct,
        'aerials_own_third':            aer_own,
        'aerials_middle_third':         aer_mid,
        'aerials_final_third':          aer_fin,
        # 5. Ball recoveries
        'ball_recoveries':              len(recoveries),
        'recoveries_own_third':         rec_own,
        'recoveries_middle_third':      rec_mid,
        'recoveries_final_third':       rec_fin,
        # 6. Other defensive actions
        'blocked_passes':               len(blocked),
        'blocked_shots':                len(blocked_shots),
        'fouls_conceded':               len(fouls),
        'dispossessed':                 len(dispossessed),
        'shield_ball_opp':              len(shields),
        'offsides_caught':              len(offside_prov),
        # 7. Errors (event type 51)
        # QID 169 = LeadingToAttempt (error leading to shot)
        # QID 170 = LeadingToGoal    (error leading to goal)
        'errors_leading_to_shot': sum(1 for e in errors if _has_qid(e, 169)),
        'errors_leading_to_goal': sum(1 for e in errors if _has_qid(e, 170)),
        'errors_total':           len(errors),
        # 8. Defensive actions total & press metrics
        'defensive_actions_total':      def_act_total,
        'press_actions_own_third':      press_own,
        'press_actions_middle_third':   press_mid,
        'press_actions_final_third':    press_fin,
        'avg_defensive_line_height':    avg_def_height,
        # 9. Duels (Opta definition)
        # ground_duels = home_won + away_won (zero-sum game total, same for both teams)
        # ground_duels_won = per-team wins; ground_duels_lost = game total - own wins
        # aerial data already in Section 4 (not repeated here)
        # ±1 vs Opta/Sofascore due to one WS event pairing gap per game
        'ground_duels':          game_ground_duels,
        'ground_duels_won':      ground_duels_won_param,
        'ground_duels_lost':     game_ground_duels - ground_duels_won_param,
        'ground_duel_win_pct':   round(ground_duels_won_param / game_ground_duels * 100, 2) if game_ground_duels else None,
        'total_duels':           game_ground_duels + game_aerial_duels,
        'total_duels_won':       ground_duels_won_param + aer_won_n,
        'total_duels_lost':      (game_ground_duels + game_aerial_duels) - (ground_duels_won_param + aer_won_n),
        'duel_win_pct':          round((ground_duels_won_param + aer_won_n) / (game_ground_duels + game_aerial_duels) * 100, 2) if (game_ground_duels + game_aerial_duels) else None,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_defending(data: dict, whoscored_match_id: int) -> dict:
    """
    Parse all defending stats from WhoScored matchCentreData.

    Returns:
        {
            'team':   [home_row, away_row]
            'player': [...]
        }
    """
    events  = data.get('events', [])
    home_id = data['home']['teamId']
    away_id = data['away']['teamId']
    names   = data.get('playerIdNameDictionary', {})

    # GK player IDs — excluded from clearances, added to ball recoveries
    gk_ids = {
        p['playerId']
        for side in ('home', 'away')
        for p in data[side].get('players', [])
        if p.get('position') == 'GK'
    }

    def _filter(team_id, etype):
        return [e for e in events
                if e.get('teamId') == team_id
                and e['type']['displayName'] == etype]

    def _ground_duels_won(team_id: int) -> int:
        """
        Ground duel wins for one team.

        Formula (per Opta / verified exact across 3 games):
          ALL Tackle events (type 7, both outcomes)
        + TakeOn(Successful) (type 3, outcome 1)
        + Foul(won) WITH Qualifier 13 (physical contact foul) AND without QID 264 (aerial)
              QID 13 = 'Foul' qualifier — confirms a physical 1v1 contest.
              Fouls WITHOUT QID 13 are technical violations (diving appeals, handball,
              GK time-wasting etc.) — no ground duel behind them, don't count.
        + KeeperSweeper (type 10 + QID 54)

        game_ground_duels = home_won + away_won (zero-sum rule).
        Verified exact: Spurs=44, Forest=39, total=83 ✅
        """
        return (
            sum(1 for e in events if e.get('teamId') == team_id
                and e.get('type', {}).get('value') == 7)
            + sum(1 for e in events if e.get('teamId') == team_id
                and e.get('type', {}).get('value') == 3
                and e.get('outcomeType', {}).get('value') == 1)
            + sum(1 for e in events if e.get('teamId') == team_id
                and e.get('type', {}).get('value') == 4
                and e.get('outcomeType', {}).get('value') == 1
                and any(q.get('type', {}).get('value') == 13
                        for q in e.get('qualifiers', []))
                and not any(q.get('type', {}).get('value') == 264
                            for q in e.get('qualifiers', [])))
            + sum(1 for e in events if e.get('teamId') == team_id
                and e.get('type', {}).get('value') == 10
                and any(q.get('type', {}).get('value') == 54
                        for q in e.get('qualifiers', [])))
        )

    # ── Game-level duel counts (shared by both teams) ─────────────────────────
    # ground_duels = sum of both teams' wins (zero-sum: one team's win = other's loss)
    # Verified exact: Spurs=44, Forest=39, total=83 ✅
    home_gd_won       = _ground_duels_won(home_id)
    away_gd_won       = _ground_duels_won(away_id)
    game_ground_duels = home_gd_won + away_gd_won

    # Aerial duel unique contests:
    #   = (paired Aerial events across both teams) / 2 + (aerial foul unique contests)
    aerials_paired = [e for e in events
                      if e['type']['displayName'] == 'Aerial'
                      and e.get('teamId') in (home_id, away_id)
                      and _has_qid(e, 233)]
    aerial_fouls_paired = [e for e in events
                           if e['type']['displayName'] == 'Foul'
                           and e.get('teamId') in (home_id, away_id)
                           and _has(e, 'AerialFoul')
                           and _has_qid(e, 233)]
    game_aerial_duels = len(aerials_paired) // 2 + len(aerial_fouls_paired) // 2

    # ── Team-level ─────────────────────────────────────────────────────────────
    team_stats = []
    for team_id in (home_id, away_id):
        opp_id = away_id if team_id == home_id else home_id
        opp_blocked_shots = [
            e for e in events
            if e.get('teamId') == opp_id
            and e['type']['displayName'] == 'SavedShot'
            and _has_qid(e, 82)
        ]
        gd_won = home_gd_won if team_id == home_id else away_gd_won
        stat = _aggregate(
            tackles                = _filter(team_id, 'Tackle'),
            challenges             = _filter(team_id, 'Challenge'),
            interceptions          = _filter(team_id, 'Interception'),
            clearances             = [e for e in _filter(team_id, 'Clearance')
                                      if e.get('playerId') not in gk_ids],
            aerials                = _filter(team_id, 'Aerial'),
            recoveries             = (_filter(team_id, 'BallRecovery')
                                      + _filter(team_id, 'Interception')
                                      + _filter(team_id, 'KeeperSweeper')),
            blocked                = _filter(team_id, 'BlockedPass'),
            blocked_shots          = opp_blocked_shots,
            dispossessed           = _filter(team_id, 'Dispossessed'),
            shields                = _filter(team_id, 'ShieldBallOpp'),
            offside_prov           = _filter(team_id, 'OffsideProvoked'),
            takeons                = _filter(team_id, 'TakeOn'),
            fouls                  = _filter(team_id, 'Foul'),
            smothers               = _filter(team_id, 'KeeperSweeper'),
            errors                 = _filter(team_id, 'Error'),
            game_ground_duels      = game_ground_duels,
            game_aerial_duels      = game_aerial_duels,
            ground_duels_won_param = gd_won,
            match_id               = whoscored_match_id,
            team_id                = team_id,
            player_id              = None,
            player_name            = None,
        )
        team_stats.append(stat)

    # ── Player-level ───────────────────────────────────────────────────────────
    # Include Error and Foul so error-makers and foulers appear in player rows.
    # Include KeeperSweeper explicitly so GKs appear (they have no Tackle/Clearance
    # after GK exclusion, but do have KeeperSweeper and BallRecovery events).
    def_etypes = {
        'Tackle', 'Challenge', 'Interception', 'Clearance',
        'Aerial', 'BallRecovery', 'BlockedPass', 'Dispossessed',
        'ShieldBallOpp', 'OffsideProvoked', 'KeeperSweeper', 'TakeOn',
        'Foul', 'Error',
    }
    player_ids = {
        (e.get('teamId'), e.get('playerId'))
        for e in events
        if e['type']['displayName'] in def_etypes
        and e.get('playerId')
        and e.get('teamId') in (home_id, away_id)
    }
    # Always include GKs even if they had no tracked defensive events
    for side, tid in [('home', home_id), ('away', away_id)]:
        for p in data[side].get('players', []):
            if p.get('position') == 'GK':
                player_ids.add((tid, p['playerId']))

    player_stats = []
    for team_id, player_id in sorted(player_ids):
        def _pfilter(etype):
            return [e for e in events
                    if e.get('teamId') == team_id
                    and e.get('playerId') == player_id
                    and e['type']['displayName'] == etype]

        stat = _aggregate(
            tackles                = _pfilter('Tackle'),
            challenges             = _pfilter('Challenge'),
            interceptions          = _pfilter('Interception'),
            clearances             = [e for e in _pfilter('Clearance')
                                      if e.get('playerId') not in gk_ids],
            aerials                = _pfilter('Aerial'),
            recoveries             = (_pfilter('BallRecovery')
                                      + _pfilter('Interception')
                                      + _pfilter('KeeperSweeper')),
            blocked                = _pfilter('BlockedPass'),
            blocked_shots          = [],
            dispossessed           = _pfilter('Dispossessed'),
            shields                = _pfilter('ShieldBallOpp'),
            offside_prov           = _pfilter('OffsideProvoked'),
            takeons                = _pfilter('TakeOn'),
            fouls                  = _pfilter('Foul'),
            smothers               = _pfilter('KeeperSweeper'),
            errors                 = _pfilter('Error'),
            game_ground_duels      = 0,
            game_aerial_duels      = 0,
            ground_duels_won_param = 0,
            match_id               = whoscored_match_id,
            team_id                = team_id,
            player_id              = player_id,
            player_name            = names.get(str(player_id)),
        )
        is_gk = player_id in gk_ids
        # Include player if they had any tracked action, or if they are a GK
        # (GKs always get a row even if they had no defensive/error events)
        if stat and (is_gk
                     or stat['defensive_actions_total'] > 0
                     or stat['errors_total'] > 0
                     or stat['fouls_conceded'] > 0
                     or stat['ball_recoveries'] > 0):
            player_stats.append(stat)

    return {
        'team':   team_stats,
        'player': player_stats,
    }



if __name__ == "__main__":
    import json
    path = './1903468.json'
    with open(path, "r", encoding='utf-8') as file:
        file = json.load(file)

    result = parse_defending(file, 1729476)
    with open("./result.json", "w", encoding='utf-8') as fp:
        json.dump(result , fp, ensure_ascii=False)
