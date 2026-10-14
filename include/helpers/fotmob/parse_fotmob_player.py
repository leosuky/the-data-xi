"""
include/helpers/fotmob/parse_fotmob_player.py
=============================================
Parses a Fotmob PLAYER PAGE json (fotmob_{player_id}.json) into rows for the
raw player-meta tables. This is the source of the granular positional data the
match feeds don't carry: per-position appearance counts, main-position flag and
pitch coordinates.

Returns
-------
{
  'player':    [ one row  ]   -> fot_player_meta
  'positions': [ N rows   ]   -> fot_player_positions   (EMPTY for coaches)
  'career':    [ N rows   ]   -> fot_player_career
}

COACHES
-------
A retired player who is now a coach has NO positional breakdown. Fotmob signals
this four independent ways, and this parser treats the entity as a coach if ANY
of them fire (belt-and-braces, since a single flag could change upstream):

    isCoach == True
    positionDescription has no 'positions' array
    positionDescription.primaryPosition.key == 'coach'
    coachStats is populated

For a coach we still emit the meta row — they keep height / age / foot /
country / primary team, and they remain a real identity in the warehouse — but
we emit ZERO position rows rather than inventing a 'Coach' pseudo-position.
Coaches also lack shirt / market value / contract end; those come back None.

NB Fotmob misspells "occurrences" as "occurences" in its payload. We read their
spelling and write ours.
"""

import re
import logging

log = logging.getLogger(__name__)


# ── small helpers ────────────────────────────────────────────────────────────
def _utc(node):
    """{'utcTime': '2002-07-26T00:00:00.000Z', ...} -> '2002-07-26T00:00:00.000Z'."""
    if isinstance(node, dict):
        return node.get('utcTime')
    return node or None


def _date_only(node):
    """Same, trimmed to a plain YYYY-MM-DD date string."""
    v = _utc(node)
    if not v:
        return None
    m = re.match(r'^(\d{4}-\d{2}-\d{2})', str(v))
    return m.group(1) if m else None


def _info_map(data):
    """
    playerInformation is a list of {title, translationKey, value:{...}}.
    Key it by translationKey — `title` is localised and unstable.
    """
    out = {}
    for item in data.get('playerInformation') or []:
        tk = item.get('translationKey')
        if tk:
            out[tk] = item
    return out


def _num(info, key):
    item = info.get(key) or {}
    return (item.get('value') or {}).get('numberValue')


def _key_val(info, key):
    item = info.get(key) or {}
    return (item.get('value') or {}).get('key')


def _fallback(info, key):
    item = info.get(key) or {}
    v = (item.get('value') or {}).get('fallback')
    return v if isinstance(v, (str, int, float)) else None


def _int(v):
    """Fotmob ships several numeric fields as strings ('98', '78'). Coerce them
    here so they never land in the warehouse as text (a text column that looks
    numeric sorts lexically and silently corrupts ordering/aggregation)."""
    if v is None or v == '':
        return None
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def is_coach(data) -> bool:
    """
    True when the entity has no playing-position breakdown to parse.
    Any one signal is enough — they are redundant on purpose.
    """
    pos_desc = data.get('positionDescription') or {}
    primary = (pos_desc.get('primaryPosition') or {}).get('key')
    return bool(
        data.get('isCoach')
        or 'positions' not in pos_desc
        or primary == 'coach'
        or data.get('coachStats')
    )


# ── row builders ─────────────────────────────────────────────────────────────
def _player_row(data, coach):
    info = _info_map(data)
    pos_desc = data.get('positionDescription') or {}
    primary = pos_desc.get('primaryPosition') or {}
    team = data.get('primaryTeam') or {}
    meta = data.get('meta') or {}

    return {
        'fotmob_player_id': data.get('id'),
        'player_name':      data.get('name'),
        'birth_date':       _date_only(data.get('birthDate')),
        'gender':           data.get('gender'),
        'is_coach':         coach,
        'is_captain':       bool(data.get('isCaptain')),
        # primary position: 'Coach'/'coach' for coaches — kept as-is, it's true
        'primary_position':      primary.get('label'),
        'primary_position_key':  primary.get('key'),
        'position_count':        len(pos_desc.get('positions') or []),
        # club
        'primary_team_id':   team.get('teamId'),
        'primary_team_name': team.get('teamName'),
        'on_loan':           team.get('onLoan'),
        # playerInformation — shirt/market value/contract end are absent for coaches
        'height_cm':        _num(info, 'height_sentencecase'),
        'shirt_number':     _num(info, 'shirt'),
        'age':              _num(info, 'age_sentencecase'),
        'preferred_foot':   _key_val(info, 'preferred_foot'),
        'country':          _fallback(info, 'country_sentencecase'),
        'country_code':     (info.get('country_sentencecase') or {}).get('countryCode'),
        'market_value_eur': _num(info, 'transfer_value'),
        'contract_end':     _date_only(data.get('contractEnd')),
        # provenance
        'seo_path':         meta.get('seopath'),
        'page_url':         meta.get('pageurl'),
    }


def _position_rows(data, player_id):
    """
    The granular positional breakdown. Empty list for coaches.
    `position` is Fotmob's numeric slot code (string in the payload).
    """
    pos_desc = data.get('positionDescription') or {}
    rows = []
    for p in pos_desc.get('positions') or []:
        str_pos = p.get('strPos') or {}
        short = p.get('strPosShort') or {}
        pitch = p.get('pitchPositionData') or {}
        rows.append({
            'fotmob_player_id': player_id,
            'position_key':     str_pos.get('key'),
            'position_label':   str_pos.get('label'),
            'position_short':   short.get('label'),
            # Fotmob spells this 'occurences'
            'occurrences':      p.get('occurences'),
            'position_code':    _int(p.get('position')),
            'is_main_position': bool(p.get('isMainPosition')),
            'pitch_right':      pitch.get('right'),
            'pitch_top':        pitch.get('top'),
            'pitch_ratio':      pitch.get('ratio'),
        })
    return rows


def _career_rows(data, player_id):
    """
    Senior career history: one row per club spell. Useful for transfer-aware
    analytics (when a player changed club, and what he did there).
    """
    ch = data.get('careerHistory') or {}
    items = (ch.get('careerItems') or {}).get('senior') or {}
    rows = []
    for e in items.get('teamEntries') or []:
        rows.append({
            'fotmob_player_id': player_id,
            'team_id':          e.get('teamId'),
            'team_name':        e.get('team'),
            'start_date':       (e.get('startDate') or '')[:10] or None,
            'end_date':         (e.get('endDate') or '')[:10] or None,
            'is_active':        bool(e.get('active')),
            'transfer_type':    e.get('transferType'),
            'role':             e.get('role'),
            'appearances':      _int(e.get('appearances')),
            'goals':            _int(e.get('goals')),
            'assists':          _int(e.get('assists')),
        })
    return rows


# ── entry point ──────────────────────────────────────────────────────────────
def parse_fotmob_player(data, player_id=None):
    """
    Parse one Fotmob player page.

    Args:
        data:      Parsed player-page JSON.
        player_id: Optional override; otherwise taken from the payload.

    Returns dict with 'player', 'positions', 'career'.
    """
    pid = data.get('id') or player_id
    if pid is None:
        raise ValueError('player page has no id')

    coach = is_coach(data)
    row = _player_row(data, coach)
    row['fotmob_player_id'] = pid

    positions = [] if coach else _position_rows(data, pid)
    if coach:
        log.info(f"  [{pid}] {row['player_name']}: coach — meta only, no positions")

    return {
        'player':    [row],
        'positions': positions,
        'career':    _career_rows(data, pid),
    }
