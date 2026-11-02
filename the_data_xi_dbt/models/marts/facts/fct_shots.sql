{{ config(materialized='table', pre_hook="set local enable_nestloop = off") }}
-- enable_nestloop off (this model's transaction only): Postgres estimates the
-- candidate CTEs at ~1 row (really ~130k; it multiplies the match/team/player/
-- time-window selectivities as if independent) and then picks nested-loop anti
-- joins -> ~16 min on PG17. Every join here is keyed or per-match, so hash/merge
-- is always right. With it: ~17 s, identical output.
-- One row per shot. Providers are matched SHOT-BY-SHOT on content, not by
-- position: the old "Nth shot of a team" alignment broke whenever providers
-- logged different shots, or Fotmob's shot_id wasn't chronological (it can add
-- a late-reviewed shot with a much larger id), and glued different shots into
-- one row (~5% of rows, incl. goals).
--
-- Matching, per (match, team):
--   * Fotmob <-> Sofascore: identical xG (they usually carry the same model) is
--     preferred; otherwise the same player within 2.5 minutes (own goals have
--     no xG, and a few matches carry different xG per provider).
--   * WhoScored <-> Fotmob: same player within 2.5 minutes; a goal/no-goal
--     disagreement is penalised.
--   * WhoScored <-> Sofascore: only for shots Fotmob doesn't have.
--   A pair is kept only if each shot is the other's best candidate (two rounds),
--   so nothing is forced onto the wrong shot; leftovers stay single-provider rows.
-- Clock (minutes from kickoff, 2nd half starts at 45 for all three providers):
--   WhoScored minute + second/60 · Sofascore timeseconds/60 ·
--   Fotmob minute + added - 0.5 (Fotmob shows the display minute, no seconds).
--
-- Own goals stay on the OWN-GOALER's team (team_uid). For goals-by-team counts
-- use credited_team_uid (the team the goal counted for), never team_uid:
--   sum(is_goal::int) ... group by credited_team_uid
-- shot_uid / team_row_id are kept for compatibility: team_row_id is now the
-- shot's chronological position within its team for that match.
with ws as (
    select *, minute + coalesce(second, 0) / 60.0 as t
    from {{ ref('stg_ws_shots') }}
),
fot as (
    select *, minute + coalesce(minute_added, 0) - 0.5 as t
    from {{ ref('stg_fot_shots') }}
),
sofa as (
    select *, coalesce(timeseconds / 60.0, time + coalesce(addedtime, 0) - 0.5) as t
    from {{ ref('stg_sofa_shots') }}
),
-- WhoScored shot rows carry no own-goal flag, but its context events do.
-- Pair each player's goal SHOTS with his goal EVENTS (goal / penalty_goal /
-- own_goal) in chronological order: the k-th goal shot is the k-th goal event.
-- That handles a player scoring at both ends in one match. Only used when
-- neither Fotmob nor Sofascore has the shot (e.g. 2018/19 WhoScored-only matches).
ws_goal_events as (
    select combo_id, player_uid, event_type,
           row_number() over (partition by combo_id, player_uid order by time_s, minute, second) as rk
    from {{ ref('stg_ws_context_events') }}
    where event_type in ('goal', 'penalty_goal', 'own_goal')
),
ws_goal_shots as (
    select combo_id, row_id, player_uid,
           row_number() over (partition by combo_id, player_uid order by minute, second, row_id) as rk
    from {{ ref('stg_ws_shots') }}
    where is_goal_scored          -- own goals included: they pair with own_goal events
),
ws_own_goal as (
    select gs.combo_id, gs.row_id
    from ws_goal_shots gs
    join ws_goal_events ge using (combo_id, player_uid, rk)
    where ge.event_type = 'own_goal'
),

-- ---------------- WhoScored <-> Fotmob ----------------
cand_wf as (
    select w.combo_id, w.row_id as a, f.shot_id as b,
           abs(w.t - f.t)
             + case when w.is_goal_scored is distinct from (f.event_type = 'Goal') then 3 else 0 end as score
    from ws w
    join fot f on f.combo_id = w.combo_id and f.team_uid = w.team_uid
              and f.player_uid = w.player_uid and abs(w.t - f.t) <= 2.5
),
wf1 as (
    select combo_id, a, b from (
        select *, row_number() over (partition by combo_id, a order by score, b) as ra,
                  row_number() over (partition by combo_id, b order by score, a) as rb
        from cand_wf) x
    where ra = 1 and rb = 1
),
wf2 as (
    select combo_id, a, b from (
        select c.*, row_number() over (partition by combo_id, a order by score, b) as ra,
                    row_number() over (partition by combo_id, b order by score, a) as rb
        from cand_wf c
        where not exists (select 1 from wf1 m where m.combo_id = c.combo_id and m.a = c.a)
          and not exists (select 1 from wf1 m where m.combo_id = c.combo_id and m.b = c.b)) x
    where ra = 1 and rb = 1
),
m_wf as (select * from wf1 union all select * from wf2),

-- ---------------- Fotmob <-> Sofascore ----------------
cand_fs as (
    select f.combo_id, f.shot_id as a, s.shot_id as b,
           case when abs(f.xg - s.xg) < 0.0005 then 0 else 2 end
             + case when f.player_uid = s.player_uid then 0 else 3 end
             + abs(f.t - s.t) / 10.0 as score
    from fot f
    join sofa s on s.combo_id = f.combo_id and s.team_uid = f.team_uid
    where (f.xg is not null and s.xg is not null and abs(f.xg - s.xg) < 0.0005 and abs(f.t - s.t) <= 5)
       or (f.player_uid = s.player_uid and abs(f.t - s.t) <= 2.5)
),
fs1 as (
    select combo_id, a, b from (
        select *, row_number() over (partition by combo_id, a order by score, b) as ra,
                  row_number() over (partition by combo_id, b order by score, a) as rb
        from cand_fs) x
    where ra = 1 and rb = 1
),
fs2 as (
    select combo_id, a, b from (
        select c.*, row_number() over (partition by combo_id, a order by score, b) as ra,
                    row_number() over (partition by combo_id, b order by score, a) as rb
        from cand_fs c
        where not exists (select 1 from fs1 m where m.combo_id = c.combo_id and m.a = c.a)
          and not exists (select 1 from fs1 m where m.combo_id = c.combo_id and m.b = c.b)) x
    where ra = 1 and rb = 1
),
m_fs as (select * from fs1 union all select * from fs2),

-- ---------------- leftovers: WhoScored <-> Sofascore ----------------
ws_left as (
    select w.* from ws w
    where not exists (select 1 from m_wf m where m.combo_id = w.combo_id and m.a = w.row_id)
),
sofa_left as (
    select s.* from sofa s
    where not exists (select 1 from m_fs m where m.combo_id = s.combo_id and m.b = s.shot_id)
),
cand_ws as (
    select w.combo_id, w.row_id as a, s.shot_id as b, abs(w.t - s.t) as score
    from ws_left w
    join sofa_left s on s.combo_id = w.combo_id and s.team_uid = w.team_uid
                    and s.player_uid = w.player_uid and abs(w.t - s.t) <= 2
),
m_ws as (
    select combo_id, a, b from (
        select *, row_number() over (partition by combo_id, a order by score, b) as ra,
                  row_number() over (partition by combo_id, b order by score, a) as rb
        from cand_ws) x
    where ra = 1 and rb = 1
),

-- ---------------- spine: one row per real shot ----------------
spine as (
    -- every Fotmob shot, with its matched WhoScored / Sofascore shot (if any)
    select f.combo_id, mw.a as ws_row_id, f.shot_id as fot_shot_id, ms.b as sofa_shot_id
    from fot f
    left join m_wf mw on mw.combo_id = f.combo_id and mw.b = f.shot_id
    left join m_fs ms on ms.combo_id = f.combo_id and ms.a = f.shot_id
    union all
    -- WhoScored shots Fotmob doesn't have (+ Sofascore if it has them)
    select w.combo_id, w.row_id, null::bigint, mx.b
    from ws_left w
    left join m_ws mx on mx.combo_id = w.combo_id and mx.a = w.row_id
    union all
    -- Sofascore-only shots
    select s.combo_id, null::int, null::bigint, s.shot_id
    from sofa_left s
    where not exists (select 1 from m_ws mx where mx.combo_id = s.combo_id and mx.b = s.shot_id)
),
keyed as (
    select sp.*,
           coalesce(w.team_uid, f.team_uid, so.team_uid) as team_uid,
           row_number() over (
               partition by sp.combo_id, coalesce(w.team_uid, f.team_uid, so.team_uid)
               order by coalesce(w.t, so.t, f.t), sp.fot_shot_id, sp.sofa_shot_id, sp.ws_row_id
           ) as team_row_id
    from spine sp
    left join ws   w  on w.combo_id  = sp.combo_id and w.row_id   = sp.ws_row_id
    left join fot  f  on f.combo_id  = sp.combo_id and f.shot_id  = sp.fot_shot_id
    left join sofa so on so.combo_id = sp.combo_id and so.shot_id = sp.sofa_shot_id
),
shots as (
select
    k.combo_id || '-' || k.team_uid || '-' || k.team_row_id as shot_uid,
    k.combo_id, k.team_uid, k.team_row_id,
    coalesce(w.player_uid, f.player_uid, so.player_uid) as player_uid,
    -- is_goal: the ball went in (own goals included; use credited_team_uid).
    -- WhoScored when present, else Sofascore, else Fotmob's event_type
    coalesce(w.is_goal_scored, so.shottype = 'goal', f.event_type = 'Goal') as is_goal_any_provider,
    -- weight of the providers on this row that call it a goal (for dup_goal
    -- below): WhoScored 2, Sofascore 2, Fotmob 1, since Fotmob drifts on scorers
    2 * coalesce(w.is_goal_scored, false)::int + coalesce(f.event_type = 'Goal', false)::int
      + 2 * coalesce(so.shottype = 'goal', false)::int   as goal_claims,
    -- own goal: WhoScored (shot row or context events), else Sofascore goaltype,
    -- else Fotmob. WhoScored and Sofascore agree on the scorer; Fotmob drifts
    -- (it logs some goals as own goals the other two credit to the attacker).
    case when w.row_id is not null then og.row_id is not null or coalesce(w.is_own_goal, false)
                  when so.shot_id is not null then coalesce(so.goaltype = 'own', false)
                  else coalesce(f.is_own_goal, false) end as is_own_goal,
    case when case when w.row_id is not null then og.row_id is not null or coalesce(w.is_own_goal, false)
                       when so.shot_id is not null then coalesce(so.goaltype = 'own', false)
                       else coalesce(f.is_own_goal, false) end
         then case when k.team_uid = dm.home_team_uid then dm.away_team_uid
                   else dm.home_team_uid end
         else k.team_uid
    end                                                 as credited_team_uid,
    {{ passthrough_columns(ref('stg_ws_shots'), 'w',
         exclude=['combo_id','row_id','team_row_id','player_uid','team_uid','is_goal',
                  'is_own_goal','is_goal_scored','minute',
                  'x','y','goal_mouth_y','goal_mouth_z','xgot','expanded_minute']) }},
    -- minute: WhoScored's (elapsed minute, so 52 = the 53rd); for a shot only
    -- Fotmob or Sofascore has, their display minute (+ added time) - 1 on the
    -- same scale. match_time: minutes from kickoff with seconds where known.
    coalesce(w.minute,
             greatest(f.minute + coalesce(f.minute_added, 0) - 1, 0),
             greatest(so.time + coalesce(so.addedtime, 0) - 1, 0))  as minute,
    coalesce(w.t, so.t, f.t)                            as match_time,
    -- WhoScored coordinates (prefixed)
    w.x as ws_x, w.y as ws_y, w.goal_mouth_y as ws_goal_mouth_y, w.goal_mouth_z as ws_goal_mouth_z,
    -- Match timing from Fotmob
    f.minute as expanded_minute, f.minute_added,
    -- Fotmob xg + coordinates + extras
    f.xg, f.xgot,
    f.x as fot_x, f.y as fot_y,
    f.goal_crossed_y as fot_goal_crossed_y, f.goal_crossed_z as fot_goal_crossed_z,
    f.blocked_x as fot_blocked_x, f.blocked_y as fot_blocked_y,
    f.shot_type as fot_shot_type, f.situation as fot_situation, f.keeper_id as fot_keeper_id,
    f.event_type as fot_event_type, f.player_name as fot_player_name,
    f.is_blocked as fot_is_blocked, f.is_on_target as fot_is_on_target,
    f.is_own_goal as fot_is_own_goal,
    -- provenance: which source shot each provider contributed (for audits/tests)
    w.row_id as ws_row_id, w.player_uid as ws_player_uid,
    f.player_uid as fot_player_uid, so.player_uid as sofa_player_uid,
    f.shot_id as fot_shot_id,
    -- Sofascore mapping + flags (prefixed)
    so.shot_id as sofa_shot_id, so.xg as sofa_xg, so.xgot as sofa_xgot,
    so.ishome as sofa_is_home, so.goaltype as sofa_goaltype,
    dm.season_uid, dm.competition_uid, dm.match_date
from keyed k
left join ws   w  on w.combo_id  = k.combo_id and w.row_id   = k.ws_row_id
left join fot  f  on f.combo_id  = k.combo_id and f.shot_id  = k.fot_shot_id
left join sofa so on so.combo_id = k.combo_id and so.shot_id = k.sofa_shot_id
left join ws_own_goal og on og.combo_id = k.combo_id and og.row_id = k.ws_row_id
left join {{ ref('dim_match') }} dm on dm.combo_id = k.combo_id
),
-- One goal logged twice: providers disagree on who scored, e.g. Fotmob logs an
-- own goal while WhoScored/Sofascore credit the attacker's shot (which Fotmob
-- marks as a miss), so the two never match and the side gets an extra goal.
-- Only when the side's shot goals exceed its score: for two goal rows of that
-- side within 3 minutes, one an own goal and one not, the row with the lower
-- goal_claims weight (WhoScored 2, Sofascore 2, Fotmob 1) keeps its shot data
-- but stops counting (is_goal false, goal_disputed true). So a Fotmob-only
-- version always loses to a WhoScored or Sofascore one. A tie changes nothing.
side_goals as (
    select s.combo_id, s.credited_team_uid, count(*) as goals,
           max(case when s.credited_team_uid = dm.home_team_uid
                    then nullif(dm.home_score::text, '')::int
                    else nullif(dm.away_score::text, '')::int end) as score
    from shots s
    join {{ ref('dim_match') }} dm on dm.combo_id = s.combo_id
    where s.is_goal_any_provider
    group by s.combo_id, s.credited_team_uid
),
dup_goal as (
    select distinct
           case when a.goal_claims < b.goal_claims then a.shot_uid else b.shot_uid end as shot_uid
    from shots a
    join shots b on b.combo_id = a.combo_id
                and b.credited_team_uid = a.credited_team_uid
                and a.shot_uid < b.shot_uid
                and abs(a.match_time - b.match_time) <= 3
                and a.is_own_goal <> b.is_own_goal
    join side_goals g on g.combo_id = a.combo_id
                     and g.credited_team_uid = a.credited_team_uid
    where a.is_goal_any_provider and b.is_goal_any_provider
      and a.goal_claims <> b.goal_claims
      and g.goals > g.score
)
select
    s.is_goal_any_provider and d.shot_uid is null as is_goal,
    d.shot_uid is not null                         as goal_disputed,
    s.*
from shots s
left join dup_goal d on d.shot_uid = s.shot_uid
