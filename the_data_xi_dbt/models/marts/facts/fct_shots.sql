{{ config(materialized='table') }}
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

-- ---------------- WhoScored <-> Fotmob ----------------
cand_wf as (
    select w.combo_id, w.row_id as a, f.shot_id as b,
           abs(w.t - f.t)
             + case when w.is_goal is distinct from (f.event_type = 'Goal') then 3 else 0 end as score
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
        where not exists (select 1 from wf1 m where m.combo_id = c.combo_id and (m.a = c.a or m.b = c.b))) x
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
        where not exists (select 1 from fs1 m where m.combo_id = c.combo_id and (m.a = c.a or m.b = c.b))) x
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
)
select
    k.combo_id || '-' || k.team_uid || '-' || k.team_row_id as shot_uid,
    k.combo_id, k.team_uid, k.team_row_id,
    coalesce(w.player_uid, f.player_uid, so.player_uid) as player_uid,
    -- is_goal: WhoScored when present, else Fotmob's event_type, else Sofascore
    coalesce(w.is_goal, f.event_type = 'Goal', so.shottype = 'goal') as is_goal,
    -- own goal: Fotmob flag, else Sofascore goaltype (WhoScored shot rows carry none)
    coalesce(f.is_own_goal, so.goaltype = 'own', false) as is_own_goal,
    case when coalesce(f.is_own_goal, so.goaltype = 'own', false)
         then case when k.team_uid = dm.home_team_uid then dm.away_team_uid
                   else dm.home_team_uid end
         else k.team_uid
    end                                                 as credited_team_uid,
    {{ passthrough_columns(ref('stg_ws_shots'), 'w',
         exclude=['combo_id','row_id','team_row_id','player_uid','team_uid','is_goal',
                  'x','y','goal_mouth_y','goal_mouth_z','xgot','expanded_minute']) }},
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
    w.row_id as ws_row_id,
    f.shot_id as fot_shot_id,
    -- Sofascore mapping + flags (prefixed)
    so.shot_id as sofa_shot_id, so.xg as sofa_xg, so.xgot as sofa_xgot,
    so.ishome as sofa_is_home, so.goaltype as sofa_goaltype,
    dm.season_uid, dm.competition_uid, dm.match_date
from keyed k
left join ws   w  on w.combo_id  = k.combo_id and w.row_id   = k.ws_row_id
left join fot  f  on f.combo_id  = k.combo_id and f.shot_id  = k.fot_shot_id
left join sofa so on so.combo_id = k.combo_id and so.shot_id = k.sofa_shot_id
left join {{ ref('dim_match') }} dm on dm.combo_id = k.combo_id
