/* trivial tier (player-grain): passthrough + typed casts + player_uid, team_uid,
   team_row_id.
   team_uid is resolved from the SHOOTER's lineup team (not Sofa's `ishome`), so
   own goals attach to the scorer's team — matching WhoScored/Fotmob — keeping
   the cross-provider index aligned.
   Fallbacks for a shooter missing from the crosswalk/lineups (~130 shots):
     player_uid = -sofa player_id (the crosswalk's own Sofascore-anchored rule);
     team_uid   = the side from `ishome` (flipped for an own goal, since ishome
                  is the side the goal counted for), via the match's teams.
   Without them the shot had no team or player and dropped out of team totals.
   Coordinate columns are JSON blobs ('{"x": 7.4, "y": 69.4, "z": 0}') → cast to
   jsonb and expose numeric x/y/z. */
with base as (
    select
        {{ passthrough_columns(source('raw','sofa_shots'), 's',
             exclude=['id','ingested_at','row_id','time','timeseconds',
                      'reversedperiodtime','reversedperiodtimeseconds','periodtimeseconds',
                      'playercoordinates','goalmouthcoordinates','blockcoordinates','draw']) }},
        nullif(s.row_id, '')::int                     as row_id,
        nullif(s.time, '')::int                       as time,
        nullif(s.timeseconds, '')::int                as timeseconds,
        nullif(s.reversedperiodtime, '')::int         as reversedperiodtime,
        nullif(s.reversedperiodtimeseconds, '')::int  as reversedperiodtimeseconds,
        nullif(s.periodtimeseconds, '')::int          as periodtimeseconds,
        nullif(s.draw, '')::jsonb                     as draw,
        nullif(s.playercoordinates, '')::jsonb        as playercoordinates,
        nullif(s.goalmouthcoordinates, '')::jsonb     as goalmouthcoordinates,
        nullif(s.blockcoordinates, '')::jsonb         as blockcoordinates,
        (nullif(s.playercoordinates, '')::jsonb ->> 'x')::numeric    as player_x,
        (nullif(s.playercoordinates, '')::jsonb ->> 'y')::numeric    as player_y,
        (nullif(s.goalmouthcoordinates, '')::jsonb ->> 'y')::numeric as goalmouth_y,
        (nullif(s.goalmouthcoordinates, '')::jsonb ->> 'z')::numeric as goalmouth_z,
        coalesce(px.player_uid, -s.player_id) as player_uid,
        coalesce(
            ln.team_uid,
            case when s.ishome is null then null
                 when s.ishome <> coalesce(s.goaltype = 'own', false)
                 then home_t.team_uid else away_t.team_uid end
        )                                       as team_uid
    from {{ source('raw','sofa_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
    left join {{ ref('stg_sofa_lineups') }} ln
           on ln.combo_id = s.combo_id and ln.player_uid = px.player_uid
    left join {{ ref('stg_sofa_matches') }} sm on sm.combo_id = s.combo_id
    left join {{ ref('int_team_xwalk') }}  home_t on home_t.sofa_team_id = sm.home_team_id
    left join {{ ref('int_team_xwalk') }}  away_t on away_t.sofa_team_id = sm.away_team_id
)
-- timeseconds is now a real int, so this ordering is numeric (a lexical sort on
-- the old TEXT column scrambled it: '990' after '4159').
select *,
       row_number() over (partition by combo_id, team_uid
                          order by timeseconds, shot_id) as team_row_id
from base
