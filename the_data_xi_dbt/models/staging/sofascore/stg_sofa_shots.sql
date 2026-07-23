/* trivial tier (player-grain): passthrough + typed casts + player_uid, team_uid,
   team_row_id.
   team_uid is resolved from the SHOOTER's lineup team (not Sofa's `ishome`), so
   own goals attach to the scorer's team — matching WhoScored/Fotmob — keeping
   the cross-provider index aligned.
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
        px.player_uid,
        ln.team_uid
    from {{ source('raw','sofa_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
    left join {{ ref('stg_sofa_lineups') }} ln
           on ln.combo_id = s.combo_id and ln.player_uid = px.player_uid
)
-- timeseconds is now a real int, so this ordering is numeric (a lexical sort on
-- the old TEXT column scrambled it: '990' after '4159').
select *,
       row_number() over (partition by combo_id, team_uid
                          order by timeseconds, shot_id) as team_row_id
from base
