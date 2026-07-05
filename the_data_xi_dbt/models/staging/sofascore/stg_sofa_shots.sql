/* trivial tier (player-grain): 1:1 passthrough (introspected) + player_uid,
   team_uid, team_row_id.
   team_uid is resolved from the SHOOTER's lineup team (not Sofa's `ishome`),
   so own goals attach to the scorer's team — matching WhoScored/Fotmob — rather
   than Sofa's beneficiary convention, keeping the cross-provider index aligned. */
with base as (
    select
        {{ passthrough_columns(source('raw','sofa_shots'), 's', exclude=['id','ingested_at']) }},
        px.player_uid,
        ln.team_uid
    from {{ source('raw','sofa_shots') }} s
    left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
    left join {{ ref('stg_sofa_lineups') }} ln
           on ln.combo_id = s.combo_id and ln.player_uid = px.player_uid
)
-- Sofascore carries full-match timeseconds → exact ordering. NB: timeseconds is
-- stored as TEXT, so it MUST be cast to numeric — a lexical sort scrambles it
-- ('990' after '4159', '275' after '2601'), misaligning the cross-provider join.
select *,
       row_number() over (partition by combo_id, team_uid
                          order by timeseconds::numeric, shot_id::bigint) as team_row_id
from base
