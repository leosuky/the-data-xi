/* trivial tier (player-grain): passthrough + typed expected_end_date + uids.
   NB expected_end_date carries the pandas 'NaT' sentinel → nulled before cast. */
select
    {{ passthrough_columns(source('raw','sofa_missing_players'), 's',
         exclude=['id','ingested_at','expected_end_date']) }},
    nullif(nullif(s.expected_end_date, ''), 'NaT')::date as expected_end_date,
    px.player_uid,
    tx.team_uid
from {{ source('raw','sofa_missing_players') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
