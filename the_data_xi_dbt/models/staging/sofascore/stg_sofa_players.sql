/* trivial tier (player-grain): passthrough + typed birth_date + uids. */
select
    {{ passthrough_columns(source('raw','sofa_players'), 's',
         exclude=['id','ingested_at','birth_date']) }},
    nullif(nullif(s.birth_date, ''), 'NaT')::date as birth_date,
    px.player_uid
from {{ source('raw','sofa_players') }} s
left join {{ ref('int_player_xwalk') }} px on px.sofa_player_id = s.player_id
