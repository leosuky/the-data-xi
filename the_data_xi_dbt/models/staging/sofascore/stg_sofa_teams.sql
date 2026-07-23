/* trivial tier (flat): passthrough + typed casts + uids.
   NB latitude/longitude are SIGNED ('-0.1084') so they get explicit ::numeric —
   extract_leading_number would strip the minus sign. */
select
    {{ passthrough_columns(source('raw','sofa_teams'), 's',
         exclude=['id','ingested_at','latitude','longitude','founded_date']) }},
    nullif(s.latitude, '')::numeric                  as latitude,
    nullif(s.longitude, '')::numeric                 as longitude,
    nullif(nullif(s.founded_date, ''), 'NaT')::date  as founded_date,
    tx.team_uid
from {{ source('raw','sofa_teams') }} s
left join {{ ref('int_team_xwalk') }} tx on tx.sofa_team_id = s.team_id
