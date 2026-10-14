{{ config(materialized='table') }}
-- fct_player_physical: per-(player, match) physical tracking from Sofascore.
-- One row per player who was actually tracked — untracked players (goalkeepers
-- outside the PL/UCL, patchy outfield coverage in some leagues) are excluded
-- rather than shown as 0 km. Tracking exists from 2026/27 onwards only.
--
-- Bands (Sofascore splits more finely than Fotmob): running, high-speed running
-- and sprinting are separate; low_intensity_km is what's left of the total.
-- Fotmob's "running" figure = running_km + high_speed_running_km.
--
-- The physical columns only exist in raw once a tracked match has loaded, so
-- each is referenced defensively (null until then) — same pattern as
-- sofa_rating in fct_player_match.
{% set cols = adapter.get_columns_in_relation(ref('stg_sofa_player_stats'))
              | map(attribute='name') | list %}
with base as (
    select
        s.combo_id,
        s.player_uid,
        s.team_uid,
        s.is_home_team,
        {{ col_or_null(cols, 's', 'minutesplayed') }}                    as minutes_played,
        {{ col_or_null(cols, 's', 'kilometerscovered') }}                as distance_km,
        {{ col_or_null(cols, 's', 'meterscoveredrunningkm') }}           as running_km,
        {{ col_or_null(cols, 's', 'meterscoveredhighspeedrunningkm') }}  as high_speed_running_km,
        {{ col_or_null(cols, 's', 'meterscoveredsprintingkm') }}         as sprinting_km,
        {{ col_or_null(cols, 's', 'numberofsprints') }}                  as sprints,
        {{ col_or_null(cols, 's', 'topspeed') }}                         as top_speed_kmh
    from {{ ref('stg_sofa_player_stats') }} s
)
select
    b.combo_id, b.player_uid, b.team_uid, b.is_home_team,
    dm.season_uid, dm.competition_uid, dm.match_date,
    b.minutes_played,
    b.distance_km,
    b.running_km,
    b.high_speed_running_km,
    b.sprinting_km,
    b.distance_km - coalesce(b.running_km, 0)
                  - coalesce(b.high_speed_running_km, 0)
                  - coalesce(b.sprinting_km, 0)          as low_intensity_km,
    b.high_speed_running_km + b.sprinting_km             as high_intensity_km,
    b.sprints,
    b.top_speed_kmh,
    -- per-90 rates (minutes > 0 is guaranteed by the tracked filter in practice)
    round(90 * b.distance_km / nullif(b.minutes_played, 0), 3)                          as distance_km_p90,
    round(90 * (b.high_speed_running_km + b.sprinting_km) / nullif(b.minutes_played, 0), 3) as high_intensity_km_p90,
    round(90 * b.sprints / nullif(b.minutes_played, 0), 2)                              as sprints_p90
from base b
left join {{ ref('dim_match') }} dm on dm.combo_id = b.combo_id
where b.distance_km is not null
