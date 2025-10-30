

with game_summary as (
    select * from {{ ref('stg_game_summary') }}
    where "#" IS NULL
),
adv_pass_types as (
    select * from {{ ref('stg_advanced_pass_types') }}
    where "#" IS NULL
),
adv_passing as (
    select * from {{ ref('stg_advanced_passing') }}
    where "#" IS NULL
),
adv_defending as (
    select * from {{ ref('stg_advanced_defending') }}
    where "#" IS NULL
),
adv_possession as (
    select * from {{ ref('stg_advanced_possession') }}
    where "#" IS NULL
),
misc_stats as (
    select * from {{ ref('stg_misc_stats') }}
    where "#" IS NULL
),
matxh as (
    select * from {{ ref('stg_match_details') }}
)

-- The main join logic starts here
select
    -- Identifiers
    matxh.*,


    -- summary stats (from stg_game_summary)
    coalesce(g_sum."performance_gls", 0) as goals_scored,
    coalesce(g_sum."performance_ast", 0) as assists,
    coalesce(g_sum."performance_pk", 0) as penalty_scored,
    coalesce(g_sum."performance_pkatt", 0) as penalty_attempts,
    coalesce(g_sum."performance_sh", 0) as total_shots, -- excludes penalty kicks
    coalesce(g_sum."performance_sot", 0) as shot_on_target, -- excludes penalty kicks
    coalesce(g_sum."performance_crdy", 0) as yellow_cards,
    coalesce(g_sum."performance_crdr", 0) as red_cards,
    coalesce(g_sum."performance_touches", 0) as touches,
    coalesce(g_sum."performance_tkl", 0) as tackles,
    coalesce(g_sum."performance_int", 0) as interceptions,
    coalesce(g_sum."performance_blocks", 0) as blocks, -- Number of times blocking the ball by standing in its path.
    coalesce(g_sum."expected_xg", 0) as xG,
    coalesce(g_sum."expected_npxg", 0) as non_penalty_xG,
    coalesce(g_sum."expected_xag", 0) as xA_Goals, -- Expected Assisted Goals. xG which follows a pass that assists a shot.
    coalesce(g_sum."sca_sca", 0) as SCA, -- shot creating actions
    coalesce(g_sum."sca_gca", 0) as GCA, -- goal creating actions
    coalesce(g_sum."passes_cmp", 0) as passes_completed, -- Passes Completed. Includes live ball passes, corner kicks, throw-ins, free kicks, and goal kicks.
    coalesce(g_sum."passes_att", 0) as passes_attempted, -- Passes Attempted. Includes live ball passes, corner kicks, throw-ins, free kicks, and goal kicks.
    coalesce(g_sum."passes_cmp%", 0) as pass_completion_pct,
    coalesce(g_sum."passes_prgp", 0) as progressive_passes, -- Progressive Passes. Completed passes that move the ball towards the opponent's goal line at least 10 yards.
    coalesce(g_sum."carries_carries", 0) as carries, -- Number of times the player controlled the ball with their feet.
    coalesce(g_sum."carries_prgc", 0) as progressive_carries, -- Progressive Carries. Carries that move the ball towards the opponent's goal line at least 10 yards.
    coalesce(g_sum."take-ons_att", 0) as attempted_take_ons,
    coalesce(g_sum."take-ons_succ", 0) as successful_take_ons,

    -- Advanced Passing Stats (from stg_advanced_passing)
    coalesce(ap."total_totdist", 0) as total_passing_distance, -- Yards
    coalesce(ap."total_prgdist", 0) as total_progressive_pass_dist, -- Progressive Passing Distance. Total distance, in yards, that completed passes have traveled towards the opponent's goal.
    coalesce(ap."short_cmp", 0) as completed_short_passes, -- between 5 and 15 Yards
    coalesce(ap."short_att", 0) as attempted_short_passes,
    coalesce(ap."short_cmp%", 0) as short_pass_completion_pct,
    coalesce(ap."medium_cmp", 0) as completed_medium_passes, -- 15 and 30 Yards
    coalesce(ap."medium_att", 0) as attempted_medium_passes,
    coalesce(ap."medium_cmp%", 0) as medium_pass_completion,
    coalesce(ap."long_cmp", 0) as completed_long_passes, -- longer than 30 Yards
    coalesce(ap."long_att", 0) as attempted_long_passes,
    coalesce(ap."long_cmp%", 0) as long_pass_completion,
    coalesce(ap."kp", 0) as key_passes,
    coalesce(ap."xa", 0) as xA, -- Expected Assits
    coalesce(ap."1/3", 0) as final_3rd_passes, -- Passes into Final Third. Completed passes that enter the 1/3 of the pitch closest to the goal.
    coalesce(ap."ppa", 0) as pass_into_penalty_area, -- Passes into Penalty Area. Completed passes into the 18-yard box.
    coalesce(ap."crspa", 0) as cross_into_penalty_area, -- Crosses into Penalty Area. Completed crosses into the 18-yard box.

    -- Advanced Pass Types Stats (from stg_advanced_pass_types)
    coalesce(ap_type."pass types_live", 0) as live_ball_passes,
    coalesce(ap_type."pass types_dead", 0) as dead_ball_passes,
    coalesce(ap_type."pass types_fk", 0) as pass_from_free_kick,
    coalesce(ap_type."pass types_tb", 0) as throguh_balls,
    coalesce(ap_type."pass types_sw", 0) as switches, --Switches. Passes that travel more than 40 yards of the width of the pitch.
    coalesce(ap_type."pass types_crs", 0) as crosses,
    coalesce(ap_type."pass types_ti", 0) as throw_ins,
    coalesce(ap_type."pass types_ck", 0) as corners,
    coalesce(ap_type."corner kicks_in", 0) as inswinging_corners,
    coalesce(ap_type."corner kicks_out", 0) as outswinging_corners,
    coalesce(ap_type."corner kicks_str", 0) as straight_corners,
    coalesce(ap_type."outcomes_off", 0) as passes_offside,
    coalesce(ap_type."outcomes_blocks", 0) as passes_blocked, -- Passes Blocked. Blocked by the opponent who was standing in the path.


    -- Advanced Possession Stats (from stg_advanced_possession)
    coalesce(aposs."touches_def pen", 0) as defensive_pen_touches,
    coalesce(aposs."touches_def 3rd", 0) as defensive_3rd_touches,
    coalesce(aposs."touches_mid 3rd", 0) as middle_3rd_touches,
    coalesce(aposs."touches_att 3rd", 0) as attack_3rd_touches,
    coalesce(aposs."touches_att pen", 0) as attack_pen_touches,
    coalesce(aposs."touches_live", 0) as live_ball_touches,
    coalesce(aposs."take-ons_succ%", 0) as take_on_success_pct, --Successful Take-On %. Percentage of Take-Ons Completed Successfully. 
    coalesce(aposs."take-ons_tkld", 0) as take_on_tackled, --Times Tackled During Take-On. Number of times tackled by a defender during a take-on attempt.
    coalesce(aposs."take-ons_tkld%", 0) as take_on_tackled_pct, --Tackled During Take-On Percentage. Percentage of time tackled by a defender during a take-on attempt.
    coalesce(aposs."carries_totdist", 0) as total_carry_distance, -- Total Carrying Distance. Total distance, in yards, a player moved the ball while controlling it with their feet.
    coalesce(aposs."carries_totdist", 0) as progressive_carry_distance, -- Progressive Carrying Distance. Total distance, in yards, a player moved the ball towards the opponent's goal.
    coalesce(aposs."carries_1/3", 0) as carries_into_final_3rd, -- Carries into Final Third.
    coalesce(aposs."carries_cpa", 0) as carries_into_18, -- Carries into Penalty Area. Carries into the 18-yard box.
    coalesce(aposs."carries_mis", 0) as miscontrols, -- Miscontrols. Number of times a player failed when attempting to gain control of a ball.
    coalesce(aposs."carries_dis", 0) as dispossessed, -- Dispossessed. Number of times a player loses control of the ball after being tackled.
    coalesce(aposs."receiving_rec", 0) as passes_received, --Passes Received. Number of times a player successfully received a pass.
    coalesce(aposs."receiving_prgr", 0) as progressive_passes_received, --Progressive Passes Received. Completed passes that move the ball towards the opponent's goal line at least 10 yards.

    -- Advanced Defending Stats (from stg_advanced_defending)
    coalesce(adef."tackles_tklw", 0) as tackles_won,
    coalesce(adef."tackles_def 3rd", 0) as tackles_defense_3rd,
    coalesce(adef."tackles_mid 3rd", 0) as tackles_mid_3rd,
    coalesce(adef."tackles_att 3rd", 0) as tackles_attack_3rd,
    coalesce(adef."challenges_tkl", 0) as challenges_successful, -- Dribblers Tackled. Number of dribblers tackled.
    coalesce(adef."challenges_att", 0) as challenges_attempted, -- Dribbles Challenged. Number of unsuccessful challenges plus number of dribblers tackled.
    coalesce(adef."challenges_tkl%", 0) as challenges_succ_pct,
    coalesce(adef."challenges_lost", 0) as challenges_lost,
    coalesce(adef."blocks_sh", 0) as blocked_shots, --Shots Blocked. Number of times blocking a shot by standing in its path.
    coalesce(adef."blocks_pass", 0) as blocked_passes, --Passes Blocked. Number of times blocking a pass by standing in its path.
    coalesce(adef."clr", 0) as clearances,
    coalesce(adef."err", 0) as errors_to_shot,


    -- Miscellaneuos Stats (from stg_misc_stats)
    coalesce(misc."performance_2crdy", 0) as second_yellow_cards,
    coalesce(misc."performance_fls", 0) as fouls_comitted,
    coalesce(misc."performance_fld", 0) as fouls_drawn,
    coalesce(misc."performance_off", 0) as caught_offside,
    coalesce(misc."performance_pkwon", 0) as penalty_won,
    coalesce(misc."performance_pkcon", 0) as penalty_conceded,
    coalesce(misc."performance_og", 0) as own_goals,
    coalesce(misc."performance_recov", 0) as recoveries,
    coalesce(misc."aerial duels_won", 0) as aerial_duels_won,
    coalesce(misc."aerial duels_lost", 0) as aerial_duels_lost,
    coalesce(misc."aerial duels_won%", 0) as aerial_duels_won_pct


from matxh
left join game_summary g_sum
    on matxh.combo_id = g_sum.combo_id
left join adv_passing ap
    on matxh.combo_id = ap.combo_id
left join adv_pass_types ap_type
    on matxh.combo_id = ap_type.combo_id
left join adv_possession aposs
    on matxh.combo_id = aposs.combo_id
left join adv_defending adef
    on matxh.combo_id = adef.combo_id
left join misc_stats misc
    on matxh.combo_id = misc.combo_id

