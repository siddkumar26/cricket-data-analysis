USE CricketWarehouse;
GO

/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'gold' schema, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of 'gold' Tables
===============================================================================
*/

IF OBJECT_ID('gold.fact_ball_by_ball', 'U') IS NOT NULL
    DROP TABLE gold.fact_ball_by_ball;
GO

CREATE TABLE gold.fact_ball_by_ball (
    delivery_id INT NOT NULL,
    match_id INT,
    inning_number INT,
    phase_of_match NVARCHAR(50),
    overs NVARCHAR(50),
    over_number INT,
    balls_bowled_this_over INT,
    bowling_team_id INT,
    bowler_id INT,
    batting_team_id INT,
    batter_id INT,
    non_striker_id INT,
    runs_off_bat INT,
    runs_from_extras INT,
    runs_from_wides INT,
    runs_from_no_balls INT,
    runs_from_byes INT,
    runs_from_leg_byes INT,
    runs_from_penalty INT,
    is_dot_ball BIT,
    is_four BIT,
    is_six BIT,
    is_boundary BIT,
    is_wicket_batter BIT,
    is_wicket_bowler BIT,
    is_bounce_back_ball BIT,
    dismissal_type NVARCHAR(50),
    player_dismissed_id INT,
    is_ball_faced INT,
    is_legal_delivery INT,
    bowler_runs_conceded INT,
    CONSTRAINT PK_Gold_Fact_Ball_By_Ball PRIMARY KEY CLUSTERED (delivery_id)
);

IF OBJECT_ID('gold.fact_matches', 'U') IS NOT NULL
    DROP TABLE gold.fact_matches;
GO

CREATE TABLE gold.fact_matches (
    match_id INT,
    season_year NVARCHAR(50),
    match_date DATE,
    venue_id INT,
    team_1_id INT,
    team_2_id INT,
    toss_winner_id INT,
    toss_decision NVARCHAR(50),
    match_winner_id INT,        
    win_method NVARCHAR(50), -- Chasing or batting first
    win_by_runs INT,
    win_by_wickets INT,
    team_1_score INT,
    team_2_score INT,
    team_1_super_over_score INT,
    team_1_super_over_wickets INT,
    team_2_super_over_score INT,
    team_2_super_over_wickets INT,
    player_of_match_id INT,
    CONSTRAINT PK_Gold_Fact_Matches PRIMARY KEY CLUSTERED (match_id)
);

IF OBJECT_ID('gold.dim_player', 'U') IS NOT NULL
    DROP TABLE gold.dim_player;
GO

CREATE TABLE gold.dim_player (
    player_id INT,
    player_name VARCHAR(255),
    CONSTRAINT PK_Gold_Dim_Player PRIMARY KEY CLUSTERED (player_id)
);

IF OBJECT_ID('gold.dim_team', 'U') IS NOT NULL
    DROP TABLE gold.dim_team;
GO

CREATE TABLE gold.dim_team (
    team_id INT,
    team_name VARCHAR(255),
    CONSTRAINT PK_Gold_Dim_Team PRIMARY KEY CLUSTERED (team_id)
);

IF OBJECT_ID('gold.dim_venue', 'U') IS NOT NULL
    DROP TABLE gold.dim_venue;
GO

CREATE TABLE gold.dim_venue (
    venue_id INT,
    venue_name VARCHAR(255),
    venue_city VARCHAR(255),
    CONSTRAINT PK_Gold_Dim_Venue PRIMARY KEY CLUSTERED (venue_id)
);

IF OBJECT_ID('gold.dim_match_benchmarks_by_over', 'U') IS NOT NULL
    DROP TABLE gold.dim_match_benchmarks_by_over;
GO

CREATE TABLE gold.dim_match_benchmarks_by_over (
    phase_count INT,
    season_year NVARCHAR(50),
    over_number INT,
    phase_of_match NVARCHAR(50),
    balls_bowled INT,
    runs_scored INT,
    wickets_lost_batter INT,
    wickets_taken_bowler INT,
    batting_average DECIMAL(10,2),
    batting_strike_rate DECIMAL(10,2),
    four_percentage DECIMAL(10,2),
    six_percentage DECIMAL(10,2),
    boundaries_percentage DECIMAL(10,2),
    bowling_strike_rate DECIMAL(10,2),
    economy_rate DECIMAL(10,2),
    dot_ball_percentage DECIMAL(10,2),
    CONSTRAINT PK_Gold_Dim_Match_Benchmarks PRIMARY KEY CLUSTERED (phase_count)
);

IF OBJECT_ID('gold.dim_batter_statistics_by_over', 'U') IS NOT NULL
    DROP TABLE gold.dim_batter_statistics_by_over;
GO

CREATE TABLE gold.dim_batter_statistics_by_over (
    batter_over_idx INT,
    batter_id INT,
    season_year NVARCHAR(50),
    over_number INT,
    phase_of_match NVARCHAR(50),
    runs_scored INT,
    balls_faced INT,
    fours_scored INT,
    sixes_scored INT,
    boundaries_scored INT,
    boundaries_percentage DECIMAL(10,2),
    dot_balls INT,
    times_out INT,
    batting_average DECIMAL(10,2),
    runs_above_average DECIMAL(10,2),
    batting_strike_rate DECIMAL(10,2),
    true_batting_strike_rate DECIMAL(10,2),
    true_dot_ball_percentage DECIMAL(10,2),
    true_boundaries_percentage DECIMAL(10,2),
    CONSTRAINT PK_Gold_Dim_Batter_Stats PRIMARY KEY CLUSTERED (batter_over_idx)
);

IF OBJECT_ID('gold.dim_bowler_statistics_by_over', 'U') IS NOT NULL
    DROP TABLE gold.dim_bowler_statistics_by_over;
GO

CREATE TABLE gold.dim_bowler_statistics_by_over (
    bowler_over_idx INT,
    bowler_id INT,
    season_year NVARCHAR(50),
    over_number INT,
    phase_of_match NVARCHAR(50),
    runs_conceded INT,
    balls_bowled INT,
    wickets_taken INT,
    dot_balls INT,
    fours_conceded INT,
    sixes_conceded INT,
    boundaries_conceded INT,
    boundaries_percentage DECIMAL(10,2),
    bowling_average DECIMAL(10,2),
    bowling_strike_rate DECIMAL(10,2),   
    economy_rate DECIMAL(10,2),
    dot_ball_percentage DECIMAL(10,2),
    runs_saved_above_average DECIMAL(10,2),
    wickets_above_average DECIMAL(10,2),
    true_economy_rate DECIMAL(10,2),
    true_dot_ball_percentage DECIMAL(10,2),
    true_boundaries_percentage DECIMAL(10,2),
    CONSTRAINT PK_Gold_Dim_Bowler_Stats PRIMARY KEY CLUSTERED (bowler_over_idx)
);
