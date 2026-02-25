USE CricketWarehouse;
GO

/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

-- Create Ball By Ball data table
IF OBJECT_ID('silver.deliveries', 'U') IS NOT NULL
    DROP TABLE silver.deliveries;
GO

CREATE TABLE silver.deliveries (
    delivery_id INT NOT NULL,
    match_id INT,
    inning_number INT,
    phase_of_match NVARCHAR(50),
    overs NVARCHAR(50),
    over_number INT,
    balls_bowled_this_over INT,
    current_run_rate FLOAT,
    required_run_rate FLOAT,
    cumulative_team_runs INT,
    cumulative_wickets_lost INT,
    bowling_team_id INT,
    bowler_id INT,
    batting_team_id INT,
    batter_id INT,
    batting_position INT,
    batter_entry_score INT,
    batter_entry_wickets INT,
    non_striker_id INT,
    cumulative_batter_runs INT,
    cumulative_balls_faced INT,
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
    is_wicket BIT,
    is_bounce_back_ball BIT,
    dismissal_type NVARCHAR(50),
    player_dismissed_id INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Deliveries PRIMARY KEY CLUSTERED (delivery_id)
);

-- Create basic info data table
IF OBJECT_ID('silver.matches', 'U') IS NOT NULL
    DROP TABLE silver.matches;
GO

CREATE TABLE silver.matches (
    match_number INT,
    match_id INT NOT NULL,
    season_year NVARCHAR(50),
    match_date DATE,
    venue_id INT,
    team_1_id INT,
    team_2_id INT,
    toss_winner_id INT,
    toss_decision NVARCHAR(50),
    match_winner INT,
    winning_margin NVARCHAR(50),
    player_of_match INT,
    is_dls_match BIT,
    team_1_score INT,
    team_1_wickets INT,
    team_2_score INT,
    team_2_wickets INT,
    is_super_over BIT,
    team_1_super_over_score INT,
    team_1_super_over_wickets INT,
    team_2_super_over_score INT,
    team_2_super_over_wickets INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Matches PRIMARY KEY CLUSTERED (match_id)
);

-- Create players table
IF OBJECT_ID('silver.players', 'U') IS NOT NULL
    DROP TABLE silver.players;
GO

CREATE TABLE silver.players (
    player_id INT NOT NULL,
    player_name NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Players PRIMARY KEY CLUSTERED (player_id)
);

-- Create Teams table
IF OBJECT_ID('silver.teams', 'U') IS NOT NULL
    DROP TABLE silver.teams;
GO

CREATE TABLE silver.teams (
    team_id INT NOT NULL,
    team_name NVARCHAR(50) NOT NULL,
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Teams PRIMARY KEY CLUSTERED (team_id, team_name)
);

-- Create Venues table
IF OBJECT_ID('silver.venues', 'U') IS NOT NULL
    DROP TABLE silver.venues;
GO

CREATE TABLE silver.venues (
    venue_id INT NOT NULL,
    venue_name NVARCHAR(100),
    venue_city NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Venues PRIMARY KEY CLUSTERED (venue_id)
);

-- Create Venues Lookup table
IF OBJECT_ID('silver.venues_lookup', 'U') IS NOT NULL
    DROP TABLE silver.venues_lookup;
GO

CREATE TABLE silver.venues_lookup (
    lookup_id INT NOT NULL,
    venue_name NVARCHAR(100),
    new_venue_name NVARCHAR(100),
    venue_city NVARCHAR(50),
    new_venue_city NVARCHAR(100),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Venues_Lookup PRIMARY KEY CLUSTERED (lookup_id)
);
