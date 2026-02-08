USE CricketWarehouse;
GO

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


-- Create Ball By Ball data table
IF OBJECT_ID('bronze.deliveries_updated_ipl_upto_2025', 'U') IS NOT NULL
    DROP TABLE bronze.deliveries_updated_ipl_upto_2025;
GO

CREATE TABLE bronze.deliveries_updated_ipl_upto_2025 (
    matchId INT,
    inning INT,
    over_ball FLOAT,
    [over] INT,
    ball INT,
    batting_team NVARCHAR(50),
    bowling_team NVARCHAR(50),
    batsman NVARCHAR(50),
    non_striker NVARCHAR(50),
    bowler NVARCHAR(50),
    batsman_runs INT,
    extras INT,
    isWide NVARCHAR(50),
    isNoBall NVARCHAR(50),
    Byes NVARCHAR(50),
    LegByes NVARCHAR(50),
    Penalty NVARCHAR(50),
    dismissal_kind NVARCHAR(50),
    player_dismissed NVARCHAR(50),
    [date] DATE
);

-- Create basic info data table
IF OBJECT_ID('bronze.matches_updated_ipl_upto_2025', 'U') IS NOT NULL
    DROP TABLE bronze.matches_updated_ipl_upto_2025;
GO
CREATE TABLE bronze.matches_updated_ipl_upto_2025 (
    season NVARCHAR(50),
    venue NVARCHAR(100),
    [event] NVARCHAR(50),
    winner_runs INT,
    umpire2 NVARCHAR(50),
    toss_winner NVARCHAR(50),
    [date] DATE,
    neutralvenue NVARCHAR(50),
    umpire1 NVARCHAR(50),
    city NVARCHAR(50),
    reserve_umpire NVARCHAR(50),
    winner NVARCHAR(50),
    eliminator NVARCHAR(50),
    date1 DATE,
    method NVARCHAR(50),
    team1 NVARCHAR(50),
    toss_decision NVARCHAR(50),
    gender NVARCHAR(50),
    team2 NVARCHAR(50),
    balls_per_over INT,
    winner_wickets INT,
    tv_umpire NVARCHAR(50),
    player_of_match NVARCHAR(50),
    match_referee NVARCHAR(50),
    outcome NVARCHAR(50),
    date2 DATE,
    match_number INT,
    matchId INT
);
