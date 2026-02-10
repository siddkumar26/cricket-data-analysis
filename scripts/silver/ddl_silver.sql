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
IF OBJECT_ID('silver.deliveries_updated_ipl_upto_2025', 'U') IS NOT NULL
    DROP TABLE silver.deliveries_updated_ipl_upto_2025;
GO

CREATE TABLE silver.deliveries_updated_ipl_upto_2025 (
    ball_id INT NOT NULL,
    match_id INT,
    [match_date] DATE,
    inning INT,
    over_ball NVARCHAR(10),
    [over_number] INT,
    ball_in_over INT,
    batting_team NVARCHAR(50),
    bowling_team NVARCHAR(50),
    batter NVARCHAR(50),
    non_striker NVARCHAR(50),
    bowler NVARCHAR(50),
    batter_runs INT,
    extras INT,
    wide_runs INT,
    no_ball_runs INT,
    byes_runs INT,
    leg_byes_runs INT,
    penalty_runs INT,
    dismissal_kind NVARCHAR(50),
    player_dismissed NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_Silver_Deliveries PRIMARY KEY CLUSTERED (ball_id)
);

-- Create basic info data table
IF OBJECT_ID('silver.matches_updated_ipl_upto_2025', 'U') IS NOT NULL
    DROP TABLE silver.matches_updated_ipl_upto_2025;
GO

-- Removing date1 and date2 as they hold no useful information
-- Removing Event as it is all IPL
-- Removing balls per over as its always 6
CREATE TABLE silver.matches_updated_ipl_upto_2025 (
    -- Primary Key
    match_id INT NOT NULL,

    -- Season Number
    season NVARCHAR(20),

    match_number INT,
    [match_date] DATE,
    venue NVARCHAR(100),
    city NVARCHAR(50),

    first_team NVARCHAR(50),
    second_team NVARCHAR(50),

    outcome NVARCHAR(50),
    toss_winner NVARCHAR(50),
    toss_decision NVARCHAR(50),
    match_winner NVARCHAR(50),
    player_of_match NVARCHAR(50),

    winner_runs INT,
    winner_wickets INT,

    umpire_1 NVARCHAR(50),
    umpire_2 NVARCHAR(50),
    reserve_umpire NVARCHAR(50),
    tv_umpire NVARCHAR(50),
    match_referee NVARCHAR(50),

    dls_method NVARCHAR(20),
    neutral_venue NVARCHAR(10),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),


    CONSTRAINT PK_Silver_Matches PRIMARY KEY CLUSTERED (match_id),
    CONSTRAINT CK_Neutral_Venue_Values CHECK (neutral_venue IN ('Yes', 'No')),
    CONSTRAINT CK_DLS_Method_Values CHECK (dls_method IN ('D/L', 'No D/L')),
    CONSTRAINT CK_Season_Format CHECK (season LIKE '[0-9][0-9][0-9][0-9]%' )
);
