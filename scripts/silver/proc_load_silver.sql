USE CricketWarehouse;
GO

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	SET @start_time = GETDATE();
	BEGIN TRY
		PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '>> Truncating Table: silver.deliveries_updated_ipl_upto_2025';
		TRUNCATE TABLE silver.deliveries_updated_ipl_upto_2025;
		PRINT '>> Inserting Data Into: silver.deliveries_updated_ipl_upto_2025';

		WITH cte_fix_10th_ball AS (
		SELECT
			matchId AS match_id,
			[date] AS match_date,
			inning,
			[over] AS over_number,
			ball AS ball_in_over,
			ROW_NUMBER() OVER (PARTITION BY matchId, inning, [over], ball ORDER BY over_ball ASC) as ball_instance_rank,
			TRIM(batting_team) as batting_team,
			TRIM(bowling_team) as bowling_team,
			TRIM(batsman) as batsman,
			TRIM(non_striker) as non_striker,
			TRIM(bowler) as bowler,
			batsman_runs,
			extras,
			ISNULL(CAST(CAST(isWide AS FLOAT) AS INT),0) as wide_runs,
			ISNULL(CAST(CAST(isNoBall AS FLOAT) AS INT),0) as no_ball_runs,
			ISNULL(CAST(CAST(Byes AS FLOAT) AS INT),0) as byes,
			ISNULL(CAST(CAST(LegByes AS FLOAT) AS INT),0) as leg_byes,
			ISNULL(CAST(CAST(Penalty AS FLOAT) AS INT),0) as penalty_runs,
			TRIM(dismissal_kind) as dismissal_kind,
			TRIM(player_dismissed) as player_dismissed
		FROM bronze.deliveries_updated_ipl_upto_2025
		)

		INSERT INTO silver.deliveries_updated_ipl_upto_2025 (
		ball_id,           
		match_id,      
		[match_date],      
		inning,            
		over_ball,          
		[over_number],     
		ball_in_over,       
		batting_team,       
		bowling_team,       
		batter,             
		non_striker,       
		bowler,            
		batter_runs,        
		extras,             
		wide_runs,          
		no_ball_runs,       
		byes_runs,          
		leg_byes_runs,      
		penalty_runs,       
		dismissal_kind,
		player_dismissed
		)
		SELECT
			ROW_NUMBER() OVER (ORDER BY match_id, inning, [over_number], 
			CASE 
				WHEN ball_in_over = 1 AND ball_instance_rank = 2 
					THEN 10 
					ELSE ball_in_over 
			END ASC) ball_id,
			match_id,
			match_date,
			inning,
			CONCAT(
			CAST(over_number AS VARCHAR(10)), '.', CAST(CASE 
				WHEN ball_in_over = 1 AND ball_instance_rank = 2 THEN 10
				ELSE ball_in_over 
				END AS VARCHAR(10))
			)new_over_ball,
			over_number,
			CASE 
				WHEN ball_in_over = 1 AND ball_instance_rank = 2 
					THEN 10
					ELSE ball_in_over 
			END new_ball_in_over,
			batting_team,
			bowling_team,
			batsman,
			non_striker,
			bowler,
			batsman_runs,
			extras,
			wide_runs,
			no_ball_runs,
			byes,
			leg_byes,
			penalty_runs,
			dismissal_kind,
			player_dismissed
		FROM cte_fix_10th_ball
		ORDER BY 
			match_id, 
			inning, 
			over_number, 
			new_ball_in_over ASC;

		PRINT '>> Truncating Table: silver.matches_updated_ipl_upto_2025';
		TRUNCATE TABLE silver.matches_updated_ipl_upto_2025;
		PRINT '>> Inserting Data Into: silver.matches_updated_ipl_upto_2025';

		INSERT INTO silver.matches_updated_ipl_upto_2025 (
			match_id,
			season,
			match_number,
			[match_date],
			venue,
			city,
			first_team,
			second_team,
			outcome,
			toss_winner,
			toss_decision,
			match_winner,
			player_of_match,
			winner_runs,
			winner_wickets,
			umpire_1,
			umpire_2,
			reserve_umpire,
			tv_umpire,
			match_referee,
			dls_method,
			neutral_venue
		)
		SELECT
			matchId match_id,
			TRIM(season) season,
			ROW_NUMBER() OVER(PARTITION BY season ORDER BY matchID) match_number,
			[date] match_date,
			TRIM(venue) venue,
			TRIM(city) city,
			TRIM(team1) team1,
			TRIM(team2) team2,
			CASE
				WHEN outcome IS NULL
				THEN 'result'
				WHEN outcome = 'tie'
				THEN 'super over'
				ELSE TRIM(outcome)
			END outcome,
			TRIM(toss_winner) toss_winner,
			TRIM(toss_decision) toss_decision,
			CASE
				WHEN winner IS NULL AND eliminator IS NOT NULL
				THEN TRIM(eliminator)

				WHEN winner IS NULL AND outcome = 'no result'
				THEN TRIM(outcome)

				ELSE TRIM(winner)
			END winner,
			TRIM(player_of_match) player_of_match,
			winner_runs,
			winner_wickets,
			TRIM(umpire1) umpire1,
			TRIM(umpire2) umpire2,
			CASE
				WHEN reserve_umpire IS NULL
				THEN 'Unknown'
				ELSE TRIM(reserve_umpire)
			END reserve_umpire,
			TRIM(tv_umpire) tv_umpire,
			TRIM(match_referee) match_referee,
			CASE
				WHEN method IS NULL
				THEN 'No D/L'
				ELSE method
			END method,
			CASE
				WHEN neutralvenue IS NULL
				THEN 'No' -- Not Neutral Venue
				ELSE 'Yes'
			END neutralvenue
		FROM bronze.matches_updated_ipl_upto_2025


		SET @end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Silver Layer Loading completed!';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '======================================================';
	END CATCH
END
