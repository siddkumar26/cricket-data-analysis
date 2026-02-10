USE CricketWarehouse;
GO
/*
===============================================================================
Stored Procedure: Silver Layer Data Quality Checks
===============================================================================
Script Purpose:
    This script checks for any data quality issues present in the silver table
	that must be addressed before moving to the Gold layer

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.data_quality_checks AS

BEGIN
	DECLARE @ErrorMsg NVARCHAR(MAX);
	BEGIN TRY
		PRINT '================================================';
        PRINT 'Performing Silver Layer Data Quality Checks';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Performing tests in table silver.deliveries_updated_ipl_upto_2025';
		PRINT '------------------------------------------------';


		PRINT '>> Test 1.1: Checking if player_dismissed is NULL then dismissal_kind is NULL';
		-- If player_dismissed NULL then dismissal_kind NULL
		IF EXISTS (
		SELECT
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		WHERE player_dismissed IS NULL AND dismissal_kind IS NOT NULL
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.1 Failed';
			RETURN;
		END

		-- Check Strings
		PRINT '>> Test 1.2: Checking strings in table';

		IF EXISTS (
		SELECT
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		WHERE 
			UPPER(TRIM(batting_team)) != UPPER(batting_team) OR
			UPPER(TRIM(bowling_team)) != UPPER(bowling_team) OR
			UPPER(TRIM(batter)) != UPPER(batter) OR
			UPPER(TRIM(non_striker)) != UPPER(non_striker) OR
			UPPER(TRIM(dismissal_kind)) != UPPER(dismissal_kind) OR
			UPPER(TRIM(player_dismissed)) != UPPER(player_dismissed)
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.2 Failed';
			RETURN;
		END

		-- Check if Extras = Sum of Extras
		PRINT '>> Test 1.3: Checking if extras match sum of extras';
		IF EXISTS (
		SELECT
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		WHERE extras != wide_runs + no_ball_runs + byes_runs + leg_byes_runs + penalty_runs
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.3 Failed';
			RETURN;
		END

		-- Check if wide then no other extras
		PRINT '>> Test 1.4: Checking if ball is a wide then no other extras';

		IF EXISTS (
		SELECT 
			1
		FROM (
		SELECT
			wide_runs,
			no_ball_runs,
			byes_runs,
			leg_byes_runs
		FROM silver.deliveries_updated_ipl_upto_2025
		)t
		WHERE wide_runs > 0
		AND (no_ball_runs != 0 
			OR byes_runs != 0
			OR leg_byes_runs != 0)
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.4 Failed';
			RETURN;
		END

		-- Check if byes then no other extras
		PRINT '>> Test 1.5: Checking if ball was a bye then ball cannot be a LB or wide';
		IF EXISTS (
		SELECT 1 FROM (
		SELECT
			wide_runs,
			byes_runs,
			leg_byes_runs	
		FROM silver.deliveries_updated_ipl_upto_2025
		)t
		WHERE byes_runs > 0
		AND (wide_runs != 0
			OR leg_byes_runs != 0)
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.5 Failed';
			RETURN;
		END

		-- Check if leg byes then no other extras
		PRINT '>> Test 1.6: Checking if ball was a leg bye then ball cannot be a byes or wide';
		IF EXISTS (
		SELECT 1 FROM (
		SELECT
			wide_runs,
			byes_runs,
			leg_byes_runs
		FROM silver.deliveries_updated_ipl_upto_2025
		)t
		WHERE leg_byes_runs > 0
		AND (wide_runs != 0
			OR byes_runs != 0)
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.6 Failed';
			RETURN;
		END

		-- Check if no ball then cannot be wide
		PRINT '>> Test 1.7: Checking if ball is a no ball then ball cannot  be a wide';
		IF EXISTS (
		SELECT 1 FROM (
		SELECT
			wide_runs,
			no_ball_runs
		FROM silver.deliveries_updated_ipl_upto_2025
		)t
		WHERE no_ball_runs > 0
		AND (wide_runs != 0)
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.7 Failed';
			RETURN;
		END

		-- Check dates
		PRINT '>> Test 1.8: Checking if dates are within IPL range';
		IF EXISTS (
		SELECT
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		WHERE match_date < '2008-04-18' OR match_date > '2025-06-03'
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.8 Failed';
			RETURN;
		END


		-- Check for Orphan Records (Deliveries without a parent Match)
		PRINT '>> Test 1.9: Checking if match_id on this table exists in table matches_updated_ipl_upto_2025';
		IF EXISTS (
		SELECT 
			1
		FROM silver.deliveries_updated_ipl_upto_2025 d
		LEFT JOIN silver.matches_updated_ipl_upto_2025 m 
			ON d.match_id = m.match_id
		WHERE m.match_id IS NULL
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.9 Failed';
			RETURN;
		END 

		-- Check for impossible player combinations
		PRINT '>> Test 1.10: Checking if batter = non-striker or bowler and if non-striker = bowler';
		IF EXISTS (
		SELECT 
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		WHERE 
			batter = non_striker
			OR batter = bowler
			OR non_striker = bowler
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.10 Failed';
			RETURN;
		END

		-- Check for Logical Duplicates (Two records for the exact same ball)
		
		PRINT '>> Test 1.11: Checking if two balls are repeated';
		IF EXISTS (
		SELECT 
			1
		FROM silver.deliveries_updated_ipl_upto_2025
		GROUP BY match_id, inning, over_number, ball_in_over
		HAVING COUNT(*) > 1
		)
		BEGIN
			PRINT '>> [FAIL] Test 1.11 Failed';
			RETURN;
		END

		PRINT '------------------------------------------------';
		PRINT 'All tests in table silver.deliveries_updated_ipl_upto_2025 passed';
		PRINT '------------------------------------------------';

		PRINT '------------------------------------------------';
		PRINT 'Performing tests in table silver.matches_updated_ipl_upto_2025';
		PRINT '------------------------------------------------';

		-- Check if first_team is the same as the second_team
		PRINT '>> Test 2.1: Checking if first_team is the same as the second_team';
		IF EXISTS (
		SELECT
			1
		FROM silver.matches_updated_ipl_upto_2025
		WHERE first_team = second_team
		)
		BEGIN
			PRINT '>> [FAIL] Test 2.1 Failed';
			RETURN;
		END

		-- Check that toss winner is actually playing in the match
		PRINT '>> Test 2.2: Checking if toss_winner is playing the match';
		IF EXISTS (
		SELECT
			1
		FROM silver.matches_updated_ipl_upto_2025
		WHERE first_team != toss_winner AND second_team != toss_winner
		)
		BEGIN
			PRINT '>> [FAIL] Test 2.2 Failed';
			RETURN;
		END

		-- Check match winner is playing in match
		PRINT '>> Test 2.3: Checking if match_winner is playing the match';
		IF EXISTS (
		SELECT
			1
		FROM silver.matches_updated_ipl_upto_2025
		WHERE first_team != match_winner AND second_team != match_winner AND match_winner != 'no result'
		)
		BEGIN
			PRINT '>> [FAIL] Test 2.3 Failed';
			RETURN;
		END

		-- Check for Nulls
		PRINT '>> Test 2.4: Checking for NULLs in match_date, first_team, second_team, venue, toss_winner';
		IF EXISTS (
		SELECT
			1
		FROM silver.matches_updated_ipl_upto_2025
		WHERE match_date IS NULL OR first_team IS NULL OR second_team IS NULL OR venue IS NULL OR toss_winner IS NULL OR toss_decision IS NULL
		)
		BEGIN
			PRINT '>> [FAIL] Test 2.4 Failed';
			RETURN;
		END

		-- Both winner runs and winner wickets cannot contain a value
		PRINT '>> Test 2.5: Checking if both winner_runs and winner_wickets contain a value';
		IF EXISTS (
		SELECT
			1
		FROM silver.matches_updated_ipl_upto_2025
		WHERE winner_runs IS NOT NULL AND winner_wickets IS NOT NULL
		)
		BEGIN
			PRINT '>> [FAIL] Test 2.5 Failed';
			RETURN;
		END

		PRINT '------------------------------------------------';
		PRINT 'All tests in table silver.matches_updated_ipl_upto_2025';
		PRINT '------------------------------------------------';

		PRINT '================================================';
        PRINT 'All data quality tests passed!';
        PRINT '================================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR COMPLETING DATA QUALITY CHECKS';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '======================================================';
	END CATCH
END
