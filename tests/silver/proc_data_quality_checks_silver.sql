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
    EXEC silver.data_quality_checks;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.data_quality_checks
AS
BEGIN
    BEGIN TRY
        
        -- ==============================================================================
        -- TABLE: silver.teams
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.teams';
        PRINT '------------------------------------------------';
        
        PRINT '>> Test TM-01: Verify composite primary key uniqueness.';
        IF EXISTS (SELECT team_id, team_name FROM silver.teams GROUP BY team_id, team_name HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test TM-01 Failed';
            RETURN;
        END

        PRINT '>> Test TM-02: Ensure completeness of team names.';
        IF EXISTS (SELECT 1 FROM silver.teams WHERE team_name IS NULL OR LTRIM(RTRIM(team_name)) = '')
        BEGIN
            PRINT '>> [FAIL] Test TM-02 Failed';
            RETURN;
        END

        PRINT '>> Test TM-03: Verify hardcoded mapping logic for franchises.';
        IF EXISTS (SELECT 1 FROM silver.teams WHERE team_name IN ('Delhi Capitals', 'Delhi Daredevils') AND team_id <> 2)
        BEGIN
            PRINT '>> [FAIL] Test TM-03 Failed';
            RETURN;
        END

        -- ==============================================================================
        -- TABLE: silver.players
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.players';
        PRINT '------------------------------------------------';

        PRINT '>> Test PL-01: Verify primary key uniqueness.';
        IF EXISTS (SELECT player_id FROM silver.players GROUP BY player_id HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test PL-01 Failed';
            RETURN;
        END

        PRINT '>> Test PL-02: Ensure completeness of extracted player names.';
        IF EXISTS (SELECT 1 FROM silver.players WHERE player_name IS NULL OR LTRIM(RTRIM(player_name)) = '')
        BEGIN
            PRINT '>> [FAIL] Test PL-02 Failed';
            RETURN;
        END

        -- ==============================================================================
        -- TABLE: silver.venues_lookup
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.venues_lookup';
        PRINT '------------------------------------------------';

        PRINT '>> Test VL-01: Verify primary key uniqueness.';
        IF EXISTS (SELECT lookup_id FROM silver.venues_lookup GROUP BY lookup_id HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test VL-01 Failed';
            RETURN;
        END

        PRINT '>> Test VL-02: Verify legacy stadium name standardization.';
        IF EXISTS (SELECT 1 FROM silver.venues_lookup WHERE venue_name LIKE '%Feroz Shah Kotla%' AND new_venue_name NOT LIKE '%Arun Jaitley%')
        BEGIN
            PRINT '>> [FAIL] Test VL-02 Failed';
            RETURN;
        END

        PRINT '>> Test VL-03: Verify deduplication ROW_NUMBER() logic.';
        IF EXISTS (SELECT venue_name FROM silver.venues_lookup GROUP BY venue_name HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test VL-03 Failed';
            RETURN;
        END

        -- ==============================================================================
        -- TABLE: silver.venues
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.venues';
        PRINT '------------------------------------------------';

        PRINT '>> Test VN-01: Verify primary key uniqueness.';
        IF EXISTS (SELECT venue_id FROM silver.venues GROUP BY venue_id HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test VN-01 Failed';
            RETURN;
        END

        PRINT '>> Test VN-02: Ensure no venues slipped through without a city assigned.';
        IF EXISTS (SELECT 1 FROM silver.venues WHERE venue_city IS NULL OR LTRIM(RTRIM(venue_city)) = '')
        BEGIN
            PRINT '>> [FAIL] Test VN-02 Failed';
            RETURN;
        END

        -- ==============================================================================
        -- TABLE: silver.matches
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.matches';
        PRINT '------------------------------------------------';

        PRINT '>> Test MA-01: Verify granularity / Primary Key uniqueness.';
        IF EXISTS (SELECT match_id FROM silver.matches GROUP BY match_id HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test MA-01 Failed';
            RETURN;
        END

        PRINT '>> Test MA-02: Reconciliation: Compare row count to Bronze layer.';
        IF (SELECT COUNT(*) FROM silver.matches) <> (SELECT COUNT(*) FROM bronze.matches_updated_ipl_upto_2025)
        BEGIN
            PRINT '>> [FAIL] Test MA-02 Failed';
            RETURN;
        END

        PRINT '>> Test MA-03: Verify foreign key relationships to dimensions.';
        IF EXISTS (
            SELECT 1 FROM silver.matches m 
            LEFT JOIN silver.venues v ON m.venue_id = v.venue_id
            LEFT JOIN silver.teams t1 ON m.team_1_id = t1.team_id
            LEFT JOIN silver.teams t2 ON m.team_2_id = t2.team_id
            WHERE (m.venue_id IS NOT NULL AND v.venue_id IS NULL)
               OR (m.team_1_id IS NOT NULL AND t1.team_id IS NULL)
               OR (m.team_2_id IS NOT NULL AND t2.team_id IS NULL)
        )
        BEGIN
            PRINT '>> [FAIL] Test MA-03 Failed';
            RETURN;
        END

        PRINT '>> Test MA-04: The Rain Rule: Validate match winner handling.';
        IF EXISTS (SELECT 1 FROM silver.matches WHERE winning_margin IS NULL AND match_winner <> 0 AND match_winner IS NOT NULL)
        BEGIN
            PRINT '>> [FAIL] Test MA-04 Failed';
            RETURN;
        END

        PRINT '>> Test MA-05: Verify concatenation logic.';
        IF EXISTS (
            SELECT 1 FROM silver.matches 
            WHERE winning_margin IS NOT NULL 
              AND winning_margin NOT LIKE '% runs' 
              AND winning_margin NOT LIKE '% wickets'
        )
        BEGIN
            PRINT '>> [FAIL] Test MA-05 Failed';
            RETURN;
        END

        PRINT '>> Test MA-06: Validate DLS boolean generation.';
        IF EXISTS (
            SELECT 1 FROM silver.matches s
            JOIN bronze.matches_updated_ipl_upto_2025 b ON s.match_id = b.matchId
            WHERE (b.method = 'D/L' AND s.is_dls_match = 0) OR (ISNULL(b.method, '') <> 'D/L' AND s.is_dls_match = 1)
        )
        BEGIN
            PRINT '>> [FAIL] Test MA-06 Failed';
            RETURN;
        END

        -- TO DO: Fix test failure
        PRINT '>> Test MA-07: Cross-Check: Verify match aggregate scores against deliveries.';
        IF EXISTS (
            SELECT 1 FROM silver.matches m
            JOIN (
                SELECT match_id, MAX(cumulative_team_runs) AS calculated_score
                FROM silver.deliveries
                WHERE inning_number = 1
                GROUP BY match_id
            ) d ON m.match_id = d.match_id
            WHERE m.team_1_score <> d.calculated_score
        )
        BEGIN
            PRINT '>> [FAIL] Test MA-07 Failed';
            RETURN;
        END
        

        PRINT '>> Test MA-08: Verify aggregate sum logic for Super Overs.';
        IF EXISTS (
            SELECT 1 FROM silver.matches m
            JOIN (
                SELECT match_id, batting_team_id, SUM(runs_off_bat + runs_from_extras) AS so_runs
                FROM silver.deliveries
                WHERE inning_number > 2
                GROUP BY match_id, batting_team_id
            ) d ON m.match_id = d.match_id AND m.team_1_id = d.batting_team_id
            WHERE m.team_1_super_over_score <> d.so_runs
        )
        BEGIN
            PRINT '>> [FAIL] Test MA-08 Failed';
            RETURN;
        END

        -- ==============================================================================
        -- TABLE: silver.deliveries
        -- ==============================================================================
        PRINT '------------------------------------------------';
        PRINT 'Performing tests in table silver.deliveries';
        PRINT '------------------------------------------------';

        PRINT '>> Test DE-01: Verify composite sorting generated a unique delivery key.';
        IF EXISTS (SELECT delivery_id FROM silver.deliveries GROUP BY delivery_id HAVING COUNT(*) > 1)
        BEGIN
            PRINT '>> [FAIL] Test DE-01 Failed';
            RETURN;
        END

        PRINT '>> Test DE-02: Reconciliation: Compare row count to Bronze layer.';
        IF (SELECT COUNT(*) FROM silver.deliveries) <> (SELECT COUNT(*) FROM bronze.deliveries_updated_ipl_upto_2025)
        BEGIN
            PRINT '>> [FAIL] Test DE-02 Failed';
            RETURN;
        END

        PRINT '>> Test DE-03: Verify linkage to Match parent table.';
        IF EXISTS (SELECT 1 FROM silver.deliveries d LEFT JOIN silver.matches m ON d.match_id = m.match_id WHERE m.match_id IS NULL)
        BEGIN
            PRINT '>> [FAIL] Test DE-03 Failed';
            RETURN;
        END

        PRINT '>> Test DE-04: Verify linkage to Player dimension.';
        IF EXISTS (
            SELECT 1 FROM silver.deliveries d 
            LEFT JOIN silver.players p1 ON d.batter_id = p1.player_id
            LEFT JOIN silver.players p2 ON d.bowler_id = p2.player_id
            WHERE (d.batter_id IS NOT NULL AND p1.player_id IS NULL)
               OR (d.bowler_id IS NOT NULL AND p2.player_id IS NULL)
        )
        BEGIN
            PRINT '>> [FAIL] Test DE-04 Failed';
            RETURN;
        END

        PRINT '>> Test DE-05: Validate boolean domain ranges.';
        IF EXISTS (
            SELECT 1 FROM silver.deliveries 
            WHERE is_dot_ball NOT IN (0,1) OR is_four NOT IN (0,1) 
               OR is_six NOT IN (0,1) OR is_boundary NOT IN (0,1) 
               OR is_wicket NOT IN (0,1) OR is_bounce_back_ball NOT IN (0,1)
        )
        BEGIN
            PRINT '>> [FAIL] Test DE-05 Failed';
            RETURN;
        END

        PRINT '>> Test DE-06: Validate cricket domain boundaries for overs.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE inning_number IN (1,2) AND (over_number < 0 OR over_number > 19))
        BEGIN
            PRINT '>> [FAIL] Test DE-06 Failed';
            RETURN;
        END

        PRINT '>> Test DE-07: Validate cricket domain boundaries for wickets.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE cumulative_wickets_lost < 0 OR cumulative_wickets_lost > 10)
        BEGIN
            PRINT '>> [FAIL] Test DE-07 Failed';
            RETURN;
        END

        PRINT '>> Test DE-08: Validate Cross-Column consistency for boundaries.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE is_boundary = 1 AND (is_four = 0 AND is_six = 0))
        BEGIN
            PRINT '>> [FAIL] Test DE-08 Failed';
            RETURN;
        END

        PRINT '>> Test DE-09: Validate batter''s perspective dot ball logic.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE is_dot_ball = 1 AND (runs_off_bat <> 0 OR runs_from_wides <> 0))
        BEGIN
            PRINT '>> [FAIL] Test DE-09 Failed';
            RETURN;
        END

        PRINT '>> Test DE-10: Validate previous-ball window function (LAG).';
        IF EXISTS (
            SELECT 1 FROM (
                SELECT is_bounce_back_ball, 
                       LAG(runs_off_bat + runs_from_extras) OVER(PARTITION BY match_id, inning_number ORDER BY delivery_id) as prev_ball_runs
                FROM silver.deliveries
            ) d 
            WHERE d.is_bounce_back_ball = 1 AND ISNULL(d.prev_ball_runs, 0) < 4
        )
        BEGIN
            PRINT '>> [FAIL] Test DE-10 Failed';
            RETURN;
        END

        PRINT '>> Test DE-11: Validate powerplay/middle/death bucketing.';
        IF EXISTS (
            SELECT 1 FROM silver.deliveries 
            WHERE (over_number < 6 AND phase_of_match <> 'powerplay')
               OR (over_number BETWEEN 6 AND 14 AND phase_of_match <> 'middle_overs')
               OR (over_number > 14 AND phase_of_match <> 'death_overs')
        )
        BEGIN
            PRINT '>> [FAIL] Test DE-11 Failed';
            RETURN;
        END

        PRINT '>> Test DE-12: Validate RRR constraints (NULLing rules).';
        IF EXISTS (
            SELECT 1 FROM silver.deliveries d
            LEFT JOIN silver.matches m ON d.match_id = m.match_id
            WHERE required_run_rate IS NOT NULL AND (d.inning_number = 1 OR m.is_dls_match = 1)
        )
        BEGIN
            PRINT '>> [FAIL] Test DE-12 Failed';
            RETURN;
        END

        PRINT '>> Test DE-13: Verify the "10th ball" anomaly fix logic.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE balls_bowled_this_over IS NULL OR balls_bowled_this_over < 1)
        BEGIN
            PRINT '>> [FAIL] Test DE-13 Failed';
            RETURN;
        END

        PRINT '>> Test DE-14: Validate batter_entry_score via FIRST_VALUE.';
        IF EXISTS (SELECT 1 FROM silver.deliveries WHERE batter_entry_score IS NULL OR batter_entry_score < 0)
        BEGIN
            PRINT '>> [FAIL] Test DE-14 Failed';
            RETURN;
        END

        PRINT '------------------------------------------------';
        PRINT 'ALL DATA QUALITY CHECKS PASSED SUCCESSFULLY';
        PRINT '------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT '======================================================';
        PRINT 'ERROR COMPLETING DATA QUALITY CHECKS';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '======================================================';
    END CATCH
END;
GO
