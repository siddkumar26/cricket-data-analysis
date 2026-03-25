USE CricketWarehouse;
GO

/*
===============================================================================
Stored Procedure: Load Silver Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'gold' schema formatted as a Star Schema
    optimized for Power BI (VertiPaq Engine).
    - Populates descriptive dimensions (dim_player, dim_team, dim_venue).
    - Uses Integer IDs in Fact tables instead of Strings.
    - Calculates advanced contextual expected metrics (WAA, RAA) handling zero-division.
    - Uses Indexed Temp Tables for caching aggregations.
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    SET NOCOUNT ON
    DECLARE @start_time DATETIME, @end_time DATETIME
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Gold Layer (Star Schema Architecture)';
        PRINT '================================================';

        -- ==========================================
        -- 1. LOAD DESCRIPTIVE DIMENSIONS
        -- ==========================================
        PRINT '>> Loading: gold.dim_player';
        TRUNCATE TABLE gold.dim_player;
        INSERT INTO gold.dim_player (player_id, player_name)
        SELECT player_id, player_name FROM silver.players;

        PRINT '>> Loading: gold.dim_team';
        TRUNCATE TABLE gold.dim_team;
        INSERT INTO gold.dim_team (team_id, team_name)
        SELECT team_id, team_name FROM silver.teams;

        PRINT '>> Loading: gold.dim_venue';
        TRUNCATE TABLE gold.dim_venue;
        INSERT INTO gold.dim_venue (venue_id, venue_name, venue_city)
        SELECT venue_id, venue_name, venue_city FROM silver.venues;

        -- ==========================================
        -- 2. LOAD FACT TABLES
        -- ==========================================
        PRINT '>> Loading: gold.fact_ball_by_ball';
        TRUNCATE TABLE gold.fact_ball_by_ball;
        INSERT INTO gold.fact_ball_by_ball (
            delivery_id, match_id, inning_number, phase_of_match, overs, over_number,
            balls_bowled_this_over, bowling_team_id, bowler_id, batting_team_id, batter_id,
            non_striker_id, runs_off_bat, runs_from_extras, runs_from_wides, runs_from_no_balls,
            runs_from_byes, runs_from_leg_byes, runs_from_penalty, is_dot_ball, is_four, is_six,
            is_boundary, is_wicket_batter, is_wicket_bowler, is_bounce_back_ball, dismissal_type, player_dismissed_id,
            is_ball_faced, is_legal_delivery, bowler_runs_conceded
        )
        SELECT
            delivery_id, match_id, inning_number, phase_of_match, overs, over_number,
            balls_bowled_this_over, bowling_team_id, bowler_id, batting_team_id, batter_id,
            non_striker_id, runs_off_bat, runs_from_extras, runs_from_wides, runs_from_no_balls,
            runs_from_byes, runs_from_leg_byes, runs_from_penalty, is_dot_ball, is_four, is_six,
            is_boundary, is_wicket, 
            CASE 
                WHEN dismissal_type IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket', 'caught and bowled') 
                THEN 1 ELSE 0 
            END AS is_wicket_bowler,
            is_bounce_back_ball,
            CASE WHEN dismissal_type IS NULL THEN 'NOT OUT' ELSE dismissal_type END,
            player_dismissed_id,
            CASE WHEN ISNULL(runs_from_wides, 0) > 0 THEN 0 ELSE 1 END AS is_ball_faced,
            CASE WHEN ISNULL(runs_from_wides, 0) > 0 OR ISNULL(runs_from_no_balls, 0) > 0 THEN 0 ELSE 1 END AS is_legal_delivery,
            ISNULL(runs_off_bat, 0) + ISNULL(runs_from_wides, 0) + ISNULL(runs_from_no_balls, 0) AS bowler_runs_conceded

        FROM silver.deliveries;

        PRINT '>> Loading: gold.fact_matches';
        TRUNCATE TABLE gold.fact_matches;
        INSERT INTO gold.fact_matches (
            match_id, season_year, match_date, venue_id, team_1_id, team_2_id,
            toss_winner_id, toss_decision, match_winner_id, win_method, win_by_runs, win_by_wickets,
            team_1_score, team_2_score, team_1_super_over_score, team_1_super_over_wickets,
            team_2_super_over_score, team_2_super_over_wickets, player_of_match_id
        )
        SELECT
            match_id, season_year, match_date, venue_id, team_1_id, team_2_id,
            toss_winner_id, toss_decision, match_winner,
            CASE 
                WHEN winning_margin LIKE '%run%' THEN 'Batting First'
                WHEN winning_margin LIKE '%wicket%' THEN 'Chasing'
                WHEN winning_margin LIKE '%Tie%' OR winning_margin LIKE '%Super Over%' THEN 'Tie/Super Over'
                WHEN winning_margin LIKE '%No Result%' THEN 'No Result'
                ELSE 'Unknown' 
            END AS win_method,
            CASE WHEN winning_margin LIKE '%run%' AND CHARINDEX(' ', LTRIM(winning_margin)) > 0 
                THEN TRY_CAST(LEFT(LTRIM(winning_margin), CHARINDEX(' ', LTRIM(winning_margin)) - 1) AS INT) ELSE NULL END,
            CASE WHEN winning_margin LIKE '%wicket%' AND CHARINDEX(' ', LTRIM(winning_margin)) > 0 
                THEN TRY_CAST(LEFT(LTRIM(winning_margin), CHARINDEX(' ', LTRIM(winning_margin)) - 1) AS INT) ELSE NULL END,
            team_1_score, team_2_score, team_1_super_over_score, team_1_super_over_wickets,
            team_2_super_over_score, team_2_super_over_wickets, player_of_match
        FROM silver.matches;

        -- ==========================================
        -- 3. CREATE MASTER TEMP TABLE (#BaseOverStats)
        -- ==========================================
        PRINT '>> Generating Master Aggregations (#BaseOverStats)';
        IF OBJECT_ID('tempdb..#BaseOverStats') IS NOT NULL DROP TABLE #BaseOverStats;

        SELECT 
            m.season_year,
            f.over_number + 1 AS over_number,
            f.phase_of_match,
            f.batter_id, 
            f.bowler_id, 
            SUM(f.runs_off_bat) AS runs_off_bat,
            SUM(f.runs_off_bat + f.runs_from_extras) AS total_runs_scored,
            
            SUM(f.is_ball_faced) AS balls_faced,
            SUM(f.is_legal_delivery) AS legal_balls_bowled,
            SUM(f.bowler_runs_conceded) AS runs_conceded,
            
            SUM(CAST(f.is_wicket_batter AS INT)) AS wickets_batter,
            SUM(CAST(f.is_wicket_bowler AS INT)) AS wickets_bowler,
            SUM(CAST(f.is_four AS INT)) AS fours,
            SUM(CAST(f.is_six AS INT)) AS sixes,
            SUM(CAST(f.is_boundary AS INT)) AS boundaries,
            SUM(CAST(f.is_dot_ball AS INT)) AS dot_balls
        INTO #BaseOverStats
        FROM gold.fact_ball_by_ball f
        LEFT JOIN gold.fact_matches m ON m.match_id = f.match_id
        WHERE f.phase_of_match IS NOT NULL
        GROUP BY m.season_year, f.over_number, f.phase_of_match, f.batter_id, f.bowler_id;

        CREATE CLUSTERED INDEX CIX_BaseOverStats ON #BaseOverStats(season_year, over_number, phase_of_match);
        -- ==========================================
        -- 4. LOAD STATISTICAL BENCHMARKS & DIMENSIONS
        -- ==========================================
        PRINT '>> Loading: gold.dim_match_benchmarks_by_over';
        TRUNCATE TABLE gold.dim_match_benchmarks_by_over;

        WITH group_by_phases_seasons AS (
            SELECT
                season_year, over_number, phase_of_match,
                SUM(legal_balls_bowled) AS balls_bowled,
                CAST(SUM(legal_balls_bowled) / 6.0 AS DECIMAL(10, 2)) AS overs_bowled,
                SUM(runs_conceded) AS runs_scored,
                SUM(wickets_batter) AS wickets_lost_batter,
                SUM(wickets_bowler) AS wickets_taken_bowler,
                SUM(fours) AS fours_scored,
                SUM(sixes) AS sixes_scored,
                SUM(boundaries) AS boundaries_scored,
                SUM(dot_balls) AS dot_balls
            FROM #BaseOverStats 
            GROUP BY over_number, season_year, phase_of_match
        )
        INSERT INTO gold.dim_match_benchmarks_by_over (
            phase_count, season_year, over_number, phase_of_match, balls_bowled, runs_scored, wickets_lost_batter, wickets_taken_bowler,
            batting_average, batting_strike_rate, four_percentage, six_percentage, boundaries_percentage,
            bowling_strike_rate, economy_rate, dot_ball_percentage
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY season_year ASC, over_number ASC),
            season_year, over_number, phase_of_match, balls_bowled, runs_scored, wickets_lost_batter, wickets_taken_bowler,
            CAST(runs_scored / NULLIF(CAST(wickets_lost_batter AS FLOAT), 0) AS DECIMAL(10, 2)),
            CAST((runs_scored / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)),
            CAST((fours_scored / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)),
            CAST((sixes_scored / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)),
            CAST((boundaries_scored / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)),
            CAST(balls_bowled / NULLIF(CAST(wickets_taken_bowler AS FLOAT), 0) AS DECIMAL(10, 2)),
            CAST((runs_scored / NULLIF(CAST(overs_bowled AS FLOAT), 0)) AS DECIMAL(10, 2)),
            CAST((dot_balls / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2))
        FROM group_by_phases_seasons;

        PRINT '>> Loading: gold.dim_batter_statistics_by_over';
        TRUNCATE TABLE gold.dim_batter_statistics_by_over;

        WITH group_batter_stats AS (
            SELECT 
                batter_id, season_year, over_number, phase_of_match,
                SUM(runs_off_bat) AS runs_scored,
                SUM(balls_faced) AS balls_faced,
                SUM(wickets_batter) AS times_out,
                SUM(fours) AS fours_scored,
                SUM(sixes) AS sixes_scored,
                SUM(boundaries) AS boundaries_scored,
                SUM(dot_balls) AS dot_balls
            FROM #BaseOverStats
            GROUP BY batter_id, season_year, over_number, phase_of_match
        ),
        data_aggregation AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY batter_id ASC, over_number ASC) AS batter_over_idx,
                batter_id, season_year, over_number, phase_of_match, runs_scored, balls_faced, times_out,
                COALESCE(CAST((runs_scored / NULLIF(CAST(times_out AS FLOAT), 0)) AS DECIMAL(10, 2)), 
                CAST(runs_scored AS DECIMAL(10,2))) AS batting_average,
                CAST((runs_scored / NULLIF(CAST(balls_faced AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS batting_strike_rate,
                fours_scored,
                CAST((fours_scored / NULLIF(CAST(balls_faced AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS four_percentage,
                sixes_scored,
                CAST((sixes_scored / NULLIF(CAST(balls_faced AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS six_percentage,
                boundaries_scored,
                CAST((boundaries_scored / NULLIF(CAST(balls_faced AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS boundaries_percentage,
                dot_balls,
                CAST((dot_balls / NULLIF(CAST(balls_faced AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS dot_ball_percentage
            FROM group_batter_stats
        )
        INSERT INTO gold.dim_batter_statistics_by_over (
            batter_over_idx, batter_id, season_year, over_number, phase_of_match, runs_scored, balls_faced, fours_scored, 
            sixes_scored, boundaries_scored, boundaries_percentage, dot_balls, times_out, batting_average, 
            runs_above_average, batting_strike_rate, true_batting_strike_rate, true_dot_ball_percentage, true_boundaries_percentage
        )
        SELECT
            d.batter_over_idx, d.batter_id, d.season_year, d.over_number, d.phase_of_match, d.runs_scored, d.balls_faced, 
            d.fours_scored, d.sixes_scored, d.boundaries_scored, d.boundaries_percentage, d.dot_balls, d.times_out, d.batting_average,
            CAST(d.runs_scored - (d.balls_faced * (b.batting_strike_rate / 100.0)) AS DECIMAL(10,2)),
            d.batting_strike_rate,
            CAST(d.batting_strike_rate / NULLIF(b.batting_strike_rate, 0) AS DECIMAL(10,2)),
            CAST(d.dot_ball_percentage / NULLIF(b.dot_ball_percentage, 0) AS DECIMAL(10,2)),
            CAST(d.boundaries_percentage / NULLIF(b.boundaries_percentage, 0) AS DECIMAL(10,2))
        FROM data_aggregation d
        LEFT JOIN gold.dim_match_benchmarks_by_over b ON b.season_year = d.season_year AND b.over_number = d.over_number AND 
        b.phase_of_match = d.phase_of_match;

        PRINT '>> Loading: gold.dim_bowler_statistics_by_over';
        TRUNCATE TABLE gold.dim_bowler_statistics_by_over;

        WITH group_bowler_stats AS (
            SELECT 
                bowler_id, season_year, over_number, phase_of_match,
                CAST(SUM(legal_balls_bowled) / 6.0 AS DECIMAL(10, 2)) AS overs_bowled,
                SUM(runs_conceded) AS runs_conceded,
                SUM(legal_balls_bowled) AS balls_bowled,
                SUM(wickets_bowler) AS wickets_taken,
                SUM(fours) AS fours_conceded,
                SUM(sixes) AS sixes_conceded,
                SUM(boundaries) AS boundaries_conceded,
                SUM(dot_balls) AS dot_balls
            FROM #BaseOverStats
            GROUP BY bowler_id, season_year, over_number, phase_of_match
        ),
        data_aggregation AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY bowler_id ASC, over_number ASC) AS bowler_over_idx,
                bowler_id, season_year, over_number, phase_of_match, runs_conceded, balls_bowled, wickets_taken,
                CAST((runs_conceded / NULLIF(CAST(overs_bowled AS FLOAT), 0)) AS DECIMAL(10, 2)) AS economy_rate,
                COALESCE(CAST((runs_conceded / NULLIF(CAST(wickets_taken AS FLOAT), 0)) AS DECIMAL(10, 2)), 
                CAST(runs_conceded AS DECIMAL(10,2))) AS bowling_average,
                CAST((balls_bowled / NULLIF(CAST(wickets_taken AS FLOAT), 0)) AS DECIMAL(10, 2)) AS bowling_strike_rate,
                fours_conceded,
                CAST((fours_conceded / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS four_percentage,
                sixes_conceded,
                CAST((sixes_conceded / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS six_percentage,
                boundaries_conceded,
                CAST((boundaries_conceded / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS boundaries_percentage,
                dot_balls,
                CAST((dot_balls / NULLIF(CAST(balls_bowled AS FLOAT), 0)) * 100 AS DECIMAL(10, 2)) AS dot_ball_percentage
            FROM group_bowler_stats
        )
        INSERT INTO gold.dim_bowler_statistics_by_over (
            bowler_over_idx, bowler_id, season_year, over_number, phase_of_match, runs_conceded, balls_bowled, wickets_taken, dot_balls, 
            fours_conceded, sixes_conceded, boundaries_conceded, boundaries_percentage, bowling_average, bowling_strike_rate, economy_rate, 
            dot_ball_percentage, runs_saved_above_average, wickets_above_average, true_economy_rate, true_dot_ball_percentage, true_boundaries_percentage
        )
        SELECT
            d.bowler_over_idx, d.bowler_id, d.season_year, d.over_number, d.phase_of_match, d.runs_conceded, d.balls_bowled, 
            d.wickets_taken, d.dot_balls, d.fours_conceded, d.sixes_conceded, d.boundaries_conceded, d.boundaries_percentage, 
            d.bowling_average, d.bowling_strike_rate, d.economy_rate, d.dot_ball_percentage,
            CAST((d.balls_bowled * (b.economy_rate / 6.0)) - d.runs_conceded AS DECIMAL(10,2)),
            CAST(d.wickets_taken - (d.balls_bowled / NULLIF(b.bowling_strike_rate, 0)) AS DECIMAL(10,2)),
            CAST(d.economy_rate / NULLIF(b.economy_rate, 0) AS DECIMAL(10,2)),
            CAST(d.dot_ball_percentage / NULLIF(b.dot_ball_percentage, 0) AS DECIMAL(10,2)),
            CAST(d.boundaries_percentage / NULLIF(b.boundaries_percentage, 0) AS DECIMAL(10,2))
        FROM data_aggregation d
        LEFT JOIN gold.dim_match_benchmarks_by_over b ON b.season_year = d.season_year AND b.over_number = d.over_number AND 
        b.phase_of_match = d.phase_of_match;

        SET @end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Gold Layer Loading completed!';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '======================================================';
        PRINT 'ERROR OCCURRED DURING LOADING GOLD LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Line Number: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '======================================================';
    END CATCH
END
