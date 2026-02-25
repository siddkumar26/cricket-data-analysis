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
    - Uses the `INSERT INTO` command to load data from bronze tables to silver tables.

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

		PRINT '>> Truncating Table: silver.teams';
		TRUNCATE TABLE silver.teams;
		PRINT '>> Inserting Data Into: silver.teams';

		INSERT INTO silver.teams (team_id, team_name)
		VALUES 
		-- == Team ID 1: Chennai Super Kings ==
		(1, 'Chennai Super Kings'),

		-- == Team ID 2: Delhi (Capitals + Daredevils) ==
		(2, 'Delhi Capitals'),
		(2, 'Delhi Daredevils'),

		-- == Team ID 3: Gujarat Titans ==
		(3, 'Gujarat Titans'),

		-- == Team ID 4: Kolkata Knight Riders ==
		(4, 'Kolkata Knight Riders'),

		-- == Team ID 5: Lucknow Super Giants ==
		(5, 'Lucknow Super Giants'),

		-- == Team ID 6: Mumbai Indians ==
		(6, 'Mumbai Indians'),

		-- == Team ID 7: Punjab (Kings + Kings XI) ==
		(7, 'Punjab Kings'),
		(7, 'Kings XI Punjab'),

		-- == Team ID 8: Rajasthan Royals ==
		(8, 'Rajasthan Royals'),

		-- == Team ID 9: RCB (Bengaluru + Bangalore) ==
		(9, 'Royal Challengers Bengaluru'),
		(9, 'Royal Challengers Bangalore'),

		-- == Team ID 10: Hyderabad (Sunrisers + Deccan Chargers) ==
		(10, 'Sunrisers Hyderabad'),
		(10, 'Deccan Chargers'),

		-- == Team ID 11: Pune (Supergiant + Supergiants) ==
		(11, 'Rising Pune Supergiant'),
		(11, 'Rising Pune Supergiants'),

		-- == Team ID 12: Gujarat Lions ==
		(12, 'Gujarat Lions'),

		-- == Team ID 13: Kochi ==
		(13, 'Kochi Tuskers Kerala'),

		-- == Team ID 14: Pune Warriors ==
		(14, 'Pune Warriors');

		PRINT '>> Truncating Table: silver.players';
		TRUNCATE TABLE silver.players;
		PRINT '>> Inserting Data Into: silver.players';

		WITH get_all_players AS (

		SELECT DISTINCT
			TRIM(batsman) AS player_name
		FROM bronze.deliveries_updated_ipl_upto_2025

		UNION

		SELECT DISTINCT
			TRIM(non_striker) AS player_name
		FROM bronze.deliveries_updated_ipl_upto_2025

		UNION

		SELECT DISTINCT
			TRIM(bowler) AS player_name
		FROM bronze.deliveries_updated_ipl_upto_2025
		)

		INSERT INTO silver.players (
			player_id,
			player_name
		)

		SELECT 
			ROW_NUMBER() OVER (ORDER BY player_name ASC) AS player_id,
			player_name
		FROM get_all_players;


		PRINT '>> Truncating Table: silver.venues_lookup';
		TRUNCATE TABLE silver.venues_lookup;
		PRINT '>> Inserting Data Into: silver.venues_lookup';

		WITH raw_venues AS (
			-- Step 1: Get distinct venue AND city pairs from source
			SELECT DISTINCT
				TRIM(LEFT(venue, CHARINDEX(',', venue + ',') - 1)) AS venue_name,
				city
			FROM bronze.matches_updated_ipl_upto_2025
			WHERE city IS NOT NULL
		),

		renamed_stadiums AS (
			SELECT
				venue_name,
				city,
				CASE
					WHEN venue_name LIKE '%Feroz Shah Kotla%' THEN 'Arun Jaitley Stadium'
					WHEN venue_name LIKE '%Sardar Patel Stadium%' OR venue_name = 'Motera Stadium' THEN 'Narendra Modi Stadium'
					WHEN venue_name LIKE '%Subrata Roy Sahara%' THEN 'Maharashtra Cricket Association Stadium'
					WHEN venue_name LIKE '%Punjab Cricket Association%' THEN 'Punjab Cricket Association IS Bindra Stadium'
					WHEN venue_name LIKE '%Vidarbha Cricket Association%' THEN 'Vidarbha Cricket Association Stadium'
					WHEN venue_name LIKE '%Chinnaswamy%' THEN 'M Chinnaswamy Stadium'
					WHEN venue_name LIKE '%DY Patil%' THEN 'Dr DY Patil Sports Academy'
					WHEN venue_name LIKE '%Zayed Cricket Stadium%' THEN 'Sheikh Zayed Stadium'
					WHEN venue_name LIKE '%Chidambaram%' THEN 'MA Chidambaram Stadium'
					ELSE venue_name
				END AS new_venue_name
			FROM raw_venues
		),

		fixed_cities AS (
			SELECT
				venue_name,
				new_venue_name,
				city AS venue_city,
				CASE
					WHEN new_venue_name LIKE '%DY Patil%' THEN 'Navi Mumbai'
					WHEN new_venue_name LIKE '%M%Chinnaswamy%' THEN 'Bengaluru'
					WHEN new_venue_name LIKE '%Punjab Cricket Association%' THEN 'Mohali'
					WHEN new_venue_name LIKE '%Maharaja Yadavindra%' THEN 'New Chandigarh'
					WHEN new_venue_name LIKE '%Saurashtra Cricket Association%' THEN 'Rajkot'
					WHEN city = 'Bangalore' THEN 'Bengaluru'
					ELSE TRIM(city)
				END AS new_venue_city
			FROM renamed_stadiums
		),

		deduplicated_venues AS (
			SELECT
				venue_name,
				new_venue_name,
				venue_city,
				new_venue_city,
				ROW_NUMBER() OVER (
					PARTITION BY venue_name 
					ORDER BY new_venue_city DESC
				) AS rn
			FROM fixed_cities
		)

		INSERT INTO silver.venues_lookup (lookup_id, venue_name, new_venue_name, venue_city, new_venue_city)
		SELECT
			ROW_NUMBER() OVER (ORDER BY new_venue_name) AS lookup_id,
			venue_name,
			new_venue_name,
			venue_city,
			new_venue_city
		FROM deduplicated_venues
		WHERE rn = 1;

		PRINT '>> Truncating Table: silver.venues';
		TRUNCATE TABLE silver.venues;
		PRINT '>> Inserting Data Into: silver.venues';
	
		INSERT INTO silver.venues (venue_id, venue_name, venue_city)
		
		SELECT 
			ROW_NUMBER() OVER (ORDER BY new_venue_name) AS venue_id,
			* 
		FROM (
		SELECT DISTINCT
			new_venue_name,
			new_venue_city
		FROM silver.venues_lookup
		)t

		PRINT '>> Truncating Table: silver.deliveries';
		TRUNCATE TABLE silver.deliveries;
		PRINT '>> Inserting Data Into: silver.deliveries';

		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY matchId, inning, [over], ball ORDER BY over_ball ASC) as ball_instance_rank
		INTO #fix_10th_ball
		FROM bronze.deliveries_updated_ipl_upto_2025;

		SELECT
			matchId,
			SUM(batsman_runs + extras) + 1 AS target_score
		INTO #first_innings_score
		FROM #fix_10th_ball
		WHERE inning = 1
		GROUP BY matchId;

		WITH join_player_teams_venues_matches_table AS (
		SELECT
		ROW_NUMBER() OVER (ORDER BY f.matchId, f.inning, f.[over], 
							CASE 
								WHEN f.ball = 1 AND f.ball_instance_rank = 2 
									THEN 10 
									ELSE f.ball 
							END ASC) delivery_id,
		f.[date] AS match_date,
		f.matchId as match_id,
		f.inning as inning_number,
		CONCAT(
			CAST(f.[over] AS VARCHAR(10)), '.', 
			CAST(CASE 
				WHEN f.ball = 1 AND ball_instance_rank = 2 THEN 10
				ELSE f.ball 
				END AS VARCHAR(10))
		) overs,
		f.[over] AS over_number,
		CASE 
			WHEN f.ball = 1 AND ball_instance_rank = 2 
				THEN 10
				ELSE f.ball 
		END balls_bowled_this_over,
		bowl_team.team_id AS bowling_team_id,
		bowler.player_id AS bowler_id,
		bat_team.team_id AS batting_team_id,
		batter.player_id AS batter_id,
		non_striker.player_id AS non_striker_id,
		f.batsman_runs AS runs_off_bat,
		f.extras AS runs_off_extras,
		ISNULL(CAST(CAST(f.isWide AS FLOAT) AS INT),0) AS runs_off_wides,
		ISNULL(CAST(CAST(f.isNoBall AS FLOAT) AS INT),0) AS runs_off_no_balls,
		ISNULL(CAST(CAST(f.Byes AS FLOAT) AS INT),0) AS runs_off_byes,
		ISNULL(CAST(CAST(f.LegByes AS FLOAT) AS INT),0) AS runs_off_leg_byes,
		ISNULL(CAST(CAST(f.Penalty AS FLOAT) AS INT),0)AS runs_off_penalty,
		first_innings.target_score AS target_score,
		f.dismissal_kind AS dismissal_type,
		dismissed.player_id AS player_dismissed,
		bronze_matches_table.method AS is_dls

		FROM #fix_10th_ball f

		LEFT JOIN silver.teams bowl_team
		ON bowl_team.team_name = f.bowling_team

		LEFT JOIN silver.teams bat_team
		ON bat_team.team_name = f.batting_team

		LEFT JOIN silver.players batter
		ON batter.player_name = f.batsman

		LEFT JOIN silver.players bowler
		ON bowler.player_name = f.bowler

		LEFT JOIN silver.players non_striker
		ON non_striker.player_name = f.non_striker

		LEFT JOIN silver.players dismissed
		ON dismissed.player_name = f.player_dismissed

		LEFT JOIN #first_innings_score first_innings
		ON f.matchId = first_innings.matchId

		LEFT JOIN bronze.matches_updated_ipl_upto_2025 bronze_matches_table
		ON bronze_matches_table.matchId = f.matchId
		),

		generate_flags_for_aggregation AS (

			SELECT 
			*,
			CASE 
				WHEN is_dls = 'D/L' THEN NULL
				WHEN over_number < 6 THEN 'powerplay'
				WHEN over_number BETWEEN 6 AND 14 THEN 'middle_overs'
				ELSE 'death_overs' 
			END AS phase_of_match,
			CASE WHEN is_dls IS NULL THEN 'NO' ELSE 'YES' END AS is_dls_match,
			CASE WHEN runs_off_bat = 0 AND runs_off_wides = 0 THEN 1 ELSE 0 END AS is_dot_ball,
			CASE WHEN runs_off_bat = 4 THEN 1 ELSE 0 END AS is_four_runs,
			CASE WHEN runs_off_bat = 6 THEN 1 ELSE 0 END AS is_six_runs,
			CASE WHEN runs_off_wides = 0 THEN 1 ELSE 0 END AS is_legal_ball_batter, 
			CASE WHEN runs_off_wides = 0 AND runs_off_no_balls = 0 THEN 1 ELSE 0 END AS is_legal_ball_bowler,
			CASE WHEN dismissal_type IS NOT NULL AND dismissal_type NOT IN ('retired hurt') THEN 1 ELSE 0 END AS is_wicket

			FROM join_player_teams_venues_matches_table

		),

		data_aggregation AS (
			SELECT 
				*,
				LAG(CASE WHEN runs_off_bat >= 4 THEN 1 ELSE 0 END, 1, 0) 
						OVER (PARTITION BY match_id, inning_number ORDER BY delivery_id) AS is_bounce_back_ball,        
				MIN(delivery_id) OVER (PARTITION BY match_id, inning_number, batter_id) AS batter_entry_delivery_id,
				SUM(runs_off_bat) OVER (PARTITION BY match_id, batter_id ORDER BY delivery_id) AS cumulative_batter_runs,
				SUM(is_legal_ball_batter) OVER (PARTITION BY match_id, batter_id ORDER BY delivery_id) AS cumulative_batter_balls_faced,
				SUM(is_legal_ball_bowler) OVER (PARTITION BY match_id, inning_number ORDER BY delivery_id) AS legal_balls_bowled,
				SUM(runs_off_bat + runs_off_extras) OVER (PARTITION BY match_id, inning_number ORDER BY delivery_id ASC) AS cumulative_team_runs,
				SUM(is_wicket) OVER (PARTITION BY match_id, inning_number ORDER BY delivery_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_wickets_lost,
				CASE WHEN is_four_runs + is_six_runs = 1 THEN 1 ELSE 0 END AS is_boundary
			FROM generate_flags_for_aggregation
		),

		cummulative_calculations AS (
			SELECT
				*,
				FIRST_VALUE(cumulative_wickets_lost - is_wicket) 
					OVER (PARTITION BY match_id, inning_number, batter_id ORDER BY delivery_id) AS batter_entry_wickets,
            
				FIRST_VALUE(cumulative_team_runs - (runs_off_bat + runs_off_extras)) 
					OVER (PARTITION BY match_id, inning_number, batter_id ORDER BY delivery_id) AS batter_entry_score,

				CASE 
					WHEN legal_balls_bowled = 0 THEN cumulative_team_runs
					ELSE CAST((cumulative_team_runs / (legal_balls_bowled / 6.0)) AS decimal(10, 2))
				END AS current_run_rate, 

				CASE 
					WHEN inning_number = 2 AND (120 - legal_balls_bowled) > 0 AND is_dls_match = 'NO' THEN 
						CAST( (target_score - cumulative_team_runs) / ((120 - legal_balls_bowled) / 6.0) AS decimal(10, 2))
					ELSE NULL 
				END AS required_run_rate,

				DENSE_RANK() OVER (PARTITION BY match_id, inning_number ORDER BY batter_entry_delivery_id ASC) AS batter_batting_position
			FROM data_aggregation
		)

		INSERT INTO silver.deliveries 
		(
			delivery_id,
			match_id,
			inning_number,
			phase_of_match,
			overs,
			over_number,
			balls_bowled_this_over,
			current_run_rate,
			required_run_rate,
			cumulative_team_runs,
			cumulative_wickets_lost,
			bowling_team_id,
			bowler_id,
			batting_team_id,
			batter_id,
			batting_position,
			batter_entry_score,
			batter_entry_wickets,
			non_striker_id,
			cumulative_batter_runs,
			cumulative_balls_faced,
			runs_off_bat,
			runs_from_extras,
			runs_from_wides,
			runs_from_no_balls,
			runs_from_byes,
			runs_from_leg_byes,
			runs_from_penalty,
			is_dot_ball,
			is_four,
			is_six,
			is_boundary,
			is_wicket,
			is_bounce_back_ball,
			dismissal_type,
			player_dismissed_id
		)

		SELECT 
			delivery_id,
			match_id,
			inning_number,
			phase_of_match,
			overs,
			over_number,
			balls_bowled_this_over,
			current_run_rate,
			required_run_rate,
			cumulative_team_runs,
			cumulative_wickets_lost,
			bowling_team_id,
			bowler_id,
			batting_team_id,
			batter_id,
			batter_batting_position,
			batter_entry_score,
			batter_entry_wickets,
			non_striker_id,
			cumulative_batter_runs,
			cumulative_batter_balls_faced,
			runs_off_bat,
			runs_off_extras,
			runs_off_wides,
			runs_off_no_balls,
			runs_off_byes,
			runs_off_leg_byes,
			runs_off_penalty,
			is_dot_ball,
			is_four_runs,
			is_six_runs,
			is_boundary,
			is_wicket,
			is_bounce_back_ball,
			dismissal_type,
			player_dismissed
		FROM cummulative_calculations

		PRINT '>> Truncating Table: silver.matches';
		TRUNCATE TABLE silver.matches;
		PRINT '>> Inserting Data Into: silver.matches';

		WITH match_aggregations AS (
			SELECT 
				m.matchId,
				MAX(CASE WHEN d.inning_number > 2 THEN 1 ELSE 0 END) AS is_super_over,
        
				SUM(CASE WHEN d.inning_number = 1 THEN d.runs_off_bat + d.runs_from_extras ELSE 0 END) AS team_1_score,
				SUM(CASE WHEN d.inning_number = 1 THEN d.is_wicket ELSE 0 END) AS team_1_wickets,
        
				SUM(CASE WHEN d.inning_number = 2 THEN d.runs_off_bat + d.runs_from_extras ELSE 0 END) AS team_2_score,
				SUM(CASE WHEN d.inning_number = 2 THEN d.is_wicket ELSE 0 END) AS team_2_wickets,
        
				SUM(CASE WHEN d.inning_number > 2 AND d.batting_team_id = t1.team_id THEN d.runs_off_bat + d.runs_from_extras ELSE 0 END) AS team_1_super_over_score,
				SUM(CASE WHEN d.inning_number > 2 AND d.batting_team_id = t1.team_id THEN d.is_wicket ELSE 0 END) AS team_1_super_over_wickets,
        
				SUM(CASE WHEN d.inning_number > 2 AND d.batting_team_id = t2.team_id THEN d.runs_off_bat + d.runs_from_extras ELSE 0 END) AS team_2_super_over_score,
				SUM(CASE WHEN d.inning_number > 2 AND d.batting_team_id = t2.team_id THEN d.is_wicket ELSE 0 END) AS team_2_super_over_wickets

			FROM bronze.matches_updated_ipl_upto_2025 m
			LEFT JOIN silver.teams t1 ON t1.team_name = m.team1
			LEFT JOIN silver.teams t2 ON t2.team_name = m.team2
			LEFT JOIN silver.deliveries d ON d.match_id = m.matchId
    
			GROUP BY 
				m.matchId, 
				t1.team_id, 
				t2.team_id
		)

		INSERT INTO silver.matches
		(
			match_number,
			match_id,
			season_year,
			match_date,
			venue_id,
			team_1_id,
			team_2_id,
			toss_winner_id,
			toss_decision,
			match_winner,
			winning_margin,
			player_of_match,
			is_dls_match,
			team_1_score,
			team_1_wickets,
			team_2_score,
			team_2_wickets,
			is_super_over,
			team_1_super_over_score,
			team_1_super_over_wickets,
			team_2_super_over_score,
			team_2_super_over_wickets
		)

		SELECT
			ROW_NUMBER() OVER(ORDER BY m.matchId) AS match_number,
			m.matchId AS match_id,
			m.season AS season_year,
			m.[date] AS match_date,
			v.venue_id AS venue_id,
			t1.team_id AS team_1_id,
			t2.team_id AS team_2_id,
			toss.team_id AS toss_winner,
			m.toss_decision,
			CASE WHEN m.winner IS NULL THEN 0 ELSE winner.team_id END AS match_winner,
			CASE
				WHEN m.winner_runs IS NULL AND m.winner_wickets IS NOT NULL THEN CAST(m.winner_wickets AS NVARCHAR(20)) + ' wickets'
				WHEN m.winner_wickets IS NULL AND m.winner_runs IS NOT NULL THEN CAST(m.winner_runs AS NVARCHAR(20)) + ' runs'
			END AS winning_margin,
			p.player_id AS player_of_match,
			CASE WHEN m.method IS NULL THEN 0 ELSE 1 END AS is_dls,
    
			agg.is_super_over,
			agg.team_1_score,
			agg.team_1_wickets,
			agg.team_2_score,
			agg.team_2_wickets,
			agg.team_1_super_over_score,
			agg.team_1_super_over_wickets,
			agg.team_2_super_over_score,
			agg.team_2_super_over_wickets

		FROM bronze.matches_updated_ipl_upto_2025 m
		LEFT JOIN silver.venues_lookup v_lookup ON TRIM(v_lookup.venue_name) = TRIM(LEFT(venue, CHARINDEX(',', m.venue + ',') - 1))
		LEFT JOIN silver.venues v ON TRIM(v_lookup.new_venue_name) = TRIM(v.venue_name)
		LEFT JOIN silver.teams t1 ON t1.team_name = m.team1
		LEFT JOIN silver.teams t2 ON t2.team_name = m.team2
		LEFT JOIN silver.teams toss ON toss.team_name = m.toss_winner
		LEFT JOIN silver.teams winner ON winner.team_name = m.winner
		LEFT JOIN silver.players p ON TRIM(p.player_name) = TRIM(m.player_of_match)
		LEFT JOIN match_aggregations agg ON agg.matchId = m.matchId;

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
