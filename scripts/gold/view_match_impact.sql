USE CricketWarehouse;
GO

CREATE OR ALTER VIEW gold.vw_match_impact_leaderboard AS
WITH DeliveryContext AS (
    SELECT 
        f.match_id,
        m.season_year,
        f.batting_team_id,
        f.bowling_team_id,
        f.batter_id,
        f.bowler_id,
        f.over_number,
        f.phase_of_match,
        f.runs_off_bat,
        f.is_wicket_batter,
        f.is_wicket_bowler,
        f.is_ball_faced,           
        f.is_legal_delivery,       
        f.bowler_runs_conceded
    FROM gold.fact_ball_by_ball f
    JOIN gold.fact_matches m ON f.match_id = m.match_id
),
BattingImpact AS (
    SELECT 
        d.match_id,
        d.batter_id AS player_id,
        d.batting_team_id AS team_id,
        SUM(d.runs_off_bat) AS runs_scored,
        SUM(d.is_ball_faced) AS balls_faced, 
        SUM(d.runs_off_bat) - SUM(d.is_ball_faced * ISNULL((b.batting_strike_rate / 100.0), 0)) AS runs_above_average
    FROM DeliveryContext d
    LEFT JOIN gold.dim_match_benchmarks_by_over b 
        ON d.season_year = b.season_year 
        AND d.over_number = b.over_number 
        AND d.phase_of_match = b.phase_of_match
    GROUP BY d.match_id, d.batter_id, d.batting_team_id
),
BowlingImpact AS (
    SELECT 
        d.match_id,
        d.bowler_id AS player_id,
        d.bowling_team_id AS team_id,
        AVG(
        CASE 
            WHEN d.phase_of_match = 'Death' THEN 8.0
            WHEN d.phase_of_match = 'Middle' THEN 14.0
            ELSE 20.0
        END) AS avg_wicket_value,
        SUM(d.bowler_runs_conceded) AS runs_conceded, 
        SUM(CAST(d.is_wicket_bowler AS INT)) AS wickets_taken,
        SUM(d.is_legal_delivery) AS balls_bowled, 
        SUM(d.is_legal_delivery * ISNULL((b.economy_rate / 6.0), 0)) - SUM(d.bowler_runs_conceded) AS runs_saved_above_average
    FROM DeliveryContext d
    LEFT JOIN gold.dim_match_benchmarks_by_over b 
        ON d.season_year = b.season_year 
        AND d.over_number = b.over_number 
        AND d.phase_of_match = b.phase_of_match
    GROUP BY d.match_id, d.bowler_id, d.bowling_team_id
)
SELECT 
    COALESCE(bat.match_id, bowl.match_id) AS match_id,
    p.player_name,
    t.team_name,
    
    -- Batting Stats
    ISNULL(bat.runs_scored, 0) AS runs,
    ISNULL(bat.balls_faced, 0) AS balls_faced,
    CAST(ISNULL(bat.runs_scored, 0) + ISNULL(bat.runs_above_average, 0) AS DECIMAL(10,2)) AS batting_impact,
    
    -- Bowling Stats
    ISNULL(bowl.wickets_taken, 0) AS wickets,
    ISNULL(bowl.runs_conceded, 0) AS runs_conceded,
    
    CAST(
        (ISNULL(bowl.wickets_taken, 0) * ISNULL(bowl.avg_wicket_value, 14.0)) + 
        ISNULL(bowl.runs_saved_above_average, 0)
    AS DECIMAL(10,2)) AS bowling_impact,
    
    -- Total Match Impact (TI)
    CAST(
        (ISNULL(bat.runs_scored, 0) + ISNULL(bat.runs_above_average, 0)) + 
        (ISNULL(bowl.wickets_taken, 0) * ISNULL(bowl.avg_wicket_value, 14.0)) + 
        ISNULL(bowl.runs_saved_above_average, 0)
    AS DECIMAL(10,2)) AS total_impact

FROM BattingImpact bat
FULL OUTER JOIN BowlingImpact bowl 
    ON bat.match_id = bowl.match_id AND bat.player_id = bowl.player_id
LEFT JOIN gold.dim_player p ON p.player_id = COALESCE(bat.player_id, bowl.player_id)
LEFT JOIN gold.dim_team t ON t.team_id = COALESCE(bat.team_id, bowl.team_id);
