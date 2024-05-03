CREATE DATABASE GAMEANALYSIS;
USE GM;
-- Q1 
SELECT PD.P_ID,ID.DEV_ID,PD.PNAME,ID.DIFFICULTY
FROM PLAYER_DETAILS PD
INNER JOIN LEVEL_DETAILS2 ID ON PD.P_ID = ID.P_ID
WHERE ID.LEVEL=0;

-- Q2
SELECT PD.L1_CODE,AVG(LD.KILL_COUNT) AS AVG_KILL_COUNT
FROM PLAYER_DETAILS PD
INNER JOIN LEVEL_DETAILS2 LD ON PD.P_ID = LD.P_ID
WHERE LD.LIVES_EARNED=2 AND LD.STAGES_CROSSED >=3
GROUP BY PD.L1_CODE;

-- Q3 
SELECT LD.DIFFICULTY,SUM(LD.STAGES_CROSSED) AS TOTAL_STAGES_CROSSED
FROM LEVEL_DETAILS2 LD
INNER JOIN PLAYER_DETAILS PD ON LD.P_ID = PD.P_ID 
WHERE LD.LEVEL=2 AND LD.DEV_ID LIKE 'ZM_SERIES%'
GROUP BY LD.DIFFICULTY
ORDER BY TOTAL_STAGES_CROSSED DESC;

-- Q4
SELECT P_ID,COUNT(DISTINCT DATE(TIMESTAMP)) AS UNIQUE_DATE_COUNT
FROM LEVEL_DETAILS2
GROUP BY P_ID
HAVING COUNT(DISTINCT DATE(TIMESTAMP)) >1;

-- Q5
SELECT LD.P_ID,LD.LEVEL,SUM(LD.KILL_COUNT) AS TOTAL_KILL_COUNT
FROM LEVEL_DETAILS2 LD
INNER JOIN(
	SELECT AVG(KILL_COUNT) AS AVG_KILL_COUNT
    FROM LEVEL_DETAILS2
    WHERE DIFFICULTY = 'Medium' )
    AS AVG_TABLE ON LD.KILL_COUNT > AVG_TABLE.AVG_KILL_COUNT
GROUP BY LD.P_ID,LD.LEVEL;

-- Q6
SELECT LD.LEVEL,PD.L1_CODE,SUM(LD.LIVES_EARNED) AS TOTAL_LIVES_EARNED
FROM LEVEL_DETAILS2 LD
INNER JOIN PLAYER_DETAILS PD ON LD.P_ID = PD.P_ID
WHERE LD.LEVEL>0
GROUP BY LD.LEVEL,PD.L1_CODE
ORDER BY LD.LEVEL ASC;

-- Q7
SELECT 
    rs.Dev_ID,
    rs.P_ID,
    rs.score,
    rs.difficulty,
    rs.ScoreRank
FROM (
    SELECT 
        ld.P_ID,
        ld.Dev_ID,
        ld.score,
        ld.difficulty,
        @rn := IF(@prevDevID = ld.Dev_ID, @rn + 1, 1) AS ScoreRank,
        @prevDevID := ld.Dev_ID
    FROM
        (SELECT * FROM Level_Details2 ORDER BY Dev_ID, score DESC) ld,
        (SELECT @rn := 0, @prevDevID := '') AS vars
) rs
WHERE rs.ScoreRank <= 3
ORDER BY rs.Dev_ID, rs.ScoreRank;




-- Q8 
SELECT DEV_ID,MIN(TIMESTAMP) AS FIRST_LOGIN
FROM LEVEL_DETAILS2
GROUP BY DEV_ID;

-- Q9
SELECT 
    ld.Dev_ID,
    ld.P_ID,
    ld.score,
    ld.difficulty,
    (
        SELECT COUNT(*)
        FROM Level_Details2 ld2
        WHERE ld2.difficulty = ld.difficulty AND ld2.score >= ld.score
    ) AS ScoreRank
FROM Level_Details2 ld
WHERE (
    SELECT COUNT(*)
    FROM Level_Details2 ld2
    WHERE ld2.difficulty = ld.difficulty AND ld2.score >= ld.score
) <= 5
ORDER BY ld.difficulty, ScoreRank;






-- Q10
WITH FIRSTLOGIN AS(
SELECT P_ID,DEV_ID,MIN(timestamp) AS FIRST_LOGIN
FROM LEVEL_DETAILS2
GROUP BY P_ID,DEV_ID
)
SELECT FL.P_ID,FL.DEV_ID,FL.FIRST_LOGIN
FROM FIRSTLOGIN FL
INNER JOIN LEVEL_DETAILS2 LD ON FL.P_ID = LD.P_ID AND FL.FIRST_LOGIN=LD.TIMESTAMP;

-- Q11
SELECT P_ID,DATE(TIMESTAMP) AS DATE,SUM(KILL_COUNT)
OVER (PARTITION BY P_ID ORDER BY TIMESTAMP) AS TOTAL_KILL_COUNT
FROM LEVEL_DETAILS2;

-- Q11
SELECT P_ID,DATE(TIMESTAMP) AS DATE,SUM(KILL_COUNT) AS TOTAL_KILL_COUNT
FROM LEVEL_DETAILS2
GROUP BY P_ID,DATE(TIMESTAMP);

-- Q12
SELECT P_ID,TIMESTAMP,STAGES_CROSSED,
SUM(STAGES_CROSSED) OVER ( PARTITION BY P_ID
							ORDER BY timestamp
                            ROWS BETWEEN UNBOUNDED
                            PRECEDING AND 1 PRECEDING)
FROM LEVEL_DETAILS2;
select * from level_details2;
-- Q13
	WITH RankedStages AS (
    SELECT 
        P_ID,
        timestamp,
        stages_crossed,
        ROW_NUMBER() OVER (PARTITION BY P_ID ORDER BY timestamp DESC) AS rn
    FROM Level_Details2
)

SELECT 
    P_ID,
    timestamp,
    stages_crossed,
    SUM(stages_crossed) OVER (PARTITION BY P_ID ORDER BY timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cumulative_stages_crossed
FROM RankedStages
WHERE rn > 1;


-- Q14
 
SELECT P_ID
FROM (
		SELECT P_ID,SUM(SCORE) AS TOTAL_SCORE
        FROM LEVEL_DETAILS2
        GROUP BY P_ID
	) AS PLAYER_SCORES
    WHERE TOTAL_SCORE > 0.5 * (
    SELECT AVG(TOTAL_SCORE) FROM (
    SELECT SUM(SCORE) AS TOTAL_SCORE
    FROM LEVEL_DETAILS2
    GROUP BY P_ID
    ) AS AVG_SCORES
    );
    
    -- Q15 
DROP PROCEDURE IF EXISTS FindTopHeadshots;

DELIMITER //

CREATE PROCEDURE FindTopHeadshots(
    IN n INT -- Input parameter for number of top headshots to find
)
BEGIN
    -- Procedure body (as previously defined)
    DECLARE done INT DEFAULT FALSE;
    DECLARE dev_id_val INT;
    DECLARE headshots_val INT;
    DECLARE difficulty_val VARCHAR(255);
    DECLARE rank_val INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT DISTINCT Dev_ID
        FROM LevelDetails;

    DECLARE CONTINUE HANDLER FOR NOT FOUND
        SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS TopHeadshots (
        Dev_ID INT,
        difficulty VARCHAR(255),
        headshots_count INT,
        `Rank` INT
    );

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO dev_id_val;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET rank_val := 0;
        SET @prev_dev_id := NULL;

        INSERT INTO TopHeadshots (Dev_ID, difficulty, headshots_count, `Rank`)
        SELECT 
            ld.Dev_ID,
            ld.difficulty,
            ld.headshots_count,
            CASE
                WHEN @prev_dev_id = ld.Dev_ID THEN @rank := @rank + 1
                ELSE @rank := 1
            END AS `Rank`,
            @prev_dev_id := ld.Dev_ID AS prev_dev_id
        FROM (
            SELECT 
                Dev_ID,
                difficulty,
                headshots_count
            FROM LevelDetails
            WHERE Dev_ID = dev_id_val
            ORDER BY headshots_count DESC
            LIMIT n
        ) ld
        CROSS JOIN (SELECT @rank := 0) AS vars;
    END LOOP;

    CLOSE cur;

    SELECT *
    FROM TopHeadshots
    ORDER BY Dev_ID, `Rank`;

    DROP TEMPORARY TABLE IF EXISTS TopHeadshots;
    
END //

DELIMITER ;
call FindTopHeadshots(5); -- Find top 5 headshots_count for each Dev_ID

SHOW TABLES LIKE 'Level_Details2';
USE gm;


