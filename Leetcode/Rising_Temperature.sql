-- Write your PostgreSQL query statement below
WITH A AS 
(
    SELECT *,LAG(temperature,1) OVER (ORDER BY recordDate ASC) as Lag_temp
    ,LAG(recordDate,1) OVER (ORDER BY recordDate ASC) as Lag_date
    FROM Weather
)
, B AS (
SELECT *, 
CASE WHEN recordDate-Lag_date=1 AND temperature>Lag_temp THEN 1
ELSE 0 
END as Tags
FROM A
)
SELECT id FROM B WHERE tags=1