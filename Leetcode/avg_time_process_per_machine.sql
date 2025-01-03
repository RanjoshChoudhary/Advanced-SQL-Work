# Write your MySQL query statement below
WITH A AS 
(
SELECT CONCAT(machine_id,process_id) AS C,machine_id,activity_type,timestamp FROM Activity
WHERE activity_type='start'
)
, B AS 
(
  SELECT CONCAT(machine_id,process_id) AS C,machine_id,activity_type,timestamp FROM Activity  
    WHERE activity_type='end'
)
,D as (
SELECT 
A.machine_id,ROUND((B.timestamp-A.timestamp),3) as processing_time 
FROM A 
LEFT JOIN B ON A.C=B.C
)
SELECT machine_id,AVG(processing_time) AS processing_time FROM D
GROUP BY 1