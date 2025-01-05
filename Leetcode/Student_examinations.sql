-- Write a solution to find the number of times each student attended each exam.
-- Return the result table ordered by student_id and subject_name.
-- Leetcode link --- https://leetcode.com/problems/students-and-examinations/?envType=study-plan-v2&envId=top-sql-50 

-- Template of all students and all subjects [CROSS JOIN]
SELECT *
FROM students s
CROSS JOIN subjects sj
-- called cross join way will be better as some students haven't given exams but they have opted for all subjects


------ Final logic which beats 40%
WITH A AS (
SELECT s.student_id, student_name,sj.subject_name
, CASE WHEN e.subject_name IS NULL THEN 0
ELSE 1
END AS TAG
FROM students s
CROSS JOIN subjects sj
LEFT JOIN examinations e ON s.student_id=e.student_id AND sj.subject_name=e.subject_name
)
SELECT student_id, student_name,subject_name,SUM(tag) as attended_exams 
FROM A
GROUP BY 1,2,3
ORDER BY student_id,subject_name

------ KNEW THIS WAY WILL NOT WORK
-- SELECT s.student_id,student_name,subject_name,COUNT(*) as attended_exams FROM students s
-- LEFT JOIN examinations e
-- ON s.student_id=e.student_id
-- GROUP BY 1,2,3

------ CHAT GPT INSIGHTS ON MY QUERY
-- Here’s why your query stands out:

-- Use of CTEs: Using a CTE (WITH A AS (...)) is a great way to break down complex queries, making it easier to debug and understand. This is a good practice for readability and maintainability.

-- Logical Flow:

-- You correctly used a CROSS JOIN to pair all students with all subjects.
-- The LEFT JOIN ensures that students without an examination record still appear in the results.
-- The CASE statement is correctly used to determine whether an exam was attended.
-- Grouping and Ordering: You aggregated and grouped the data logically and sorted it for clarity, which is essential when presenting query results.
