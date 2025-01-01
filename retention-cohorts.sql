WITH BASE AS (
  SELECT 
    event_ts_ist::DATE AS date, 
    user_id
  FROM 
    data_playground.ranjosh_leap_events
  WHERE 
    page_url = '/circle' 
    AND page_name = 'Circle Home' -- Circle home screen visitor 
  GROUP BY 
    1, 2
),
first_visit_dates AS (
  SELECT 
    user_id, 
    MIN(date) AS first_visit_date
  FROM 
    BASE
  GROUP BY 
    1
),
date_user_mapping AS (
  -- Generate all dates for each user starting from their first visit
  SELECT 
    dd.date AS calendar_date,
    fvd.user_id,
    fvd.first_visit_date
  FROM 
    analytical_dwh.dim_date dd
  CROSS JOIN 
    first_visit_dates fvd
  WHERE 
    dd.date >= fvd.first_visit_date -- Start from the user's first visit date
    AND dd.date <= GETDATE()::DATE -- Limit to current date
),
final_status AS (
  SELECT 
    dum.calendar_date,
    dum.user_id,
    CASE 
      WHEN b.date IS NOT NULL THEN 1 -- Active on this date
      ELSE 0 -- Not active
    END AS is_student_active,
    ROW_NUMBER() OVER (PARTITION BY dum.user_id ORDER BY dum.calendar_date) AS activity_order
  FROM 
    date_user_mapping dum
  LEFT JOIN 
    BASE b
    ON dum.calendar_date = b.date 
    AND dum.user_id = b.user_id
),
churn_logic AS (
  SELECT 
    fs.calendar_date,
    fs.user_id,
    fs.is_student_active,
    CASE
      WHEN fs.is_student_active = 1 THEN 0 -- Not churned when active
      WHEN LAG(fs.is_student_active) OVER (PARTITION BY fs.user_id ORDER BY fs.calendar_date) = 0 THEN 0 -- Not churned if previous day is already inactive
      ELSE 1 -- Churned if no activity and previous day was active
    END AS is_churned
  FROM 
    final_status fs
),
addition_logic AS (
  SELECT 
    cl.calendar_date,
    cl.user_id,
    cl.is_student_active,
    cl.is_churned,
    CASE
      -- Addition logic: Active today, but not active the previous day OR first day active
      WHEN cl.is_student_active = 1 AND (
           ROW_NUMBER() OVER (PARTITION BY cl.user_id ORDER BY cl.calendar_date) = 1 -- First activity
           OR LAG(cl.is_student_active) OVER (PARTITION BY cl.user_id ORDER BY cl.calendar_date) = 0 -- Activity after inactivity
      ) THEN 1
      ELSE 0
    END AS is_addition,
    CASE
      -- First time active ever
      WHEN cl.is_student_active = 1 
           AND ROW_NUMBER() OVER (PARTITION BY cl.user_id ORDER BY cl.calendar_date) = 1 THEN 1
      ELSE 0
    END AS is_gross_add,
    CASE
      -- Winback: Active after being churned (starting from first churn)
      WHEN cl.is_student_active = 1 
           AND MAX(cl.is_churned) OVER (PARTITION BY cl.user_id ORDER BY cl.calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) = 1 THEN 1
      ELSE 0
    END AS is_winback
  FROM 
    churn_logic cl
),
retention_data AS (
  -- Get the number of gross adds for each date
  SELECT 
    al.calendar_date AS first_date,
    al.user_id
  FROM 
    addition_logic al
  WHERE 
    al.is_gross_add = 1 -- Explicitly qualify is_gross_add
),
daily_retention AS (
  SELECT 
    r.first_date,
    r.user_id,
    d.calendar_date,
    CASE 
      WHEN d.is_student_active = 1 THEN 1
      ELSE 0
    END AS is_retained,
    DATEDIFF(day, r.first_date, d.calendar_date) AS days_after -- Fixing DATEDIFF syntax for Redshift
  FROM 
    retention_data r
  JOIN 
    addition_logic d
  ON 
    r.user_id = d.user_id
  WHERE 
    d.calendar_date >= r.first_date
),
aggregate_retention AS (
  SELECT 
    first_date,
    days_after,
    COUNT(DISTINCT user_id) AS students_retained
  FROM 
    daily_retention
  WHERE 
    is_retained = 1
  GROUP BY 
    first_date, days_after
),
total_students AS (
  SELECT 
    first_date,
    COUNT(DISTINCT user_id) AS students_added
  FROM 
    retention_data
  GROUP BY 
    first_date
)
SELECT 
  a.first_date,
  t.students_added,
  a.days_after,
  a.students_retained,
  CAST(a.students_retained AS FLOAT) / t.students_added AS perc_retained
FROM 
  aggregate_retention a
JOIN 
  total_students t
ON 
  a.first_date = t.first_date
WHERE a.first_date>='2024-12-22'
ORDER BY 
  a.first_date, a.days_after