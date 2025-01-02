----------------------------------------------- Initial script -----------------------------------------------
with data AS (
SELECT A.payment_id
, A.amount
, A.currency_conversion_rate
, B.meta.credits_data.applied_credits::decimal  
, A.currency_conversion_rate::float* B.meta.credits_data.applied_credits::float  
, B.amount
, A.no_classes
, A.enrollment_type_new
, B.amount/ a.no_classes AS currency_Classperrate
, A.paid_on
, A.amount::decimal/ a.no_classes  AS INR_Classperrate

FROM data_models.payment AS A
LEFT JOIN application_service_intelenrollment.student_payment_link AS B ON A.payment_id = B.id
where A.student_id = '0da06a78-fe3a-11ea-9fea-02420a00ff63'
  )



SELECT A.id, A.student_id ,B.stream, C.meta.student_course_fee_id::Text, C.teacher_id, C.class_taken_on, C.id AS class_log_id, COALESCE( D.meta.student_payment_link_id::tEXT, D.meta.ccw_payment_link_id::TEXT) as Payment_id
, E.currency_Classperrate
, E.INR_Classperrate
, F.amount

, CASE WHEN f.meta.no_show_payout = True THEN f.amount_pre_tax*4 ELSE amount_pre_tax END AS CLass_payout_og
, D.enrollment_type
FROM application_service_intelenrollment.student_course AS A
LEFT JOIN application_service_intelenrollment.course AS B ON A.course_id= B.id
LEFT JOIN application_service_intelenrollment.student_course_class_log AS C ON A.id = C.student_course_id
LEFT JOIN application_service_intelenrollment.student_course_fee AS D ON  C.meta.student_course_fee_id::Text = D.id 
LEFT JOIN data E ON  COALESCE( D.meta.student_payment_link_id::tEXT, D.meta.ccw_payment_link_id::TEXT) = E.payment_id
LEFT JOIN application_service_payout.class_payout AS F ON C.id  =F.class_log_id
where A.student_id = '0da06a78-fe3a-11ea-9fea-02420a00ff63'
and A.access_type = 'WITH_TEACHER'
and C.class_taken_on>='2024-04-01'::Date
--------------------------------------------END OF 1 --------------------------------------------

-------------------------------------------- Start of 2--------------------------------------------
with 
---------------------------------- Students whose class transfered to HIGH 
X as
(
SELECT 
A.student_id , A.teacher_id,A.grade
,A.payment_id
, A.paid_on
, B.invoice_id
, A.no_classes
, A.enrollment_type_new
FROM data_models.payment AS A
LEFT JOIN application_service_intelenrollment.student_payment_link AS B ON A.payment_id = B.id

WHERE 0=0
--   AND A.student_id = 'a46cd6ba-afb9-11ea-abcd-02420a00f329' 
  AND A.paid_on >='2024-01-01'
  AND B.invoice_id ILIKE '%cls_transfer%'
  AND A.enrollment_type_new='HIGH'
  
  )

, Y as -- student ids 
(
SELECT z.student_id FROM X
INNER JOIN data_playground.teacher_payout_high_change z ON X.student_id=z.student_id 
  WHERE z.change_date<='2024-10-31' -- Before November
  )

------------------------------------ Students whose class transfered 


-- WITH 
,
data AS (
SELECT 
A.student_id , A.teacher_id,A.grade
,A.payment_id
, A.paid_on
,A.invoice_number
,A.COUPON_CODE
, A.amount as amount_INR_models
, B.amount as amount_currency -- in currency of payment
, B.currency as cur
, A.currency_conversion_rate
, B.meta.credits_data.applied_credits::decimal  
, B.credits_used
, B.invoice_id
, ROUND((A.currency_conversion_rate::float * B.meta.credits_data.applied_credits::float),2) as credits_applied_INR_cal  
, A.no_classes
, A.enrollment_type_new
-- , COALESCE(B.amount::decimal / NULLIF(a.no_classes, 0),0) AS currency_RatePerClass
, ROUND(COALESCE(B.amount::decimal / NULLIF(a.no_classes, 0),0),1) AS currency_RPC_Rounded
-- , COALESCE(A.amount::decimal / NULLIF(a.no_classes, 0),0)  AS INR_RatePerClass
  , ROUND(COALESCE(A.amount::decimal / NULLIF(a.no_classes, 0),0),1)  AS INR_RPC_Rounded
,CASE 
WHEN lower(B.invoice_id) LIKE '%test%' 
OR lower(B.invoice_id) LIKE '%dup%' 
OR lower(B.invoice_id) LIKE '%can%'
OR lower(B.invoice_id) LIKE '%cls_extension%'
OR lower(B.invoice_id) LIKE '%tets%'
OR lower(B.invoice_id) LIKE '%stuent_up_%'
OR lower(B.invoice_id) LIKE '%upgrade_v2%'
OR lower(B.invoice_id) LIKE '%free%'
OR B.invoice_id = '%US_CONTRACT%'
OR lower(B.invoice_id) LIKE '%dummy%' --43 dummy cases
OR B.invoice_id is null
OR lower(B.invoice_id)  LIKE '%dummy_uae_n%'
  THEN 'Free_classes'
  ELSE 'Paid_classes'
  END as payment_tags

FROM data_models.payment AS A
LEFT JOIN application_service_intelenrollment.student_payment_link AS B ON A.payment_id = B.id

WHERE 0=0
  AND A.student_id IN (SELECT student_id FROM Y)
--   = '6c0d43aa-9a69-11ed-9ca8-12452666d05d' 
--   AND B.invoice_id NOT ILIKE '%dummy%' -- Removing dummy payments
  
ORDER BY A.paid_on DESC -- latest payments first
    )

, A as
(
  SELECT A.id as student_course_id, A.student_id ,B.stream
, C.meta.student_course_fee_id::Text, C.teacher_id, C.class_taken_on, C.id AS class_log_id  -- Class log details
, COALESCE(D.meta.student_payment_link_id::TEXT, D.meta.ccw_payment_link_id::TEXT) as Payment_id
, E.currency_RPC_Rounded
 ,cur
, E.INR_RPC_Rounded
, F.amount as payout_amount -- for that class log id
, CASE WHEN f.meta.no_show_payout = True THEN f.amount_pre_tax*4 ELSE amount_pre_tax END AS CLass_payout_og
  ,payment_tags
, C.meta.class_lapsed
, C.meta.student_no_show
, C.meta.teacher_no_show
, F.meta.region::TEXT as region -- region from class_payout meta
  
, D.enrollment_type as SCF_enrollment_type
FROM application_service_intelenrollment.student_course AS A
LEFT JOIN application_service_intelenrollment.course AS B ON A.course_id= B.id
LEFT JOIN application_service_intelenrollment.student_course_class_log AS C ON A.id = C.student_course_id
LEFT JOIN application_service_intelenrollment.student_course_fee AS D ON  C.meta.student_course_fee_id::Text = D.id 
LEFT JOIN data E ON  COALESCE( D.meta.student_payment_link_id::tEXT, D.meta.ccw_payment_link_id::TEXT) = E.payment_id
LEFT JOIN application_service_payout.class_payout AS F ON C.id = F.class_log_id
where A.student_id IN (SELECT student_id FROM Y)
--   = '6c0d43aa-9a69-11ed-9ca8-12452666d05d'
and A.access_type = 'WITH_TEACHER'
  and C.meta.teacher_no_show IS NULL -- when teacher is not present she does not get payout 
and C.class_taken_on>='2024-01-01'::Date
  
  and C.class_taken_on<'2024-10-31'::Date
order by class_taken_on DESC
 )


, B as 
(
SELECT * 
,  ROUND(((NULLIF(INR_RPC_Rounded::decimal,0)-CLass_payout_og::decimal)/NULLIF(INR_RPC_Rounded::decimal,0))*100,2)  as GM_Pct
FROM A
WHERE student_id='dc04463a-f28f-4286-a26f-9d4febd80336'
)


SELECT 
student_id
,SCF_enrollment_type
,AVG(currency_rpc_rounded) as CPC
,MAX(cur)
,COUNT(class_log_id) as nr_of_classes 
,AVG(GM_Pct) as avg_gross_margins
FROM B
GROUP BY 1,2
ORDER BY 1

-- SELECT *
-- FROM data_playground.teacher_payout_high_change
-- WHERE student_id = '0da06a78-fe3a-11ea-9fea-02420a00ff63'

-- e9dee8b6-62ed-11eb-9b3c-02420a00f7b0-- student_id Dummy issue

-------------------------------------------- end of 2 --------------------------------------------


------------------------------------------ START OF 3 ------------------------------------------
with 
---------------------------------- Students whose class transfered to HIGH 
X as
(
SELECT 
A.student_id , A.teacher_id,A.grade
,A.payment_id
, A.paid_on
, B.invoice_id
, A.no_classes
, A.enrollment_type_new
FROM data_models.payment AS A
LEFT JOIN application_service_intelenrollment.student_payment_link AS B ON A.payment_id = B.id

WHERE 0=0
--   AND A.student_id = 'a46cd6ba-afb9-11ea-abcd-02420a00f329' 
  AND A.paid_on >='2024-01-01'
  AND B.invoice_id ILIKE '%cls_transfer%'
  AND A.enrollment_type_new='HIGH'
  )

, Y as -- student ids 
(
SELECT z.student_id FROM X
INNER JOIN data_playground.teacher_payout_high_change z ON X.student_id=z.student_id 
  )

-- Remove these students to get overall stats
-- Define a threshold margin below which a class is considered to have a margin delta (e.g., Threshold=25%).
-- Will be different across Class offerings, High, Regular and Plus
-- Margin Delta Flag = Gross Margin < Threshold Margin
-- Number of Margin Delta Classes / Total Classes
-- Loss = (Expected Margin − Actual Margin) × Revenue per Class 
,
data AS (
SELECT 
A.student_id , A.teacher_id  ,A.grade
,A.payment_id
, A.paid_on
,A.invoice_number
,A.COUPON_CODE
, A.amount as amount_INR_models
, B.amount as amount_currency -- in currency of payment
, B.currency as cur
, A.currency_conversion_rate
, B.meta.credits_data.applied_credits::decimal  
, B.credits_used
, B.invoice_id
, ROUND((A.currency_conversion_rate::float * B.meta.credits_data.applied_credits::float),2) as credits_applied_INR_cal  
, A.no_classes
, A.enrollment_type_new
-- , COALESCE(B.amount::decimal / NULLIF(a.no_classes, 0),0) AS currency_RatePerClass
, ROUND(COALESCE(B.amount::decimal / NULLIF(a.no_classes, 0),0),1) AS currency_RPC_Rounded
-- , COALESCE(A.amount::decimal / NULLIF(a.no_classes, 0),0)  AS INR_RatePerClass
  , ROUND(COALESCE(A.amount::decimal / NULLIF(a.no_classes, 0),0),1)  AS INR_RPC_Rounded
,CASE 
WHEN lower(B.invoice_id) LIKE '%test%' 
OR lower(B.invoice_id) LIKE '%dup%' 
OR lower(B.invoice_id) LIKE '%can%'
OR lower(B.invoice_id) LIKE '%cls_extension%'
OR lower(B.invoice_id) LIKE '%tets%'
OR lower(B.invoice_id) LIKE '%stuent_up_%'
OR lower(B.invoice_id) LIKE '%upgrade_v2%'
OR lower(B.invoice_id) LIKE '%free%'
OR B.invoice_id = '%US_CONTRACT%'
OR lower(B.invoice_id) LIKE '%dummy%' 
OR B.invoice_id is null
OR lower(B.invoice_id)  LIKE '%dummy_uae_n%'
  THEN 'Free_classes'
  ELSE 'Paid_classes'
  END as payment_tags

FROM data_models.payment AS A
LEFT JOIN application_service_intelenrollment.student_payment_link AS B ON A.payment_id = B.id

WHERE 0=0
  AND A.student_id NOT IN (SELECT student_id FROM Y) -- NOT IN -2- has these students
--   = '6c0d43aa-9a69-11ed-9ca8-12452666d05d' 
--   AND B.invoice_id NOT ILIKE '%dummy%' -- Removing dummy payments
  
ORDER BY A.paid_on DESC -- latest payments first
  )

-- SELECT COUNT(*) FROM data -- 540K Payments :o
-------------------------------------------------------------------------------------------------------------------------------

, A as
(
  SELECT A.id as student_course_id, A.student_id ,B.stream
, C.meta.student_course_fee_id::Text, C.teacher_id, C.class_taken_on, C.id AS class_log_id  -- Class log details
, COALESCE(D.meta.student_payment_link_id::TEXT, D.meta.ccw_payment_link_id::TEXT) as Payment_id
, E.currency_RPC_Rounded
 ,cur
, E.INR_RPC_Rounded
, F.amount as payout_amount -- for that class log id
, CASE WHEN f.meta.no_show_payout = True THEN f.amount_pre_tax*4 ELSE amount_pre_tax END AS CLass_payout_og
  ,payment_tags
, C.meta.class_lapsed
, C.meta.student_no_show
, C.meta.teacher_no_show
, F.meta.region::TEXT as region -- region from class_payout meta
,CASE 
  WHEN F.meta.region::TEXT IN ('ROW 1', 'ROW 2') THEN 'ROW'
  WHEN F.meta.region::TEXT IN ('India', 'Indian Subcontinent') THEN 'India'
  ELSE F.meta.region::TEXT
  END as derived_region_payout
, D.enrollment_type as SCF_enrollment_type
FROM application_service_intelenrollment.student_course AS A
LEFT JOIN application_service_intelenrollment.course AS B ON A.course_id= B.id
LEFT JOIN application_service_intelenrollment.student_course_class_log AS C ON A.id = C.student_course_id
LEFT JOIN application_service_intelenrollment.student_course_fee AS D ON  C.meta.student_course_fee_id::Text = D.id 
LEFT JOIN data E ON COALESCE( D.meta.student_payment_link_id::tEXT, D.meta.ccw_payment_link_id::TEXT) = E.payment_id
LEFT JOIN application_service_payout.class_payout AS F ON C.id = F.class_log_id
where 0=0
and A.student_id NOT IN (SELECT student_id FROM Y)
--   = '6c0d43aa-9a69-11ed-9ca8-12452666d05d'
and A.access_type = 'WITH_TEACHER'
and C.meta.teacher_no_show IS NULL -- when teacher is not present she does not get payout 
and C.class_taken_on>='2024-01-01'::Date
  
  and C.class_taken_on<'2024-10-31'::Date
order by class_taken_on DESC -- recent classes first 
 )

------------------------------------------------------------------------------------------------------------------------------------------------------
, B as 
(
SELECT * 
,  ROUND(((NULLIF(INR_RPC_Rounded::decimal,0)-CLass_payout_og::decimal)/NULLIF(INR_RPC_Rounded::decimal,0))*100,2)  as GM_Pct
FROM A
WHERE 0=0 
-- and student_id='8d518246-033a-11eb-9acd-02420a00f7d7'
and payment_tags='Paid_classes'
  -- Can handle free classes later
)
-- derived_region_payout



SELECT 
student_id
,SCF_enrollment_type
,currency_rpc_rounded
,AVG(currency_rpc_rounded) as CPC
,MAX(cur) as native_currency_max
,COUNT(class_log_id) as nr_of_classes 
,AVG(GM_Pct) as avg_gross_margins
FROM B
WHERE 0=0 -- 0 cpc classes handled for now
-- AND student_id='001de388-c2c6-11ea-b8a8-02420a00f641'
GROUP BY 1,2,3
HAVING AVG(GM_Pct) IS NOT NULL AND AVG(GM_Pct)>0
ORDER BY 1

-- Discussion
-- [X] Student - Class Offering level Average GM analysis
-- [ ] Class level GM analysis [--4--] [https://app.periscopedata.com/app/cuemath/1225508/Ranjosh-Dash?widget=18600159&udv=0]



------------------------------------------ END OF 3 ------------------------------------------
-- We were seeing what is a good GM percentage