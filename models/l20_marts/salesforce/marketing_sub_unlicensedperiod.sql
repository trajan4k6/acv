{{ config(materialized='table') }}

with mycte as (
  SELECT distinct
    us.user_id,
    to_date(SUBSCRIPTION_STARTDATE) as SUBSCRIPTION_STARTDATE,
    to_date(subscription_expiry_date) as subscription_expiry_date
  FROM {{ ref('stg_tbluser_subscription') }} us
    JOIN {{ ref('stg_tbluser_details') }} ud
      ON us.user_id = ud.user_id
    JOIN {{ ref('stg_tblcontactfirm') }} cf
        ON ud.contactfirm_id =cf.contactfirm_id
    JOIN {{ ref('stg_tblcontact') }} c
        ON cf.contact_id = c.contact_id
    JOIN {{ ref('stg_tblpei_product') }} p
        ON us.product_id = p.product_id 
  WHERE
    p.free = 0
    AND p.product_type = 'Service'
    AND us.subscription_status = 2
    AND cf.firm_id <> 952
    AND c.contact_status = 1
    AND cf.cf_status = 1
    AND NVL(p.accesslevel,'') IN ('Standard', 'Premium', 'Academic')
    AND subscription_expiry_date > '1990-01-01' AND SUBSCRIPTION_STARTDATE > '1990-01-01'
--AND us.user_id = 325123
--ORDER BY subscription_expiry_date, SUBSCRIPTION_STARTDATE
),
mycte2 as (
  select *,
    row_number() over (partition by user_ID order by subscription_expiry_date, SUBSCRIPTION_STARTDATE) as RN,
    CASE WHEN subscription_expiry_date < CURRENT_DATE THEN TRUE ELSE FALSE END AS Expired
    FROM mycte
)
, 
--select * FROM mycte2
mycte3 as (
select max(subscription_expiry_date) as Max_subscription_expiry_date, user_ID
  FROM mycte2
  WHERE Expired = TRUE 
  GROUP BY user_ID
)
,
--select * FROM mycte3
mycte4 as (
select MIN(SUBSCRIPTION_STARTDATE) as MIN_SUBSCRIPTION_STARTDATE, user_ID
  FROM mycte2
  WHERE Expired = FALSE
  GROUP BY user_ID
)
--select * FROM mycte4
SELECT 
      b.user_id
    , NVL(DATEDIFF(MONTH,a.Max_subscription_expiry_date, b.MIN_SUBSCRIPTION_STARTDATE),0) UnlicensedPeriod
    , a.Max_subscription_expiry_date
    , b.MIN_SUBSCRIPTION_STARTDATE 
    , CASE WHEN UnlicensedPeriod >= 12 OR a.Max_subscription_expiry_date IS NULL THEN 'TRUE' ELSE 'FALSE' END AS TreatAsNew
FROM  mycte4 b 
  LEFT join mycte3 a ON a.user_id = b.user_id AND b.MIN_SUBSCRIPTION_STARTDATE >= a.Max_subscription_expiry_date
WHERE b.MIN_SUBSCRIPTION_STARTDATE > DATEADD(DAY,-7,current_date) AND b.MIN_SUBSCRIPTION_STARTDATE < current_date