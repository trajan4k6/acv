{{ config(materialized='table') }}

with fsd_combined as (
SELECT 
    date_key,
    dimension_firm_key
FROM {{ ref('preqin_fact_paid_subscriber_daily') }}
GROUP BY 1, 2
UNION
SELECT 
    date_key,
    dimension_firm_key
FROM {{ ref('preqin_fact_paid_subscriber_daily_history') }}
)

,fsd_grouped as (
SELECT 
    dimension_firm_key, 
    to_date(to_char(date_key),'YYYYMMDD') sub_date,
    dateadd(day, -1 * dense_rank() over(partition by dimension_firm_key order by sub_date), sub_date) as grp
FROM fsd_combined
)

,sub_range as (
SELECT 
    dimension_firm_key, 
    min(sub_date) as start_range, 
    max(sub_date) as end_range 
FROM fsd_grouped 
GROUP BY dimension_firm_key, grp
)

,sub_break as (
SELECT 
    dimension_firm_key,
    row_number() over (partition by dimension_firm_key order by start_range asc) as RN,
    start_range, 
    end_range,
    datediff(d, end_range, lag(start_range) over (partition by dimension_firm_key order by start_range desc)) AS Sub_Break_Duration_Days
FROM sub_range
)

SELECT * FROM sub_break