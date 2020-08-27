{{ config(materialized='table', alias='heap_app_sessions') }}

SELECT
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    session_id,
    MIN(session_start_time) AS session_start_time,
    MAX(event_time) AS session_end_time,
    datediff('minute', MIN(session_start_time), MAX(event_time)) AS session_length_mins,
    MAX(ip) AS ip,
    MAX(region) AS ip_region,
    COUNT(distinct event_id) AS app_pageview_count
FROM {{ ref('heap_fact_app_page_viewed') }} AS app_page_viewed
JOIN {{ ref('heap_dimension_user') }} AS users
    ON app_page_viewed.user_id = users.user_id
WHERE contact_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7
