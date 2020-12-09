{{ config(materialized='table', alias='heap_daily_profile_views') }}

WITH app_pageviews AS
(
    SELECT 
        identity,
        contact_id,
        account_id,
        contact_name,
        firm_name,
        legacy_firm_id,
        sales_region,
        event_time,
        event_id,
        session_id,
        profile_type, 
        profile_id,
        profile_section
    FROM {{ ref('heap_fact_profile_page_viewed') }} AS profile_page_viewed
    JOIN {{ ref('heap_dimension_user') }} AS users
        ON profile_page_viewed.user_id = users.user_id
)
SELECT
    event_time::date AS date,
    identity,
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    'Profile' as app_section_category,
    profile_type,
    profile_id,
    profile_section,
    count(distinct event_id) AS profile_view_count
FROM app_pageviews
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
 