{{ config(materialized='table', alias='heap_daily_app_page_views') }}

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
        app_section_category,
        profile_type, 
        profile_id,
        profile_section
    FROM {{ ref('heap_fact_app_page_viewed') }} AS app_page_viewed
    JOIN {{ ref('heap_dimension_user') }} AS users
        ON app_page_viewed.user_id = users.user_id
   /* WHERE account_id IS NOT NULL
        AND app_section_category IS NOT NULL
    */
    WHERE app_section_category IS NOT NULL

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
    app_section_category,
    profile_type,
    profile_id,
    profile_section,
    count(distinct event_id) AS app_page_view_count
FROM app_pageviews
{{ dbt_utils.group_by(n=12) }}
 