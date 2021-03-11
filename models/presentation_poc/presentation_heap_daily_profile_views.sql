{{ config(materialized='table', alias='heap_daily_profile_views') }}

WITH profile_pageviews AS
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
        asset_class,
        profile_type, 
        profile_id,
        profile_name,
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
    asset_class,
    profile_type,
    profile_id,
    profile_name,
    profile_section,
    count(distinct event_id) AS profile_view_count
FROM profile_pageviews
{{ dbt_utils.group_by(n=13) }}
 