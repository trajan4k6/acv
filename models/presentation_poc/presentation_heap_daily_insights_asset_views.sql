{{ config(materialized='table', alias='heap_daily_insights_asset_views') }}

WITH insights_pageviews AS
(
  SELECT 
      contact_id,
      account_id,
      contact_name,
      firm_name,
      legacy_firm_id,
      sales_region,
      event_time,
      event_id,
      session_id,
      path,
      insights_asset_type
  FROM {{ ref('heap_fact_insights_page_viewed') }} AS insights_page_viewed
  JOIN {{ ref('heap_dimension_user') }} AS users
      ON insights_page_viewed.user_id = users.user_id
  WHERE account_id IS NOT NULL
    AND insights_asset_type IS NOT NULL
)
SELECT
    event_time::date AS date,
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    insights_asset_type AS content_type,
    path,
    count(distinct event_id) AS insights_asset_view_count
FROM insights_pageviews
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
