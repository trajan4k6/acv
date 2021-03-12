{{ config(materialized='table', alias='heap_daily_data_downloads') }}

WITH downloads AS
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
      profile_type,
      app_section_category,
      download_row_count
  FROM {{ ref('heap_fact_data_downloads') }} AS data_downloads
  JOIN {{ ref('heap_dimension_user') }} AS users
      ON data_downloads.user_id = users.user_id
  WHERE account_id is not null
    AND path is not null
)
SELECT
    event_time::date AS date,
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    profile_type,
    app_section_category,
    path,
    count(distinct event_id) AS total_downloads,
    sum(download_row_count) as total_download_row_count
FROM downloads
{{ dbt_utils.group_by(n=10) }}
