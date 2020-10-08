{{
    config(
        materialized='incremental', 
        alias='fact_insights_page_viewed'
    ) 
}}

SELECT
    user_id,
    event_id,
    session_id,
    time AS event_time,
    session_time AS session_start_time,
    platform,
    device_type,
    browser,
    country,
    region,
    city,
    ip,
    referrer,
    landing_page,
    landing_page_query,
    landing_page_hash,
    path,
    query,
    hash,
    utm_source,
    utm_campaign,
    utm_medium,
    utm_term,
    utm_content,
    -- same definition as this Heap custom property https://heapanalytics.com/app/definitions?view=properties&type=defined_property&id=Insights-Category-302001
    CASE
        WHEN path = '/insights/premium-publications' AND query IS NULL THEN 'Home/My Publications'
        WHEN path = '/insights/premium-publications' AND query IS NOT NULL THEN 'List View - Premium Publications'
        WHEN path ILIKE '/insights/premium-publications/publication/%' THEN 'Publication Info Page'
        WHEN path ILIKE '/insights/conferences-and-events/%/%' THEN 'Conference/Event Page'
        WHEN path ILIKE '/insights/conferences-and-events/%' OR path = '/insights/conferences-and-events' THEN 'List View - Conferences and Events'
        WHEN path ILIKE '/insights/research/%/%' OR path ILIKE '/insights/global-reports/%' THEN 'Research Asset'
        WHEN path ILIKE '/insights/research/%' OR path = '/insights/research' THEN 'List View - Research'
        WHEN path ILIKE '/insights%' AND domain = 'www.preqin.com' THEN 'Other Insights'
    END AS insights_category,
    -- same definition as this Heap custom property https://heapanalytics.com/app/definitions?view=properties&type=defined_property&id=Insights-Asset-Type-302003
    CASE
        WHEN path ILIKE '/insights/research/reports/%' THEN 'Report'
        WHEN path ILIKE '/insights/research/quarterly-updates/%' THEN 'Quarterly Update'
        WHEN path ILIKE '/insights/global-reports/%' THEN 'Global Report'
        WHEN path ILIKE '/insights/research/factsheets/%' THEN 'Factsheet'
        WHEN path ILIKE '/insights/research/videos/%' THEN 'Video'
        WHEN path ILIKE '/insights/research/podcasts/%' THEN 'Podcast'
        WHEN path ILIKE '/insights/research/investor-outlooks/%' THEN 'Investor Outlook'
        WHEN path ILIKE '/insights/research/blogs/%' THEN 'Blog'
        WHEN path ILIKE '/insights/%/%/%' THEN 'Other'
    END AS insights_asset_type
FROM {{ source('heap', 'mammoth_poc_reports_insights_insights_page_viewed') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
