{{
    config(
        materialized='incremental', 
        alias='fact_data_downloads'
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
    -- clean for only numeric values to convert string to integer
    regexp_replace(download_search_row_count, '[^[:digit:]]')::int as download_row_count,
    -- same definition as this Heap custom property https://heapanalytics.com/app/definitions?view=properties&type=defined_property&id=Profile-Type-301892
    CASE
        WHEN path ILIKE '/deal/%' THEN 'Deal Profile'
        WHEN path ILIKE '/serviceprovider/%' THEN 'Service Provider Profile'
        WHEN path ILIKE '/asset/%' THEN 'Asset Profile'
        WHEN path ILIKE '/investmentconsultant/%' THEN 'Investment Consultant'
        WHEN path ILIKE '/fundmanager/%' THEN 'Fund Manager Profile'
        WHEN path ILIKE '/funds/%' THEN 'Fund Profile'
        WHEN path ILIKE '/investor/%' THEN 'Investor Profile'
    END AS profile_type,
    -- same definition as this Heap custom property https://heapanalytics.com/app/definitions?view=properties&type=defined_property&id=App-Section-Category-301869
    CASE
        WHEN profile_type IS NOT NULL THEN 'Profile'
        WHEN path ILIKE '/dashboard/watchlist%' OR path ILIKE '/discover%targetlists%' THEN 'Target List'
        WHEN path = '/dashboard/alerts' THEN 'Alerts'
        WHEN path = '/dashboard/savedSearch' THEN 'Saved Searches'
        WHEN path ILIKE '%myBenchmarks%' OR (path ILIKE '%benchmarks%' and path ILIKE '%custom%') THEN 'Custom Benchmarks'
        WHEN path ILIKE '%benchmarks%' OR path ilike '%/analysis/quarterlyIndex' OR path = '/analysis/riskAndReturn' OR path ILIKE '/analysis/horizonIRRs%' THEN 'Market Benchmarks'
        WHEN path ILIKE '/discover%' OR path = '/search' THEN 'Search'
        WHEN path ILIKE '/analysis/leagueTables/%' THEN 'League Tables'
        WHEN path ILIKE '/analysis%' THEN 'Charts'
        WHEN path IN ('/dashboard', '/dashboard/') THEN 'Dashboard Home'
        WHEN path ILIKE '/portfolio%' THEN 'My Portfolio'
        WHEN path = '/datasupport' THEN 'Support'
    END AS app_section_category
FROM {{ source('heap', 'pro_downloads_pro_key_actions_data_table_download_confirmed') }}
WHERE 1 = 1

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and event_time > (select max(event_time) from {{ this }})

{% endif %}
