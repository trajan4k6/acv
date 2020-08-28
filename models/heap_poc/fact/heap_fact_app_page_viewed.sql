{{
    config(
        materialized='incremental', 
        alias='fact_app_page_viewed'
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
        WHEN path ILIKE '%mybenchmarks%' OR path ILIKE '/dashboard/benchmark/%/custom%' OR path ILIKE '/discoverAnalysis/benchmarks/%/custom' THEN 'My Benchmarks'
        WHEN path = '/analysis/horizonIRRs' OR path ILIKE '%benchmarks%' OR path ILIKE '/discoverAnalysis/benchmarks/%/market%' THEN 'Market Benchmarks'
        WHEN path ILIKE '/discover%' OR path = '/search' THEN 'Search'
        WHEN path ILIKE '/analysis%' THEN 'Charts'
        WHEN path IN ('/dashboard', '/dashboard/') THEN 'Dashboard Home'
        WHEN path ILIKE '/portfolio%' THEN 'My Portfolio'
        WHEN path = '/datasupport' THEN 'Support'
    END AS app_section_category,
    CASE
        WHEN profile_type IS NOT NULL THEN split_part(path, '/', 3)
    END AS profile_id
FROM {{ source('heap', 'mammoth_pro_key_actions_app_page_viewed') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
