{{
    config(
        materialized='incremental', 
        alias='fact_profile_page_viewed'
    ) 
}}

SELECT
    user_id,
    event_id,
    session_id,
    time as event_time,
    session_time as session_start_time,
    platform,
    device_type,
    screen_dimensions,
    browser,
    search_keyword,
    country,
    region,
    city,
    ip,
    referrer,
    landing_page,
    landing_page_query,
    landing_page_hash,
    domain,
    path,
    query,
    hash,
    title,
    utm_source,
    utm_campaign,
    utm_medium,
    utm_term,
    utm_content,
    type,
    upper(assetclass) as asset_class,
    contains(asset_class, 'GEN') as has_asset_class_gen,
    contains(asset_class, 'HF') as has_asset_class_hf,
    contains(asset_class, 'INF') as has_asset_class_inf,
    contains(asset_class, 'NR') as has_asset_class_nr,
    contains(asset_class, 'PD') as has_asset_class_pd,
    contains(asset_class, 'PE') as has_asset_class_pe,
    contains(asset_class, 'RE') as has_asset_class_re,
    contains(asset_class, 'SEC') as has_asset_class_sec,
    contains(asset_class, 'VC') as has_asset_class_vc,
    locations_markets,
    geography,
    profilename as profile_name,
    client_locations,
    firm_type,
    profiletype as profile_type,
    fund_type,
    nullif(split_part(path, '/', 3), '') as profile_id,
    nullif(split_part(path, '/', 4), '') as profile_section
FROM {{ source('heap', 'pro_profile_profile_viewed') }}
-- because the custom event was configured improperly and fires on all page views, filter out for only profile page views
WHERE split_part(path, '/', 2) IN ('deal', 'serviceprovider', 'investmentconsultant', 'fundmanager', 'funds', 'investor')

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
