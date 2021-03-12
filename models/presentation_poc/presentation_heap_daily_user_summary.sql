{{ config(materialized='table', alias='heap_daily_user_summary') }}

WITH key_actions AS
(
  SELECT
      user_id,
      event_time::date AS event_date,
      count(distinct CASE WHEN event_name = 'data_table_download_confirmed' THEN event_id END) AS total_downloads,
      count(distinct CASE WHEN event_name IN ('add_to_target_list_clicked', 'create_target_list_clicked') THEN event_id END) AS create_add_target_list,
      count(distinct CASE WHEN event_name IN ('add_to_custom_benchmark_clicked', 'create_custom_benchmark_clicked') THEN event_id END) AS create_add_custom_benchmark,
      count(distinct CASE WHEN event_name = 'save_new_search_clicked' THEN event_id END) AS save_new_search,
      count(distinct CASE WHEN event_name = 'create_alert_clicked' THEN event_id END) AS create_alert
  FROM {{ ref('heap_fact_pro_key_actions') }}
  WHERE event_name IN 
    ('data_table_download_confirmed', 
     'add_to_target_list_clicked', 
     'create_target_list_clicked', 
     'add_to_custom_benchmark_clicked',
     'create_custom_benchmark_clicked',
     'save_new_search_clicked',
     'create_alert_clicked'
    )
  GROUP BY 1, 2
)
, sessions AS
(
  SELECT
    user_id,
    event_time::date AS event_date,
    count(distinct event_id) AS pageviews,
    count(distinct session_id) AS sessions,
    count(distinct case when device_type = 'Desktop' then session_id end) AS desktop_sessions,
    count(distinct case when device_type = 'Mobile' then session_id end) AS mobile_sessions,
    count(distinct case when device_type = 'Tablet' then session_id end) AS tablet_sessions
  FROM {{ ref('heap_fact_app_page_viewed') }}
  GROUP BY 1, 2
)
, data_downloads AS
(
  SELECT
    user_id,
    event_time::date AS event_date,
    count(distinct CASE WHEN app_section_category = 'Search' THEN event_id END) AS search_downloads,
    count(distinct CASE WHEN app_section_category = 'Profile' THEN event_id END) AS profile_downloads,
    count(distinct CASE WHEN app_section_category = 'Charts' THEN event_id END) AS chart_downloads,
    count(distinct CASE WHEN app_section_category = 'Market Benchmarks' THEN event_id END) AS market_benchmark_downloads,
    count(distinct CASE WHEN app_section_category = 'Target List' THEN event_id END) AS target_list_downloads,
    count(distinct CASE WHEN app_section_category = 'My Benchmarks' THEN event_id END) AS my_benchmark_downloads,
    sum(download_row_count) as total_download_row_count
  FROM {{ ref('heap_fact_data_downloads') }} 
  GROUP BY 1, 2
)
,discover AS
(
  SELECT
    user_id,
    event_time::date AS event_date,
    count(distinct case when discover_section = 'deals' then event_id end) AS deals_discover,
    count(distinct case when discover_section = 'funds' then event_id end) AS funds_discover,
    count(distinct case when discover_section = 'serviceproviders' then event_id end) AS serviceproviders_discover,
    count(distinct case when discover_section = 'investors' then event_id end) AS investors_discover,
    count(distinct case when discover_section = 'fundManager' then event_id end) AS fundManager_discover,
    count(distinct case when discover_section = 'investorNews' then event_id end) AS investorNews_discover,
    count(distinct case when discover_section = 'assets' then event_id end) AS assets_discover,
    count(distinct case when discover_section = 'consultants' then event_id end) AS consultants_discover
  FROM {{ ref('heap_fact_app_page_viewed') }}
  GROUP BY 1, 2
)
SELECT
    sessions.event_date AS date,
    identity,
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    subscription_status,
    min(heap_date_created)::date AS first_session_date,
    sum(pageviews) AS pageviews,
    sum(sessions) AS sessions,
    sum(desktop_sessions) AS desktop_sessions,
    sum(mobile_sessions) AS mobile_sessions,
    sum(tablet_sessions) AS tablet_sessions,
    sum(create_add_target_list) AS create_add_target_list,
    sum(create_add_custom_benchmark) AS create_add_custom_benchmark,
    sum(save_new_search) AS save_new_search,
    sum(create_alert) AS create_alert,
    sum(total_downloads) AS total_downloads,
    sum(total_download_row_count) as total_download_row_count,
    sum(search_downloads) AS search_downloads,
    sum(profile_downloads) AS profile_downloads,
    sum(chart_downloads) AS chart_downloads,
    sum(market_benchmark_downloads) AS market_benchmark_downloads,
    sum(target_list_downloads) AS target_list_downloads,
    sum(my_benchmark_downloads) AS my_benchmark_downloads,
    sum(deals_discover) AS deals_discover,
    sum(funds_discover) AS funds_discover,
    sum(serviceproviders_discover) AS serviceproviders_discover,
    sum(investors_discover) AS investors_discover,
    sum(fundManager_discover) AS fundManager_discover,
    sum(investorNews_discover) AS investorNews_discover,
    sum(assets_discover) AS assets_discover,
    sum(consultants_discover) AS consultants_discover
FROM sessions
  JOIN {{ ref('heap_dimension_user') }} AS users
      ON sessions.user_id = users.user_id
  LEFT JOIN key_actions
      ON sessions.user_id = key_actions.user_id
        AND sessions.event_date = key_actions.event_date
  LEFT JOIN data_downloads
      ON sessions.user_id = data_downloads.user_id
        AND sessions.event_date = data_downloads.event_date
  LEFT JOIN discover
      ON sessions.user_id = discover.user_id
        AND sessions.event_date = discover.event_date
--WHERE 
--account_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
 