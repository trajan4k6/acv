{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
CAST(YEAR((Fact.DATE::DATE)) || RIGHT('0' || MONTH((Fact.DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((Fact.DATE::DATE)), 2) AS INT) AS DATE_KEY,
CASE WHEN F.conformed_dimension_firm_key = '-1' THEN F.dimension_firm_key ELSE COALESCE(F.conformed_dimension_firm_key,'-1') END DIMENSION_FIRM_KEY,
COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY,'-1') AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY,
COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') AS DIMENSION_REGION_TEAM_KEY,
COALESCE(A.DIMENSION_REGION_KEY, '-1')      AS DIMENSION_REGION_KEY,
NVL(PAGEVIEWS, 0)                           AS PAGEVIEWS,
NVL(SESSIONS, 0)                            AS SESSIONS,
NVL(DESKTOP_SESSIONS, 0)                    AS DESKTOP_SESSIONS,
NVL(MOBILE_SESSIONS, 0)                     AS MOBILE_SESSIONS,
NVL(TABLET_SESSIONS, 0)                     AS TABLET_SESSIONS,
NVL(CREATE_ADD_TARGET_LIST, 0)              AS CREATE_ADD_TARGET_LIST,
NVL(CREATE_ADD_CUSTOM_BENCHMARK, 0)         AS CREATE_ADD_CUSTOM_BENCHMARK,
NVL(SAVE_NEW_SEARCH, 0)                     AS SAVE_NEW_SEARCH,
NVL(CREATE_ALERT, 0)                        AS CREATE_ALERT,
NVL(TOTAL_DOWNLOADS, 0)                     AS TOTAL_DOWNLOADS,
NVL(SEARCH_DOWNLOADS, 0)                    AS SEARCH_DOWNLOADS,
NVL(PROFILE_DOWNLOADS, 0)                   AS PROFILE_DOWNLOADS,
NVL(CHART_DOWNLOADS, 0)                     AS CHART_DOWNLOADS,
NVL(MARKET_BENCHMARK_DOWNLOADS, 0)          AS MARKET_BENCHMARK_DOWNLOADS,
NVL(TARGET_LIST_DOWNLOADS, 0)               AS TARGET_LIST_DOWNLOADS,
NVL(MY_BENCHMARK_DOWNLOADS, 0)              AS MY_BENCHMARK_DOWNLOADS
FROM {{ ref('presentation_heap_daily_user_summary') }} Fact
LEFT
JOIN {{ ref('heap_dimension_firm_integrated') }} F
    ON Fact.Account_ID = F.Account_ID
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.salesforce_dimension_account_key = A.dimension_account_key