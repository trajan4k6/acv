{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

--Dedup Identity mapping Heap.Identity(M)Contactfirm_id(1)
WITH heap_identity_rank AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY Identity ORDER BY HEAP_LAST_MODIFIED DESC) AS RN
FROM {{ ref('heap_dimension_user_integrated') }}
),
heap_identity AS (
    SELECT * FROM heap_identity_rank WHERE RN=1
)

SELECT 
CAST(YEAR((Fact.DATE::DATE)) || RIGHT('0' || MONTH((Fact.DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((Fact.DATE::DATE)), 2) AS INT) AS DATE_KEY,
CASE WHEN U.conformed_dimension_individual_key = '-1' THEN U.dimension_user_key ELSE COALESCE(U.conformed_dimension_individual_key,'-1') END DIMENSION_INDIVIDUAL_KEY,
CASE WHEN F.conformed_dimension_firm_key = '-1' THEN F.dimension_firm_key ELSE COALESCE(F.conformed_dimension_firm_key,'-1') END DIMENSION_FIRM_KEY,
SUM(NVL(PAGEVIEWS, 0))                           AS PAGEVIEWS,
SUM(NVL(SESSIONS, 0))                            AS SESSIONS,
SUM(NVL(DESKTOP_SESSIONS, 0))                    AS DESKTOP_SESSIONS,
SUM(NVL(MOBILE_SESSIONS, 0))                     AS MOBILE_SESSIONS,
SUM(NVL(TABLET_SESSIONS, 0))                     AS TABLET_SESSIONS,
SUM(NVL(CREATE_ADD_TARGET_LIST, 0))              AS CREATE_ADD_TARGET_LIST,
SUM(NVL(CREATE_ADD_CUSTOM_BENCHMARK, 0))         AS CREATE_ADD_CUSTOM_BENCHMARK,
SUM(NVL(SAVE_NEW_SEARCH, 0))                     AS SAVE_NEW_SEARCH,
SUM(NVL(CREATE_ALERT, 0))                        AS CREATE_ALERT,
SUM(NVL(TOTAL_DOWNLOADS, 0))                     AS TOTAL_DOWNLOADS,
SUM(NVL(SEARCH_DOWNLOADS, 0))                    AS SEARCH_DOWNLOADS,
SUM(NVL(PROFILE_DOWNLOADS, 0))                   AS PROFILE_DOWNLOADS,
SUM(NVL(CHART_DOWNLOADS, 0))                     AS CHART_DOWNLOADS,
SUM(NVL(MARKET_BENCHMARK_DOWNLOADS, 0))          AS MARKET_BENCHMARK_DOWNLOADS,
SUM(NVL(TARGET_LIST_DOWNLOADS, 0))               AS TARGET_LIST_DOWNLOADS,
SUM(NVL(MY_BENCHMARK_DOWNLOADS, 0))              AS MY_BENCHMARK_DOWNLOADS,
SUM(NVL(DEALS_DISCOVER, 0))                      AS DEALS_DISCOVER,
SUM(NVL(FUNDS_DISCOVER, 0))                      AS FUNDS_DISCOVER,
SUM(NVL(SERVICEPROVIDERS_DISCOVER, 0))           AS SERVICEPROVIDERS_DISCOVER,
SUM(NVL(INVESTORS_DISCOVER, 0))                  AS INVESTORS_DISCOVER,
SUM(NVL(FUNDMANAGER_DISCOVER, 0))                AS FUNDMANAGER_DISCOVER,
SUM(NVL(INVESTORNEWS_DISCOVER, 0))               AS INVESTORNEWS_DISCOVER,
SUM(NVL(ASSETS_DISCOVER, 0))                     AS ASSETS_DISCOVER,
SUM(NVL(CONSULTANTS_DISCOVER, 0))                AS CONSULTANTS_DISCOVER
FROM {{ ref('presentation_heap_daily_user_summary') }} Fact
LEFT
JOIN {{ ref('heap_dimension_firm_integrated') }} F
    ON Fact.Account_ID = F.Account_ID
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.salesforce_dimension_account_key = A.dimension_account_key
LEFT
JOIN heap_identity U
    ON Fact.identity = U.identity
GROUP BY 1, 2, 3