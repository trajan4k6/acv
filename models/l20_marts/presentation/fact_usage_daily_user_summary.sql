{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

--Dedup Contact_ID mapping Heap.User_Id(M)Contact_Id(1)
WITH heap_contact_rank AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY CONTACT_ID ORDER BY HEAP_LAST_MODIFIED DESC) AS RN
FROM {{ ref('heap_dimension_user_integrated') }}
),
heap_contact AS (
    SELECT * FROM heap_contact_rank WHERE RN=1
)

SELECT 
CAST(YEAR((Fact.DATE::DATE)) || RIGHT('0' || MONTH((Fact.DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((Fact.DATE::DATE)), 2) AS INT) AS DATE_KEY,
CASE WHEN U.conformed_dimension_individual_key = '-1' THEN U.dimension_user_key ELSE COALESCE(U.conformed_dimension_individual_key,'-1') END DIMENSION_INDIVIDUAL_KEY,
CASE WHEN F.conformed_dimension_firm_key = '-1' THEN F.dimension_firm_key ELSE COALESCE(F.conformed_dimension_firm_key,'-1') END DIMENSION_FIRM_KEY,
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
NVL(MY_BENCHMARK_DOWNLOADS, 0)              AS MY_BENCHMARK_DOWNLOADS,
NVL(DEALS_DISCOVER, 0)                      AS DEALS_DISCOVER,
NVL(FUNDS_DISCOVER, 0)                      AS FUNDS_DISCOVER,
NVL(SERVICEPROVIDERS_DISCOVER, 0)           AS SERVICEPROVIDERS_DISCOVER,
NVL(INVESTORS_DISCOVER, 0)                  AS INVESTORS_DISCOVER,
NVL(FUNDMANAGER_DISCOVER, 0)                AS FUNDMANAGER_DISCOVER,
NVL(INVESTORNEWS_DISCOVER, 0)               AS INVESTORNEWS_DISCOVER,
NVL(ASSETS_DISCOVER, 0)                     AS ASSETS_DISCOVER,
NVL(CONSULTANTS_DISCOVER, 0)                AS CONSULTANTS_DISCOVER
FROM {{ ref('presentation_heap_daily_user_summary') }} Fact
LEFT
JOIN {{ ref('heap_dimension_firm_integrated') }} F
    ON Fact.Account_ID = F.Account_ID
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.salesforce_dimension_account_key = A.dimension_account_key
LEFT
JOIN heap_contact U
    ON Fact.Contact_ID = U.Contact_ID