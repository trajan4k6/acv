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
app_section_category,
profile_type,
profile_id,
profile_section,
SUM(profile_view_count) AS profile_view_count
FROM {{ ref('presentation_heap_daily_profile_views') }} Fact
LEFT
JOIN {{ ref('heap_dimension_firm_integrated') }} F
    ON Fact.Account_ID = F.Account_ID
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.salesforce_dimension_account_key = A.dimension_account_key
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9



 