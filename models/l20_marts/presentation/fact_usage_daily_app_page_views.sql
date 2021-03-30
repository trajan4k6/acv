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
COALESCE(PT.dimension_profile_type_key,'-1') AS dimension_profile_type_key,
app_section_category,
profile_section,
SUM(app_page_view_count) AS total_page_views
FROM {{ ref('presentation_heap_daily_app_page_views') }} Fact
LEFT
JOIN {{ ref('heap_dimension_firm_integrated') }} F
    ON Fact.Account_ID = F.Account_ID
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.salesforce_dimension_account_key = A.dimension_account_key
LEFT
JOIN heap_identity U
    ON Fact.Identity = U.Identity
LEFT
JOIN {{ ref('dimension_profile_type') }} PT
    ON Fact.profile_type = PT.profile_type
{{ dbt_utils.group_by(n=7) }}