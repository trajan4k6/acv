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
CASE WHEN lower(fact.profile_type) = ('fundmanager') THEN GP.dimension_firm_key ELSE NULL END dimension_firm_GP_profile_key,
CASE WHEN lower(fact.profile_type) = ('investor') THEN LP.dimension_firm_key ELSE NULL END dimension_firm_LP_profile_key,
CASE WHEN lower(fact.profile_type) = ('serviceprovider') THEN SP.dimension_firm_key ELSE NULL END dimension_firm_ServiceProvider_profile_key,
CASE WHEN lower(fact.profile_type) = ('consultant') THEN IC.dimension_firm_key ELSE NULL END dimension_firm_consultant_profile_key,
COALESCE(PT.dimension_profile_type_key,'-1') AS dimension_profile_type_key,
fact.profile_type,  --to be deprecated from this model
profile_id,
profile_section,    --to be deprecated from this model
SUM(profile_view_count) AS profile_view_count
FROM {{ ref('presentation_heap_daily_profile_views') }} Fact
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
JOIN {{ ref('dimension_firm') }} GP
    ON Fact.profile_id = GP.FIRM_ID
LEFT
JOIN {{ ref('dimension_firm') }} LP
    ON Fact.profile_id = LP.FIRM_ID
LEFT
JOIN {{ ref('dimension_firm') }} SP
    ON Fact.profile_id = SP.FIRM_ID
LEFT
JOIN {{ ref('dimension_firm') }} IC
    ON Fact.profile_id = IC.FIRM_ID
LEFT
JOIN {{ ref('dimension_profile_type') }} PT
    ON Fact.profile_type = PT.profile_type
{{ dbt_utils.group_by(n=11) }}