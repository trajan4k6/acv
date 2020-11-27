{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

with current_paid_firm AS (
    SELECT f.dimension_firm_key 
    FROM {{ ref('fact_paid_subscriber_daily')}} f
    JOIN {{ ref('dimension_date')}} d USING(Date_Key)
    WHERE 
        NVL(d.Is_Today, false) = true
    GROUP BY 1
),
firm_details AS (

SELECT '-1' AS DIMENSION_FIRM_KEY, NULL AS CRM_FIRM_ID, NULL FIRM_NAME, NULL SALESFORCE_ACCOUNT_ID, NULL AS ACCOUNT_CLASSIFICATION, NULL AS FIRM_TYPE, NULL AS REGION_NAME, NULL AS REGION_TEAM_NAME, NULL AS parent_firm_id, NULL AS  parent_firm_name, '-1' AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1' AS DIMENSION_REGION_KEY, '-1' AS DIMENSION_REGION_TEAM_KEY, NULL AS DATASOURCE_ID
UNION
--1.Primary Firm list from Core
SELECT F.DIMENSION_FIRM_KEY, TO_CHAR(F.FIRM_ID) AS CRM_FIRM_ID, F.FIRM_NAME, A.ACCOUNT_ID AS SALESFORCE_ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, F.FIRM_TYPE, AR.REGION_NAME, ART.REGION_TEAM_NAME, pf.parent_firm_id, pf.parent_firm_name, COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') DIMENSION_ACCOUNT_CLASSIFICATION_KEY, COALESCE(A.DIMENSION_REGION_KEY, '-1') DIMENSION_REGION_KEY, COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') DIMENSION_REGION_TEAM_KEY, F.DATASOURCE_ID
FROM {{ ref('preqin_dimension_firm') }} F
--JOIN TO Salesforce Account Dimension FOR Salesforce mastered attributes
LEFT
JOIN {{ ref('salesforce_dimension_account') }} A
    ON F.DIMENSION_FIRM_KEY = A.CONFORMED_DIMENSION_FIRM_KEY
LEFT
JOIN {{ ref('salesforce_dimension_account_classification') }} AC
    ON A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY = AC.DIMENSION_ACCOUNT_CLASSIFICATION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region') }} AR
    ON A.DIMENSION_REGION_KEY = AR.DIMENSION_REGION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region_team') }} ART
    ON A.DIMENSION_REGION_TEAM_KEY = ART.DIMENSION_REGION_TEAM_KEY
LEFT
JOIN {{ ref('preqin_dimension_parent_firm') }} pf
    on f.dimension_parent_firm_key = pf.dimension_parent_firm_key
--2 + any Unmapped Salesforce Accounts to Primary\Master Firm
UNION 
SELECT A.DIMENSION_ACCOUNT_KEY, TO_CHAR(A.CRM_FIRM_ID), A.ACCOUNT_NAME, A.ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, A.CRM_FIRM_TYPE, AR.REGION_NAME, ART.REGION_TEAM_NAME, NULL, NULL, A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, A.DIMENSION_REGION_KEY, A.DIMENSION_REGION_KEY, A.DATASOURCE_ID
FROM {{ ref('salesforce_dimension_account') }} A
LEFT
JOIN {{ ref('salesforce_dimension_account_classification') }} AC
    ON A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY = AC.DIMENSION_ACCOUNT_CLASSIFICATION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region') }} AR
    ON A.DIMENSION_REGION_KEY = AR.DIMENSION_REGION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region_team') }} ART
    ON A.DIMENSION_REGION_TEAM_KEY = ART.DIMENSION_REGION_TEAM_KEY

WHERE 
    CONFORMED_DIMENSION_FIRM_KEY = '-1'
--3 + any Unmapped Heap Account to Primary\Master Firm
UNION
SELECT F.DIMENSION_FIRM_KEY,TO_CHAR(F.LEGACY_FIRM_ID), F.FIRM_NAME,  F.ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, A.CRM_FIRM_TYPE, AR.REGION_NAME, ART.REGION_TEAM_NAME, NULL, NULL, COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') DIMENSION_ACCOUNT_CLASSIFICATION_KEY, COALESCE(A.DIMENSION_REGION_KEY, '-1') DIMENSION_REGION_KEY, COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') DIMENSION_REGION_TEAM_KEY, F.DATASOURCE_ID
FROM {{ ref('heap_dimension_firm_integrated') }} F
LEFT
JOIN {{ ref('salesforce_dimension_account') }}  A
    ON F.SALESFORCE_DIMENSION_ACCOUNT_KEY = A.DIMENSION_ACCOUNT_KEY
LEFT
JOIN {{ ref('salesforce_dimension_account_classification') }} AC
    ON A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY = AC.DIMENSION_ACCOUNT_CLASSIFICATION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region') }} AR
    ON A.DIMENSION_REGION_KEY = AR.DIMENSION_REGION_KEY
LEFT
JOIN {{ ref('salesforce_dimension_region_team') }} ART
    ON A.DIMENSION_REGION_TEAM_KEY = ART.DIMENSION_REGION_TEAM_KEY

WHERE 
    F.CONFORMED_DIMENSION_FIRM_KEY = '-1'
)

SELECT fd.*,
case when cpf.dimension_firm_key is null then false else true end is_paid_customer
 FROM firm_details fd
LEFT
JOIN current_paid_firm cpf 
    ON fd.dimension_firm_key = cpf.dimension_firm_key


