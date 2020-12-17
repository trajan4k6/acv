{{ config(
    materialized = 'view',
    unique_key = [],
    tags = ["firm"]
) }}

with current_paid_firm AS (
    SELECT  ps.dimension_firm_key, f.dimension_parent_firm_key
    FROM {{ ref('fact_paid_subscriber_daily')}} ps
      JOIN {{ ref('dimension_date')}} d USING(Date_Key)
    JOIN  {{ ref('preqin_dimension_firm') }} f USING (dimension_firm_key)
    WHERE 
        NVL(d.Is_Today, false) = true
    GROUP BY 1,2
),
--Parent Logo: where at least one firm under a parent has a paid subscription
current_paid_subsidiary_firm AS (
    SELECT  f.dimension_firm_key
    FROM current_paid_firm
    JOIN  {{ ref('preqin_dimension_firm') }} f USING (dimension_parent_firm_key)
    GROUP BY 1
),

firm_office_count AS (
    SELECT dimension_firm_key, 
    count(1) AS Count_Of_Offices 
    FROM {{ ref('bridge_firm_to_address')}} f2a
    GROUP BY 1
),

primary_firm_address AS (
    SELECT  f2a.dimension_firm_key, 
            fa.*
    FROM {{ ref('bridge_firm_to_address')}} f2a
    JOIN {{ ref('dimension_firm_address')}} fa
        ON f2a.dimension_firm_address_key = fa.dimension_firm_address_key
    WHERE 
        f2a.is_primary_address = true
),

firm_aum as (
SELECT dimension_firm_key, aum_usd
FROM {{ ref('preqin_dimension_firm') }}
)
,

conformed_firm_details AS (
SELECT '-1' AS DIMENSION_FIRM_KEY, NULL AS FIRM_ID, NULL FIRM_NAME, NULL SALESFORCE_ACCOUNT_ID, NULL AS ACCOUNT_CLASSIFICATION, NULL AS FIRM_TYPE, NULL AS FIRM_CATEGORY, NULL AS REGION_NAME, NULL AS REGION_TEAM_NAME, NULL AS parent_firm_id, NULL AS  parent_firm_name, NULL AS parent_firm_type, NULL as is_active,  '-1' AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1' AS DIMENSION_REGION_KEY, '-1' AS DIMENSION_REGION_TEAM_KEY, NULL AS DATASOURCE_ID
UNION
--1.Primary Firm list from Core
SELECT F.DIMENSION_FIRM_KEY, TO_CHAR(F.FIRM_ID) AS FIRM_ID, F.FIRM_NAME, A.ACCOUNT_ID AS SALESFORCE_ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, F.FIRM_TYPE, F.FIRM_CATEGORY, AR.REGION_NAME, ART.REGION_TEAM_NAME, pf.parent_firm_id, pf.parent_firm_name, pf.parent_firm_type, f.is_active, COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') DIMENSION_ACCOUNT_CLASSIFICATION_KEY, COALESCE(A.DIMENSION_REGION_KEY, '-1') DIMENSION_REGION_KEY, COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') DIMENSION_REGION_TEAM_KEY, F.DATASOURCE_ID
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
SELECT A.DIMENSION_ACCOUNT_KEY, TO_CHAR(A.CRM_FIRM_ID), A.ACCOUNT_NAME, A.ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, A.CRM_FIRM_TYPE, NULL AS FIRM_CATEGORY, AR.REGION_NAME, ART.REGION_TEAM_NAME, NULL, NULL, NULL, CASE A.Account_Status WHEN 'Active' THEN true WHEN 'Inactive' THEN false END Is_Active, A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, A.DIMENSION_REGION_KEY, A.DIMENSION_REGION_KEY, A.DATASOURCE_ID
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
SELECT F.DIMENSION_FIRM_KEY,TO_CHAR(F.LEGACY_FIRM_ID), F.FIRM_NAME,  F.ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, A.CRM_FIRM_TYPE, NULL AS FIRM_CATEGORY, AR.REGION_NAME, ART.REGION_TEAM_NAME, NULL, NULL, NULL, CASE A.Account_Status WHEN 'Active' THEN true WHEN 'Inactive' THEN false END Is_Active, COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') DIMENSION_ACCOUNT_CLASSIFICATION_KEY, COALESCE(A.DIMENSION_REGION_KEY, '-1') DIMENSION_REGION_KEY, COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') DIMENSION_REGION_TEAM_KEY, F.DATASOURCE_ID
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
--4 + any Unmapped Acumatica Firms to Primary\Master Firm
UNION 
SELECT F.DIMENSION_FIRM_KEY, TO_CHAR(F.FIRM_ID) AS FIRM_ID, F.FIRM_NAME, A.ACCOUNT_ID AS SALESFORCE_ACCOUNT_ID, AC.ACCOUNT_CLASSIFICATION, F.FIRM_TYPE, F.FIRM_CATEGORY, AR.REGION_NAME, ART.REGION_TEAM_NAME, NULL, NULL, NULL, f.is_active, COALESCE(A.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') DIMENSION_ACCOUNT_CLASSIFICATION_KEY, COALESCE(A.DIMENSION_REGION_KEY, '-1') DIMENSION_REGION_KEY, COALESCE(A.DIMENSION_REGION_TEAM_KEY, '-1') DIMENSION_REGION_TEAM_KEY, F.DATASOURCE_ID
FROM {{ ref('acumatica_dimension_firm') }} F
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
)

SELECT
fd.DIMENSION_FIRM_KEY,
fd.FIRM_ID,
fd.FIRM_NAME,
fd.SALESFORCE_ACCOUNT_ID,
fd.ACCOUNT_CLASSIFICATION,
fd.FIRM_TYPE,
fd.FIRM_CATEGORY,
fd.REGION_NAME,
fd.REGION_TEAM_NAME,
fd.parent_firm_id,
fd.parent_firm_name,
fd.parent_firm_type,
fd.Is_Active,
case when cpf.dimension_firm_key is not null then true else false end is_firm_logo,
case when is_firm_logo = true or cpsf.dimension_firm_key is not null then true else false end is_parent_logo,
NVL(foc.Count_Of_Offices, 0) AS Number_of_Offices,
--Primary address details
fa.address_line_1,
fa.address_line_2,
fa.city,
fa.state,
fa.postal_code,
fa.country,
--AUM - assets under management (mn)
faum.aum_usd,
--dim keys
fd.DIMENSION_ACCOUNT_CLASSIFICATION_KEY,
fd.DIMENSION_REGION_KEY,
fd.DIMENSION_REGION_TEAM_KEY,
fd.DATASOURCE_ID
FROM conformed_firm_details fd
LEFT
JOIN current_paid_firm cpf
    ON fd.dimension_firm_key = cpf.dimension_firm_key

LEFT
JOIN current_paid_subsidiary_firm cpsf
    ON fd.dimension_firm_key = cpsf.dimension_firm_key

LEFT
JOIN firm_office_count foc
    ON fd.dimension_firm_key = foc.dimension_firm_key

LEFT
JOIN primary_firm_address fa
    ON fd.dimension_firm_key = fa.dimension_firm_key

LEFT
JOIN firm_aum faum
    ON fd.dimension_firm_key = faum.dimension_firm_key

