{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS dimension_parent_firm_key, NULL AS parent_firm_id,  NULL AS Parent_Firm_Name, NULL AS Parent_Firm_Type
UNION
SELECT '-2' AS dimension_parent_firm_key, NULL AS parent_firm_id,  'N/A' AS Parent_Firm_Name, NULL AS Parent_Firm_Type
UNION
SELECT 
dimension_parent_firm_key,
parent_firm_id,
Parent_Firm_Name,
Parent_Firm_Type
FROM {{ ref('preqin_dimension_parent_firm') }} pf