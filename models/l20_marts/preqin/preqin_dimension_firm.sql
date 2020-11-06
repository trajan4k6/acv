{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'FIRM_ID']                       
    ) }} AS dimension_firm_key,
    firm_id AS firm_id,
    NULLIF(
        firm_name,
        ''
    ) AS firm_name,
    ft.DisplayName Firm_Type,
    NVL(f.firm_status, FALSE) Is_Active,
    COALESCE(pf.dimension_parent_firm_key, '-2') AS dimension_parent_firm_key,
    1 AS Datasource_ID
FROM {{ source('preqin', 'tblFirm') }} f
LEFT
JOIN {{ source('preqin', 'tblFirm_Type') }} ft
    ON f.firm_type_id = ft.firm_type_id
LEFT
JOIN {{ ref('preqin_dimension_parent_firm') }} pf
    ON f.ParentFirm_ID = pf.parent_firm_id
