{{ config(materialized='table', tags=["firm"]) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'ParentFirm_ID']                       
    ) }} AS dimension_parent_firm_key,
    ParentFirm_ID AS parent_firm_id,
    NULLIF(
        ParentFirm_Name,
        ''
    ) AS Parent_Firm_Name,
    ft.DisplayName Parent_Firm_Type
FROM {{ source('preqin', 'tblParentFirm') }} pf
LEFT
JOIN {{ source('preqin', 'tblFirm_Type') }} ft
ON pf.firm_type_id = ft.firm_type_id
