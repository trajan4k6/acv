{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'Product_Id']                       
    ) }} AS dimension_product_key,
 Product_Id
,Product_Description AS Product_Name
,Product_description_short AS Product_Short_Name
,Product_Type
,ProductFamilyId AS Product_Family_Id
,Product_Family AS Product_Family_Name
,AccessLevel AS Access_Level
,Free AS Is_Free
,COALESCE(AC.dimension_asset_class_key, '-2')  AS DIMENSION_ASSET_CLASS_KEY ---2 = N/A
FROM {{ source('preqin', 'tblpei_product') }} p
LEFT
JOIN {{ ref('preqin_dimension_asset_class') }} ac
    ON p.Asset_Class = ac.Asset_Class_Short_Name
