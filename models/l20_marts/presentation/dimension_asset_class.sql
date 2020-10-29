{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS DIMENSION_ASSET_CLASS_KEY, NULL AS Asset_Class_Short_Name, NULL AS Asset_Class_Name, NULL AS DATASOURCE_ID
UNION
SELECT '-2' AS DIMENSION_ASSET_CLASS_KEY, 'N/A' AS Asset_Class_Short_Name, 'Not Applicable' AS Asset_Class_Name, NULL AS DATASOURCE_ID
UNION
--1.Primary asset class list from Core
SELECT AC.DIMENSION_ASSET_CLASS_KEY, AC.Asset_Class_Short_Name, AC.Asset_Class_Name,  AC.DATASOURCE_ID
<<<<<<< HEAD
FROM {{ ref('preqin_dimension_asset_class') }} AC
=======
FROM {{ ref('preqin_dimension_asset_class') }} AC

UNION
--4. + Unmapped Acumatica Asset Class list to Primary / Master.
SELECT A.DIMENSION_ASSET_CLASS_KEY, A.ASSET_CLASS_SHORT_NAME, A.ASSET_CLASS_NAME, A.DATASOURCE_ID
FROM {{ ref('acumatica_dimension_asset_class') }} A
WHERE CONFORMED_DIMENSION_ASSET_CLASS_KEY = '-1'
>>>>>>> dbt_gcarlin_mvp_161020
