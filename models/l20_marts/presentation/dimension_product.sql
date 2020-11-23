{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS DIMENSION_PRODUCT_KEY, NULL AS PRODUCT_ID,  NULL AS PRODUCT_NAME, NULL AS PRODUCT_SHORT_NAME, NULL AS PRODUCT_TYPE, NULL AS PRODUCT_FAMILY_ID, NULL AS PRODUCT_FAMILY_NAME, NULL AS ACCESS_LEVEL, NULL AS IS_FREE, '-1' AS DIMENSION_ASSET_CLASS_KEY,  NULL AS DATASOURCE_ID
UNION
SELECT 
DIMENSION_PRODUCT_KEY,
PRODUCT_ID,
PRODUCT_NAME,
PRODUCT_SHORT_NAME,
PRODUCT_TYPE,
PRODUCT_FAMILY_ID,
PRODUCT_FAMILY_NAME,
ACCESS_LEVEL,
IS_FREE,
DIMENSION_ASSET_CLASS_KEY,
DATASOURCE_ID
FROM {{ ref('preqin_dimension_product') }} P