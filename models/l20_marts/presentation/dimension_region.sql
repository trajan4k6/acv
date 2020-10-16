{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS DIMENSION_REGION_KEY, NULL AS REGION_NAME, NULL AS DATASOURCE_ID
UNION
--1.Primary Region list from Salesforce
SELECT R.DIMENSION_REGION_KEY, REGION_NAME,  R.DATASOURCE_ID
FROM {{ ref('salesforce_dimension_region') }} R
