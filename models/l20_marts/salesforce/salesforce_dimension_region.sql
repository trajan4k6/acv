{{ config(materialized='table', tags=["firm"]) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [2,'REGION_C']
    ) }} AS DIMENSION_REGION_KEY,
  REGION_C AS REGION_NAME,
  2 AS DATASOURCE_ID
FROM
    {{ source('salesforce', 'account') }} a
WHERE 
    REGION_C IS NOT NULL
GROUP BY REGION_C