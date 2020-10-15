{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [2,'CLASSIFICATION_C']
    ) }}                           AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY,
  CLASSIFICATION_C                 AS ACCOUNT_CLASSIFICATION,
  2 AS DATASOURCE_ID
FROM
    {{ source('acumatica', 'account') }} a
WHERE 
    CLASSIFICATION_C IS NOT NULL
GROUP BY CLASSIFICATION_C