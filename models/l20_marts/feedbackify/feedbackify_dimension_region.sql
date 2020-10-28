{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [5,'n.LOCATION']
    ) }}                        AS DIMENSION_REGION_KEY,
  LOCATION                      AS REGION_NAME,
  5                             AS DATASOURCE_ID
FROM
    {{ source('feedbackify', 'net_promoter_score') }} n
WHERE 
    LOCATION IS NOT NULL
GROUP BY LOCATION