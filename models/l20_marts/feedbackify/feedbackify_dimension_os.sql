{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [5,'n.OS']
    ) }}                        AS DIMENSION_OS_KEY,
  OS,
  5                             AS DATASOURCE_ID
FROM
    {{ source('feedbackify', 'net_promoter_score') }} n
WHERE 
    OS IS NOT NULL
GROUP BY OS