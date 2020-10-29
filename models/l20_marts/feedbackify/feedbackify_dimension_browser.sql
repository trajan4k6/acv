{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [5,'n.BROWSER']
    ) }}                        AS DIMENSION_BROWSER_KEY,
  BROWSER,
  SPLIT_PART(BROWSER,' ',1)     AS BROWSER_TYPE,
  5                             AS DATASOURCE_ID
FROM
    {{ source('feedbackify', 'net_promoter_score') }} n
WHERE 
    BROWSER IS NOT NULL
GROUP BY BROWSER