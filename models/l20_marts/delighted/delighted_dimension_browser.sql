{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [6,'n.browser']
    ) }}                        AS dimension_browser_key,
  browser,
  6                             AS datasource_id
FROM
    {{ ref('stg_delighted_net_promoter_score') }} n
WHERE 
    browser IS NOT NULL
GROUP BY browser