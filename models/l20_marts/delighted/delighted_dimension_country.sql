{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [6,'n.browser_country']
    ) }}                        AS dimension_country_key,
  browser_country               AS country_name,
  6                             AS datasource_id
FROM
    {{ ref('stg_delighted_net_promoter_score') }} n
WHERE 
    browser_country IS NOT NULL
GROUP BY browser_country