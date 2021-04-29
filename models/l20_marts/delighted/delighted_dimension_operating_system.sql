{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [6,'n.os']
    ) }}                        AS dimension_operating_system_key,
  os                            AS operating_system,
  6                             AS datasource_id
FROM
    {{ ref('stg_delighted_net_promoter_score') }} n
WHERE 
    os IS NOT NULL
GROUP BY os