{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['LEAD_TEAM_C']
    ) }} AS DIMENSION_REGION_TEAM_KEY,
  LEAD_TEAM_C AS REGION_TEAM_NAME
FROM
    {{ source('acumatica', 'account') }} a
WHERE 
    LEAD_TEAM_C IS NOT NULL
GROUP BY LEAD_TEAM_C