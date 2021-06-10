{{ config(materialized='table') }}

WITH region AS
    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [7,'region']
            ) }}                       as dimension_territory_region_key,
            region,
            7 as datasource_id
        FROM
             {{ ref('stg_acv_values') }} a
        WHERE 
            region IS NOT NULL
        GROUP BY region
    )

SELECT
    *
FROM
    region