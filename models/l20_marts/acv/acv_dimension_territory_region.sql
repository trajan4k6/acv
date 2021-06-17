{{ config(materialized='table') }}

WITH region AS
    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [7,'sales_region','region']
            ) }}                       as dimension_territory_region_key,
            region,
            sales_region,
            7 as datasource_id
        FROM
             {{ ref('stg_acv_values') }} a
        WHERE 
            region IS NOT NULL
        GROUP BY region,sales_region
    )

SELECT
    *
FROM
    region