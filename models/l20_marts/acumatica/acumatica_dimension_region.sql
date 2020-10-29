{{ config(materialized='table') }}

WITH region AS
    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [4,'region']
            ) }}                       as dimension_region_key,
            region,
            4 as datasource_id
        FROM
            {{ source('acumatica', 'book_of_business') }} a
        WHERE 
            region IS NOT NULL
        GROUP BY region
    )

SELECT
    *
FROM
    region