{{ config(materialized='table') }}

WITH product AS
    (
      SELECT
            {{ dbt_utils.surrogate_key(
                [7,'a.inventory_description']
            ) }}                         as dimension_inventory_description_key,
            a.inventory_description,
            a.product_type,
            7                            as datasource_id
        FROM
            {{ ref('stg_acv_values') }} a
        WHERE 
            a.inventory_description IS NOT NULL
        GROUP BY 
            a.inventory_description, a.product_type  
    )

SELECT
    *                                               
FROM
    product