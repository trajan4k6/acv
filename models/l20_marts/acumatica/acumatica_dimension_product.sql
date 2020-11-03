{{ config(materialized='table') }}

WITH product AS
    (
      SELECT
            {{ dbt_utils.surrogate_key(
                [4,'a.product']
            ) }}                         as dimension_product_key,
            a.product,
            a.product_type,
            4                            as datasource_id
        FROM
            {{ ref('stg_acumatica_book_of_business') }} a
        WHERE 
            a.product IS NOT NULL
        GROUP BY 
            a.product, a.product_type  
    )

SELECT
    *                                               
FROM
    product