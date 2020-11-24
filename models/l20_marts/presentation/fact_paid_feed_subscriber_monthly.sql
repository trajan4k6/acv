{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
    fct.date_key,
    fct.dimension_firm_key,
    fct.dimension_product_key,
    fct.dimension_asset_class_key
FROM {{ ref('fact_paid_subscriber_daily') }} fct
JOIN {{ ref('dimension_date') }} d
    ON fct.date_key = d.date_key
WHERE
    is_last_day_of_month = TRUE 
GROUP BY 1, 2, 3, 4
