{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
    Fact.*,
    COALESCE( p.dimension_asset_class_key, '-1' ) AS dimension_asset_class_key
FROM {{ ref('preqin_fact_paid_subscriber_daily') }} Fact
LEFT
JOIN  {{ ref('preqin_dimension_product') }} p
    ON Fact.dimension_product_key = p.dimension_product_key

