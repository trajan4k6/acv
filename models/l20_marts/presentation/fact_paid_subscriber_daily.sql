{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
    Fact.date_key,
    fact.dimension_individual_key,
    fact.dimension_firm_key,
    fact.dimension_product_key,
    COALESCE( p.dimension_asset_class_key, '-1' ) AS dimension_asset_class_key
FROM {{ ref('preqin_fact_paid_subscriber_daily') }} Fact
LEFT
JOIN  {{ ref('preqin_dimension_product') }} p
    ON Fact.dimension_product_key = p.dimension_product_key

UNION ALL

SELECT
    History.date_key,
    History.dimension_individual_key,
    History.dimension_firm_key,
    History.dimension_product_key,
    History.dimension_asset_class_key
FROM {{ ref('preqin_fact_paid_subscriber_daily_history') }} History
--include migrated history prior to the earliest subscription history capture on snowflake
WHERE 
    History.date_key < (select max(date_key) from {{ ref('preqin_fact_paid_subscriber_daily') }})




