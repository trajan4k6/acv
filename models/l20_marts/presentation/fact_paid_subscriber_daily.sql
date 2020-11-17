{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
    Fact.*,
    1 AS SUBS_COUNT
FROM {{ ref('preqin_fact_paid_subscriber_daily') }} Fact

