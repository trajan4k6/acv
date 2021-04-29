{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT dimension_product_use_case_key, use_case
FROM {{ ref('salesforce_dimension_product_use_case') }}
