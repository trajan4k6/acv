{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT '-1' AS dimension_country_key, NULL AS country_name, NULL AS datasource_id
UNION
SELECT
dimension_country_key,
country_name,
datasource_id
FROM {{ ref('delighted_dimension_country') }} A