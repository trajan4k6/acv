{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT '-1' AS dimension_operating_system_key, NULL AS operating_system, NULL AS datasource_id
UNION
SELECT
dimension_operating_system_key,
operating_system,
datasource_id
FROM {{ ref('delighted_dimension_operating_system') }} A