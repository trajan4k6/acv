{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT '-1' AS dimension_browser_key, NULL AS browser, NULL AS datasource_id
UNION
SELECT
dimension_browser_key,
browser,
datasource_id
FROM {{ ref('delighted_dimension_browser') }} A