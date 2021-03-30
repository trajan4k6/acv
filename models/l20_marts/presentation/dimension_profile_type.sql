{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []

) }}


SELECT '-1' AS DIMENSION_PROFILE_TYPE_KEY, NULL AS PROFILE_TYPE
UNION
SELECT DISTINCT
    {{ dbt_utils.surrogate_key(
        ['profile_type']                       
    ) }} AS dimension_profile_type_key,
profile_type
FROM {{ ref('heap_fact_app_page_viewed') }}
