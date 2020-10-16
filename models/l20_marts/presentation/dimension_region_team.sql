{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS DIMENSION_REGION_TEAM_KEY, NULL AS REGION_TEAM_NAME
UNION
SELECT RT.DIMENSION_REGION_TEAM_KEY, REGION_TEAM_NAME
FROM {{ ref('salesforce_dimension_region_team') }} RT

/*
No additional sources currently defined
*/
