{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
datasource_id,
datasource_name,
datasource_desc
FROM {{ ref('DataSource')}}