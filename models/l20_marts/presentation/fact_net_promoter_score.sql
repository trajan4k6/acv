{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
feedback_id,
date_created_key,
dimension_browser_key,
dimension_operating_system_key,
dimension_country_key,
conformed_dimension_individual_key  AS dimension_individual_key,
conformed_dimension_firm_key        AS dimension_firm_key,
feedback_score,
feedback_comment,
feedback_additional_response,
feedback_url,
datasource_id
FROM 
    {{ ref('delighted_fact_net_promoter_score') }}