SELECT 
conformed_dimension_firm_key as dimension_firm_key,
dimension_competitor_key
FROM {{ ref('salesforce_dimension_competitor') }} c