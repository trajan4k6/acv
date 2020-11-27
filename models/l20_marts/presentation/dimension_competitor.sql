SELECT 
dimension_competitor_key,
competitor_entry_id, 
competitor_entry_name,
competitor_company_name,
competitor_product_list,
subscripion_expiry_date,
created_date
FROM {{ ref('salesforce_dimension_competitor')}}
