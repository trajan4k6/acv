WITH cte_bridge_contact_account_to_product_use_case AS (

SELECT  
        ca.dimension_contact_account_key,
        ca.conformed_dimension_individual_key,
        c2puc.dimension_product_use_case_key

FROM {{ ref('salesforce_bridge_contact_to_product_use_case')}} c2puc

JOIN {{ ref('salesforce_bridge_contact_to_contact_account')}} c2ca
    ON c2puc.dimension_contact_key = c2ca.dimension_contact_key

JOIN {{ ref('salesforce_dimension_contact_account')}} ca
    ON c2ca.dimension_contact_account_key = ca.dimension_contact_account_key
)

SELECT 
conformed_dimension_individual_key AS dimension_individual_key,
dimension_product_use_case_key
FROM cte_bridge_contact_account_to_product_use_case b
WHERE 
    conformed_dimension_individual_key <> '-1'

UNION

SELECT 
dimension_contact_account_key AS dimension_individual_key,
dimension_product_use_case_key
FROM cte_bridge_contact_account_to_product_use_case b
WHERE 
    conformed_dimension_individual_key = '-1'
