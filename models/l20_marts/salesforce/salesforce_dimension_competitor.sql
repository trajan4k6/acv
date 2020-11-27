{{ config(materialized='table') }}

with cte_active_competitor_records AS (
    SELECT * FROM
    {{ ref('stg_competitor')}}
    WHERE 
        isdeleted = false
)

SELECT 
     {{ dbt_utils.surrogate_key(
        ['c.ID']                       
    ) }} AS dimension_competitor_key,
c.ID as competitor_entry_id,
c.name as competitor_entry_name,
c.competitor_c as competitor_company_name,
c.competitor_product_services_c as competitor_product_list,
c.subscription_expiry_c as subscripion_expiry_date,
c.createddate as created_date,
COALESCE(a.DIMENSION_ACCOUNT_KEY, '-1') AS DIMENSION_ACCOUNT_KEY,
COALESCE(A.CONFORMED_DIMENSION_FIRM_KEY, '-1') AS CONFORMED_DIMENSION_FIRM_KEY
FROM
    cte_active_competitor_records c
LEFT
JOIN {{ ref('salesforce_dimension_account') }} a
    ON c.account_id = a.account_id
