{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [3,'firm.account_id']
    ) }} AS dimension_firm_key,
    /*{{ dbt_utils.star(
            from = ref('heap_dimension_firm'),
            except = []
        ) }},*/
    firm.*, 
    COALESCE(salesforceAccount.conformed_dimension_firm_key, '-1')  AS conformed_dimension_firm_key,
    COALESCE(salesforceAccount.dimension_account_key, '-1')         AS salesforce_dimension_account_key,
    3 AS datasource_id
FROM
    {{ ref('heap_dimension_firm') }} firm
    LEFT 
    JOIN {{ ref('salesforce_dimension_account') }} salesforceAccount
        ON firm.account_id = salesforceAccount.account_id
