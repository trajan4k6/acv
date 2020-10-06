{{ config(
    materialized = 'view',
    tags = ["firm"]
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [3,'firm.account_id']
    ) }} AS dimension_firm_key,
    /*  {{ dbt_utils.star(
            from = ref('heap_dimension_firm'),
            except = []
        ) }},
    */
    firm.*,
    COALESCE(
        salesforcefirmmaster.conformed_dimension_key,
        NULL
    ) AS conformed_dimension_key,
    salesforcefirmmaster.dimension_account_key AS salesforce_dimension_account_key,
    3 AS datasource_id
FROM
    {{ ref('heap_dimension_firm') }}
    firm
    LEFT JOIN {{ ref('salesforce_dimension_account') }}
    salesforcefirmmaster
    ON firm.account_id = salesforcefirmmaster.account_id
