{{ config(materialized='table') }}

WITH asset_class AS
    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [4,'a.account_classification']
            ) }}                                                                              as dimension_account_classification_key,
            a.account_classification                                                          as account_classification,
            coalesce(accountclassificationmaster.dimension_account_classification_key, '-1')  as conformed_dimension_account_classification_key,
            4                                                                                 as datasource_id
        FROM
            {{ ref('stg_acumatica_book_of_business') }} a

        LEFT JOIN {{ ref('salesforce_dimension_account_classification') }} accountclassificationmaster
            ON a.account_classification = accountclassificationmaster.account_classification

        WHERE 
            a.account_classification IS NOT NULL
        GROUP BY a.account_classification, accountclassificationmaster.dimension_account_classification_key
    )

SELECT 
    * 
FROM 
    asset_class