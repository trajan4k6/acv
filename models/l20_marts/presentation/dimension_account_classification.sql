{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}


SELECT '-1' AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY, NULL AS ACCOUNT_CLASSIFICATION, NULL AS DATASOURCE_ID
UNION
--1.Primary Account Classification list from Salesforce
SELECT AC.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, ACCOUNT_CLASSIFICATION,  AC.DATASOURCE_ID
FROM {{ ref('salesforce_dimension_account_classification') }} AC
