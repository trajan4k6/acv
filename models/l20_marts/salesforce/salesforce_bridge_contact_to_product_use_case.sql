{{ config(materialized='table') }}

SELECT
cd.dimension_contact_key,
puc.dimension_product_use_case_key
FROM (SELECT id, value FROM {{ source('salesforce', 'contact') }} , TABLE(split_to_table(Use_Case_c, ';')) c) use_case
JOIN {{ ref('salesforce_dimension_product_use_case')}} puc
    ON use_case.value = puc.use_case
JOIN {{ ref('salesforce_dimension_contact')}} cd
    ON use_case.id = cd.contact_id