{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['value']
    ) }} AS dimension_product_use_case_key,
  value AS use_case
FROM 
{{ source('salesforce', 'contact') }} , TABLE(split_to_table(Use_Case_c, ';'))
GROUP BY value