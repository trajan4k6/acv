SELECT
    {{ dbt_utils.surrogate_key(
        ['firm_address_id']                       
    ) }} AS dimension_firm_address_key,
    firm_address_id,
    address_line_1,
    address_line_2,
    city,
    state,
    postal_code,
    country
FROM {{ ref('stg_tblfirm_address') }}