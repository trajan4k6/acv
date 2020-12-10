SELECT
f.dimension_firm_key,
fa.dimension_firm_address_key,
stg.is_primary_address
FROM {{ref('stg_tblfirm_address')}} stg
JOIN {{ ref('preqin_dimension_firm_address')}} fa
    ON stg.firm_address_id = fa.firm_address_id
JOIN {{ ref('preqin_dimension_firm')}} f
    ON stg.firm_id = f.firm_id
