SELECT 
dimension_firm_key,
dimension_firm_address_key,
is_primary_address
FROM {{ ref('preqin_bridge_firm_to_address') }} fa