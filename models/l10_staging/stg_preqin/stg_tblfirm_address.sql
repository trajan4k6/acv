SELECT 
    firm_address_id,
    firm_id,
    NULLIF( firm_address_1, '' ) AS address_line_1,
    NULLIF( firm_address_2, '' ) AS address_line_2,
    NULLIF( firm_city, '' ) AS city,
    NULLIF( firm_state, '' ) AS state,
    NULLIF( firm_country, '' ) AS country,
    NULLIF( firm_zip_code, '' ) AS postal_code,
    NVL(Main_Address, 0) AS Is_Primary_Address

FROM  {{ source('preqin', 'tblFirm_Address') }}

WHERE __DELETED = 'FALSE' OR __DELETED IS NULL