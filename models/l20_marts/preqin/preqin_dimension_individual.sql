{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'cf.ContactFirm_ID']                       
    ) }} AS dimension_individual_key,
    CF.CONTACTFIRM_ID AS CONTACTFIRM_ID,
    NULLIF(contact_title,'') AS contact_title,
    NULLIF(contact_firstname,'') AS contact_firstname,
    NULLIF(contact_surname,'') AS contact_lastname,
    NULLIF(cf_Email,'') AS Email_Address,
    NULLIF(cf_LinkedIn,'') LinkedIn,
    NVL(cf.cf_Status, FALSE) Is_Active,
    COALESCE(DIMENSION_FIRM_KEY, '-1') DIMENSION_FIRM_KEY,
    1 AS Datasource_ID
FROM {{ source('preqin', 'tblContactFirm') }} CF
JOIN {{ source('preqin', 'tblContact') }} C
	ON cf.Contact_ID = c.contact_id
LEFT
JOIN {{ ref('preqin_dimension_firm') }} FIRM
    ON CF.firm_id = FIRM.FIRM_ID
