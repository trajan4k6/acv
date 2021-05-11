{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'cf.ContactFirm_ID']                       
    ) }} AS dimension_individual_key,
    CF.CONTACTFIRM_ID AS CONTACTFIRM_ID,
    c.Contact_ID,
    NULLIF(c.contact_title,'') AS contact_title,
    NULLIF(c.contact_firstname,'') AS contact_firstname,
    NULLIF(c.contact_surname,'') AS contact_lastname,
    COALESCE(cf.cf_email, u.user_email,'') email,
    NULLIF(cf.cf_LinkedIn,'') LinkedIn,
    NULLIF(cf.cf_Tel,'') Phone,
    NULLIF(cf.cf_Mob,'') Mobile,
    NULLIF(cf.cf_JobTitle,'') Job_Title,
    NVL(cf.cf_Status, FALSE) Is_Active,
    NULLIF(fa.Country,'') as Contact_Country,
    COALESCE(DIMENSION_FIRM_KEY, '-1') DIMENSION_FIRM_KEY,
    1 AS Datasource_ID
FROM {{ source('preqin', 'tblContactFirm') }} CF
JOIN {{ source('preqin', 'tblContact') }} C
	ON cf.Contact_ID = c.contact_id
LEFT
JOIN {{ ref('preqin_dimension_firm') }} FIRM
    ON CF.firm_id = FIRM.FIRM_ID
LEFT JOIN {{ ref('stg_tbluser_details') }} u 
    ON us.user_id = u.user_id
LEFT JOIN {{ ref('stg_tblfirm_address') }} fa 
    ON cf.firm_Address_ID = fa.firm_Address_ID