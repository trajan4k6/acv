{{ config(materialized='table') }}
/*
with sub_earliest_start_date as (
SELECT
cf.contactfirm_id,
MIN(COALESCE(us.subscription_Startdate,us.Date_Entered)) AS first_paid_subscription
FROM  
	{{ source('preqin', 'tblFirm') }} f 
	JOIN {{ source('preqin', 'tblContactFirm') }} cf ON cf.firm_id = f.firm_id
	JOIN {{ source('preqin', 'tbluser_details') }} ud ON ud.ContactFirm_ID = cf.ContactFirm_ID
	JOIN {{ source('preqin', 'tbluser_Subscription') }} us ON us.user_id = ud.user_id  
	JOIN {{ source('preqin', 'tblpei_product') }} pd ON pd.product_id = us.product_id
WHERE   
	pd.product_type = 'Service'  
	AND pd.free = 0
    AND (
        NVL(pd.accesslevel,'') IN ('Standard', 'Premium', 'Academic')
    OR 
        NVL(pd.product_family, '') = 'Feeds'
    )
GROUP BY
    cf.contactfirm_id
)
*/

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'cf.ContactFirm_ID']                       
    ) }} AS dimension_individual_key,
    CF.CONTACTFIRM_ID AS CONTACTFIRM_ID,
    c.Contact_ID,
    NULLIF(c.contact_title,'') AS contact_title,
    NULLIF(c.contact_firstname,'') AS contact_firstname,
    NULLIF(c.contact_surname,'') AS contact_lastname,
    NULLIF(cf.cf_Email,'') AS Email_Address,
    NULLIF(cf.cf_LinkedIn,'') LinkedIn,
    NULLIF(cf.cf_Tel,'') Phone,
    NULLIF(cf.cf_Mob,'') Mobile,
    NVL(cf.cf_Status, FALSE) Is_Active,
    --sub.first_paid_subscription::date AS first_paid_subscription_date,
    COALESCE(DIMENSION_FIRM_KEY, '-1') DIMENSION_FIRM_KEY,
    1 AS Datasource_ID
FROM {{ source('preqin', 'tblContactFirm') }} CF
JOIN {{ source('preqin', 'tblContact') }} C
	ON cf.Contact_ID = c.contact_id
LEFT
JOIN {{ ref('preqin_dimension_firm') }} FIRM
    ON CF.firm_id = FIRM.FIRM_ID
/*LEFT
JOIN sub_earliest_start_date sub
    ON cf.contactfirm_id = sub.contactfirm_id
*/
