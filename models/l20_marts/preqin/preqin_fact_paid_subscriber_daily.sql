{{ config(materialized='table') }}

SELECT 
cf.contactfirm_id,
cf.firm_id,
us.product_id
FROM {{ source('preqin', 'tbluser_Subscription') }} us
JOIN {{ source('preqin', 'tbluser_details') }} ud
    ON us.user_id = ud.user_id
JOIN {{ source('preqin', 'tblContactFirm') }} cf
    ON ud.contactfirm_id =cf.contactfirm_id
JOIN {{ source('preqin', 'tblContact') }} ct
    ON ct.contact_id = cf.contact_id
/*LEFT
JOIN {{ ref('preqin_dimension_product') }} p
    ON us.product_id = p.product_id
*/
JOIN {{ source('preqin', 'tblpei_product') }} p
    ON us.product_id = p.product_id
WHERE
    p.free = 0
AND p.product_type = 'Service'
AND us.subscription_status = 2
AND us.subscription_expiry_date >= GETDATE()
AND cf.firm_id <> 952
AND ct.contact_status = 1
AND cf.cf_status = 1
AND (
    NVL(p.accesslevel,'') IN ('Standard', 'Premium', 'Academic')
OR 
    NVL(product_family, '') = 'Feeds'
)
