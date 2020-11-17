{{
    config(
        materialized='incremental'
    ) 
}}

SELECT DISTINCT
CAST(YEAR((GETDATE())) || RIGHT('0' || MONTH((GETDATE())), 2) || RIGHT('0' || DAYOFMONTH((GETDATE())), 2) AS INT) AS DATE_KEY,
COALESCE(ind.dimension_individual_key, '-1')  AS DIMENSION_INDIVIDUAL_KEY,
COALESCE(firm.dimension_firm_key, '-1')  AS DIMENSION_FIRM_KEY,
COALESCE(prod.dimension_product_key, '-1')  AS DIMENSION_PRODUCT_KEY
FROM {{ source('preqin', 'tbluser_Subscription') }} us
JOIN {{ source('preqin', 'tbluser_details') }} ud
    ON us.user_id = ud.user_id
JOIN {{ source('preqin', 'tblContactFirm') }} cf
    ON ud.contactfirm_id =cf.contactfirm_id
JOIN {{ source('preqin', 'tblContact') }} c
    ON cf.contact_id = c.contact_id
JOIN {{ source('preqin', 'tblpei_product') }} p
    ON us.product_id = p.product_id
LEFT
JOIN {{ ref('preqin_dimension_individual') }} ind
    ON cf.contactfirm_id = ind.CONTACTFIRM_ID
LEFT
JOIN {{ ref('preqin_dimension_firm') }} firm
    ON cf.firm_id = firm.FIRM_ID
LEFT
JOIN {{ ref('preqin_dimension_product') }} prod
    ON us.product_id = p.product_id

WHERE
    p.free = 0
AND p.product_type = 'Service'
AND us.subscription_status = 2
AND us.subscription_expiry_date >= GETDATE()
AND cf.firm_id <> 952
AND c.contact_status = 1
AND cf.cf_status = 1
AND (
    NVL(p.accesslevel,'') IN ('Standard', 'Premium', 'Academic')
OR 
    NVL(product_family, '') = 'Feeds'
)

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and DATE_KEY > (select max(DATE_KEY) from {{ this }})

{% endif %}
