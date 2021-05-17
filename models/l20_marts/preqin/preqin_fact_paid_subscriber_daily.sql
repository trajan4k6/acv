{{
    config(
        materialized='incremental',
        cluster_by=['date_key']
    ) 
}}

with cte_order (

    select ORDERID, COLLATE(regexp_substr(NOTES, '\\SF\\sID\\W+(\\w+)', 1, 1, 'ime', 1),'upper') as "OPPORTUNITYID" 
    FROM {{ ref('stg_tblorders') }}
    WHERE regexp_substr(NOTES, '\\SF\\sID\\W+(\\w+)', 1, 1, 'ime', 1) IS NOT NULL

)
SELECT DISTINCT
CAST(YEAR((GETDATE())) || RIGHT('0' || MONTH((GETDATE())), 2) || RIGHT('0' || DAYOFMONTH((GETDATE())), 2) AS INT) AS DATE_KEY,
COALESCE(ind.dimension_individual_key, '-1')  AS DIMENSION_INDIVIDUAL_KEY,
COALESCE(firm.dimension_firm_key, '-1')  AS DIMENSION_FIRM_KEY,
COALESCE(prod.dimension_product_key, '-1')  AS DIMENSION_PRODUCT_KEY
COALESCE(sds.DIMENSION_SUBSCRIPTION_KEY, '-1') AS DIMENSION_SUBSCRIPTION_KEY
FROM {{ ref('stg_tbluser_Subscription') }} us
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
    ON p.product_id = prod.product_id
LEFT JOIN cte_order o ON us.orderid = o.orderid 
LEFT JOIN {{ ref('salesforce_dimension_subscription') }} sds ON o.opportunityID = sds.opportunityID

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
