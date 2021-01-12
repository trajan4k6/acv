{{ config(materialized='table', tags=["firm"]) }}

with current_usd_fx_rate as (
    select * 
    from {{ ref('stg_tblcurrency_rates_history')}}
    where end_date >= getdate()
    AND base_currency_code = 'USD'
),

firm_category as (
    select *
    from {{ ref('stg_FirmType_To_FirmCategory')}}
)

/* -SB 20210111 - Reverted after discovering the logic doesnt tie in close enough with the subscription range and customer tenure calculation.
## tbluser_Subscription is not a reliable source of customer tenure due the nature in which the previous subscription_enddate is overwritten when editing a sub in the CRM.

,sub_earliest_start_date as (
SELECT
f.firm_id,
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
    f.firm_id
)
*/

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'f.FIRM_ID']                       
    ) }} AS dimension_firm_key,
    f.firm_id AS firm_id,
    NULLIF(
        firm_name,
        ''
    ) AS firm_name,
    ft.DisplayName firm_type,
    fc.firm_category,
    NVL(f.firm_status, FALSE) is_active,
    COALESCE(pf.dimension_parent_firm_key, '-2') AS dimension_parent_firm_key,
    f.Firm_funds_managed / currency_rate AS aum_usd, 
    Firm_funds_managed AS aum_local,
    Firm_funds_managed_currency local_currency,
    --sub.first_paid_subscription::date AS first_paid_subscription_date,
    1 AS Datasource_ID
FROM {{ source('preqin', 'tblFirm') }} f
LEFT
JOIN {{ source('preqin', 'tblFirm_Type') }} ft
    ON f.firm_type_id = ft.firm_type_id
LEFT
JOIN {{ ref('preqin_dimension_parent_firm') }} pf
    ON f.ParentFirm_ID = pf.parent_firm_id
LEFT
JOIN current_usd_fx_rate fx
    ON currency_code = f.Firm_funds_managed_currency
LEFT
JOIN firm_category fc
    ON ft.DisplayName = fc.firm_type
--LEFT
--JOIN sub_earliest_start_date sub
--    ON f.firm_id = sub.firm_id