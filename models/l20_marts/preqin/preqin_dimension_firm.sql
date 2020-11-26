{{ config(materialized='table') }}

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

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'FIRM_ID']                       
    ) }} AS dimension_firm_key,
    firm_id AS firm_id,
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
