{{
    config(
        materialized='incremental'
    ) 
}}

SELECT PL.PAGELOG_ID, 
CAST(YEAR((PL.LOG_DATE::DATE)) || RIGHT('0' || MONTH((PL.LOG_DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((PL.LOG_DATE::DATE)), 2) AS INT) AS DATE_KEY,
COALESCE(FIRM.dimension_firm_key, '-1')  AS DIMENSION_FIRM_KEY,
COALESCE(AC.dimension_asset_class_key, '-2')  AS DIMENSION_ASSET_CLASS_KEY ---2 = N/A
FROM {{ source('preqin', 'STG_Data_Delivery_Data_Feeds_PageLogs') }} PL
LEFT
JOIN {{ ref('preqin_dimension_firm') }} FIRM
    ON PL.firm_id = FIRM.FIRM_ID
LEFT
JOIN {{ ref('preqin_dimension_asset_class') }} AC
    ON  UPPER((split_part(PL.PAGE_NAME, '/', 3))) = AC.Asset_Class_Short_Name
        OR      
        UPPER((split_part(PL.PAGE_NAME, '/', 4))) = AC.Asset_Class_Short_Name

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where PAGELOG_ID > (select max(PAGELOG_ID) from {{ this }})

{% endif %}
