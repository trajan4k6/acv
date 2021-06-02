{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

with final as (
   SELECT 
       CAST(YEAR((A.AS_AT_DATE::DATE)) || RIGHT('0' || MONTH((A.AS_AT_DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((A.AS_AT_DATE::DATE)), 2) AS INT) 				AS DATE_KEY,
	   CASE WHEN AC.CONFORMED_DIMENSION_ASSET_CLASS_KEY = '-1' THEN AC.DIMENSION_ASSET_CLASS_KEY ELSE COALESCE(AC.CONFORMED_DIMENSION_ASSET_CLASS_KEY,'-1') END 					 				AS DIMENSION_ASSET_CLASS_KEY,
	   COALESCE(R.DIMENSION_REGION_KEY,'-1') 																																		 				AS ACUMATICA_DIMENSION_REGION_KEY,
	   COALESCE(P.DIMENSION_PRODUCT_KEY,'-1')    																																	 				AS ACUMATICA_DIMENSION_PRODUCT_KEY,
       CASE WHEN F.CONFORMED_DIMENSION_FIRM_KEY = '-1' THEN F.DIMENSION_FIRM_KEY ELSE COALESCE(F.CONFORMED_DIMENSION_FIRM_KEY,'-1') END AS DIMENSION_FIRM_KEY,
       A.ROW_TYPE,
       S.ACV_GBP
    FROM 
             {{ref('acv_growth_levers')}} A
    JOIN     {{ref('stg_acv_values')}} S
                ON A.FIRM_ID=S.FIRM_ID AND A.LOOKUP_ID=S.LOOKUP_ID AND NVL(A.FIRM_TIER,'')=NVL(S.FIRM_TIER,'') AND NVL(A.FIRM_CATEGORY,'') = NVL(S.FIRM_CATEGORY,'' )
                    AND A.INVENTORY_DESCRIPTION=S.INVENTORY_DESCRIPTION AND A.ASSET_CLASS=S.ASSET_CLASS AND A.PRODUCT_TYPE=S.PRODUCT_TYPE
                    AND A.AS_AT_DATE=S.AS_AT_DATE AND A.REGION=S.REGION AND A.CURRENCY=S.CURRENCY AND NVL(A.START_DATE,'1900-01-01') = NVL(S.START_DATE,'1900-01-01') AND NVL(A.END_DATE,'2099-12-31')=NVL(S.END_DATE,'2099-12-31')
    
    LEFT JOIN {{ ref('acumatica_dimension_asset_class') }}  AC
        ON A.ASSET_CLASS = AC.ASSET_CLASS_NAME
    
    LEFT JOIN {{ ref('acumatica_dimension_region') }}  R
        ON A.REGION = R.REGION

    LEFT JOIN {{ ref('acumatica_dimension_product') }}  P
        ON A.INVENTORY_DESCRIPTION = P.PRODUCT

    LEFT JOIN {{ ref('acumatica_dimension_firm') }} F
        ON A.FIRM_ID = F.FIRM_ID
)

select * from final
