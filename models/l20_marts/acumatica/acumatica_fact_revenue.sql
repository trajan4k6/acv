{{ config(materialized='table') }}

WITH revenue AS (

    SELECT 
       case when a.account_classification = 'Core' then a.invoice_no||a.product
            when a.account_classification = 'Enterprise' then a.invoice_no||a.asset_class
        end as core_id,
       invoice_no,
       implied_value,
       cast(year((a.management_start_date::date)) || right('0' || month((a.management_start_date::date)), 2) || right('0' || dayofmonth((a.management_start_date::date)), 2) as int) as date_key,
       coalesce(ac.dimension_asset_class_key,'-1')    																																 as dimension_asset_class_key,
       coalesce(r.dimension_region_key,'-1')    																																	 as dimension_region_key,
       coalesce(p.dimension_product_key,'-1')    																																	 as dimension_product_key,
       coalesce(dac.dimension_account_classification_key,'-1')    																													 as dimension_account_classification_key,
       --Commented out 28-10-2020 as not needed for MVP currently.
       --coalesce(f.dimension_firm_key,'-1')    																																		 as dimension_firm_key,
       --coalesce(pf.dimension_parent_firm_key,'-1')    																																 as dimension_parent_firm_key  
    FROM 
        {{ ref('stg_acumatica_book_of_business') }} a
    
    LEFT JOIN {{ ref('acumatica_dimension_asset_class') }} ac
        ON a.asset_class_map = ac.asset_class_name
    
    LEFT JOIN {{ ref('acumatica_dimension_region') }} r
        ON a.region = r.region

    LEFT JOIN {{ ref('acumatica_dimension_product') }} p
        ON a.product = p.product

    LEFT JOIN {{ ref('acumatica_dimension_account_classification') }} dac
        ON a.account_classification = dac.account_classification
    
    --LEFT JOIN {{ ref('acumatica_dimension_firm') }} f
    --ON a.firm_id = f.firm_id

    --LEFT JOIN {{ ref('acumatica_dimension_parent_firm') }} pf
    --ON a.parent_firm_id = pf.parent_firm_id

    WHERE list_price_per_product = '1'
),

 final as (
 
            SELECT
                {{ dbt_utils.surrogate_key(
                    ['revenue.core_id']
                ) }}                                                   AS ROW_ID,
            *
            FROM
            revenue

 )

 select * from final