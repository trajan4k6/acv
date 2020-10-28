{{ config(materialized='table') }}

WITH source AS (
SELECT * FROM  {{ source('acumatica', 'book_of_business') }}
),
renamed AS (
    SELECT
        invoice_no,
        firm_id,
        lookup_id               as parent_firm_id,
        asset_class,
        prod                    as product,
        currency,
        amount_local,
        months_round,
        bob_gbp,
        enterprise_core         as account_classification,
        countif                 as list_price_per_product,
        list_price,
        region,
        total_list_price,
        percent_of_list_price,
        sum_bob_gbp,
        implied_value,
        management_start        as management_start_date

    FROM source

),

final AS (

    SELECT 
    *,
    case
        when asset_class = '3rdPartyPlatformFee'                        then 'Private Capital'
        when asset_class = 'API-Cashflow'                               then 'Private Capital'
        when asset_class = 'API-HedgeFund'                              then 'Hedge Funds'
        when asset_class = 'API-Infrastructure'                         then 'Infrastructure'
        when asset_class = 'API-NaturalResources'                       then 'Natural Resources'
        when asset_class = 'API-PrivateDebt'                            then 'Private Debt'
        when asset_class = 'API-PrivateEquity'                          then 'Private Equity'
        when asset_class = 'API-RealEstate'                             then 'Real Estate'
        when asset_class = 'API-VentureCapital'                         then 'Venture Capital'
        when asset_class = 'Data Feed Misc'                             then 'Private Capital'
        when asset_class = 'DataFeed(MISC)'       	                    then 'Private Capital'
        when asset_class = 'DataFeed(MISC)-Custom'                      then 'Private Capital'
        when asset_class = 'DataFeed-HedgeFund'                         then 'Hedge Funds'
		when asset_class = 'DataFeed-HedgeFund-Custom'         			then 'Hedge Funds'
		when asset_class = 'DataFeed-HF-Standardized(FeedsAPI)'         then 'Hedge Funds'		
        when asset_class = 'DataFeed-Infrastructure'                    then 'Infrastructure'
		when asset_class = 'DataFeed-INF-Standardized(FeedsAPI)'        then 'Infrastructure'
        when asset_class = 'DataFeed-NaturalResources'                  then 'Natural Resources'
        when asset_class = 'DataFeed-NR-Standardized(FeedsAPI)'         then 'Natural Resources'		
        when asset_class = 'DataFeed-PrivateDebt'                       then 'Private Debt'
		when asset_class = 'DataFeed-PD-Standardized(FeedsAPI)'         then 'Private Debt'
		when asset_class = 'DataFeed-PrivateEquity'                     then 'Private Equity'
		when asset_class = 'DataFeed-PrivateEquity-Custom'              then 'Private Equity'
		when asset_class = 'DataFeed-PE-Standardized(FeedsAPI)'         then 'Private Equity'
        when asset_class = 'DataFeed-RealEstate'        				then 'Real Estate'
        when asset_class = 'DataFeed-RE-Standardized(FeedsAPI)'         then 'Real Estate'		
        when asset_class = 'DataFeed-VentureCapital'                    then 'Venture Capital'
        when asset_class = 'HedgeFunds'                                 then 'Hedge Funds'
        when asset_class = 'IO'                                         then 'Infrastructure'
        when asset_class = 'Mixed'                                      then 'Private Capital'
        when asset_class = 'NRO'                                        then 'Natural Resources'
        when asset_class = 'PECF'                                       then 'Private Equity'		
		when asset_class = 'PrivateDebt'                                then 'Private Debt'
        when asset_class = 'PrivateEquity'                              then 'Private Equity'
        when asset_class = 'RealEstate'                                 then 'Real Estate'
        when asset_class = 'Secondaries'                                then 'Secondaries'
        when asset_class = 'SpecialProjects'                            then 'Private Capital'
        when asset_class = 'VentureCapital'                             then 'Venture Capital'
    end as asset_class_map,
      
    case when product like 'API%'  then 'Data Delivery'
         when product like 'Data%' then 'Data Delivery'
         when product like 'DATA%' then 'Data Delivery'
         else 'Core Product'
    end                                                as product_type                     

FROM renamed

)

SELECT * FROM final
