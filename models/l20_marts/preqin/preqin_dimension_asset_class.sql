{{ config(materialized='table') }}

WITH SOURCE_DIMENSION_ASSET_CLASS AS(

SELECT 'PE' AS Asset_Class_Short_Name,  'Private Equity' as Asset_Class_Name
UNION ALL
SELECT 'HF', 'Hedge Funds'
UNION ALL
SELECT 'RE', 'Real Estate' 
UNION ALL 
SELECT 'INF',  'Infrastructure' 
UNION ALL 
SELECT 'PD',  'Private Debt'
UNION ALL 
SELECT 'NR',  'Natural Resources'
UNION ALL 
SELECT 'VC',  'Venture Capital'
UNION ALL 
SELECT 'SEC',  'Secondaries'
UNION ALL
SELECT 'PC',  'Private Capital'
)

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'Asset_Class_Short_Name']                       
    ) }} AS dimension_asset_class_key,
    Asset_Class_Short_Name,
    Asset_Class_Name,
    1 AS Datasource_ID
FROM SOURCE_DIMENSION_ASSET_CLASS