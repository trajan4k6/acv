{{ config(materialized='table') }}

WITH source AS (
SELECT * FROM  {{ source('acv', 'ACV_VALUES_2020_2018') }}
),
renamed AS (
    SELECT
			"Firm ID" FIRM_ID,
			"Lookup ID" LOOKUP_ID,
			"Firm Name" FIRM_NAME,
			"Firm Tier" FIRM_TIER,
			"Parent Firm Type" PARENT_FIRM_TYPE,
			"Firm Category"  FIRM_CATEGORY,
			"Inventory Description" INVENTORY_DESCRIPTION,
			"Asset Class" ASSET_CLASS,
			"Product Type" PRODUCT_TYPE,
			"Month Reflected In Bank" AS_AT_DATE,
			"Region" REGION,
			"Country" COUNTRY,
			"State" STATE,
			"Currency" CURRENCY,ACV_GBP,
			"Start Date" START_DATE,
			"End Date" END_DATE

    FROM source

),

final AS (

    SELECT 
    *                 
FROM renamed

)

SELECT * FROM final
