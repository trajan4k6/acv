{{ config(materialized='table') }}

WITH asset_class AS
    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [4,'asset_class_map']
            ) }}                                                    as dimension_asset_class_key,
        asset_class_map                                             as asset_class_name,
        assetclassmaster.asset_class_short_name,
        coalesce(assetclassmaster.dimension_asset_class_key, '-1')  as conformed_dimension_asset_class_key,
        4                                                           as datasource_id
        FROM
            {{ ref('stg_acumatica_book_of_business') }}
        LEFT JOIN {{ ref('preqin_dimension_asset_class') }} assetclassmaster
            ON asset_class_map = assetclassmaster.asset_class_name 
        GROUP BY dimension_asset_class_key, asset_class_map,asset_class_short_name,datasource_id
    )

SELECT
    *
FROM
    asset_class
