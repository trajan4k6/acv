{{ config(materialized='table') }}

WITH parent_firm AS
    (
        SELECT DISTINCT
            {{ dbt_utils.surrogate_key(
                [4,'a.parent_firm_id']
            ) }}                                                            as dimension_parent_firm_key,
            a.parent_firm_id						                        as parent_firm_id,
            parentfirmmaster.parent_firm_name,
            parentfirmmaster.parent_firm_type,
            coalesce(parentfirmmaster.dimension_parent_firm_key, '-1')      as conformed_dimension_parent_firm_key,
            4 as datasource_id
        FROM
            {{ ref('stg_acumatica_book_of_business') }} a

        LEFT JOIN {{ ref('preqin_dimension_parent_firm') }} parentfirmmaster
            ON a.parent_firm_id = parentfirmmaster.parent_firm_id

        WHERE a.parent_firm_id NOT LIKE 's%'
    )

SELECT
    *
FROM
    parent_firm