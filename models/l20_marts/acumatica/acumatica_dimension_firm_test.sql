{{ config(materialized='table') }}

WITH firm AS

    (
        SELECT
            {{ dbt_utils.surrogate_key(
                [4,'a.firm_id']
            ) }}                                            as dimension_firm_key,
            a.firm_id,
            --a.region,
            firmmaster.firm_name,
            firmmaster.firm_type,
            firmmaster.firm_category,
            firmmaster.is_active,
            coalesce(firmmaster.dimension_firm_key, '-1')    as conformed_dimension_firm_key,
            4 as datasource_id,
            count(*) grp_cnt
        FROM
            {{ ref('stg_acumatica_book_of_business') }} a
        LEFT JOIN {{ ref('preqin_dimension_firm') }} firmmaster
            ON a.firm_id = firmmaster.firm_id
        GROUP BY a.firm_id, 
                --/*a.region,/* 
                   firmmaster.firm_name, firmmaster.firm_type,firmmaster.firm_category, firmmaster.is_active, firmmaster.dimension_firm_key
    )


create or replace sequence seq1; 
SELECT
    s.nextval,f.*
FROM
    firm f, table(getnextval(seq1)) s;
