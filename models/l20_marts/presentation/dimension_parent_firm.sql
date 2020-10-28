{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
'-1' as dimension_parent_firm_key, null as parent_firm_id, null as parent_firm_name, null as parent_firm_type, null as datasource_id
UNION
--1.Primary Parent Firms from Core
SELECT dimension_parent_firm_key, parent_firm_id, parent_firm_name, parent_firm_type, datasource_id
FROM {{ ref('preqin_dimension_parent_firm') }} p

UNION
--4. + any Unmapped Acumatica Parent Firms to Primary\Master Parent Firms
SELECT dimension_parent_firm_key, parent_firm_id, parent_firm_name, parent_firm_type, datasource_id
FROM {{ ref('acumatica_dimension_parent_firm') }} a
WHERE 
    a.conformed_dimension_parent_firm_key ='-1'