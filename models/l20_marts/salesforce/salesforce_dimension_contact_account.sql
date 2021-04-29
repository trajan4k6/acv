{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [2,'acr.id']                       
    ) }} AS dimension_contact_account_key,
    acr.id                                 AS contact_account_id,
    crm_contact_firm_id_c                  AS crm_contact_firm_id,
    NVL(acr.isdirect, FALSE)               AS is_primary_firm,
    NVL(acr.isactive, FALSE)               AS is_active,                          
--INDIVIDUAL MAPPING
    COALESCE(IndividualMaster.dimension_individual_key, '-1') AS CONFORMED_DIMENSION_INDIVIDUAL_KEY,
--FIRM MAPPING
    COALESCE(AC.DIMENSION_ACCOUNT_KEY, '-1') AS DIMENSION_ACCOUNT_KEY,
    COALESCE(AC.CONFORMED_DIMENSION_FIRM_KEY, '-1') AS CONFORMED_DIMENSION_FIRM_KEY,
    2 AS Datasource_ID
FROM {{ source('salesforce', 'accountcontactrelationship') }} acr
LEFT 
JOIN {{ ref('preqin_dimension_individual') }} IndividualMaster
    ON acr.crm_contact_firm_id_c = IndividualMaster.contactfirm_id
LEFT
JOIN {{ ref('salesforce_dimension_account') }} ac
    ON acr.ACCOUNTID = ac.ACCOUNT_ID
WHERE
    acr.isdeleted = FALSE
AND acr.crm_contact_firm_id_c IS NOT NULL

