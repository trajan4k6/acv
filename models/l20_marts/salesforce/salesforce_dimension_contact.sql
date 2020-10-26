{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        [2,'C.ID']                       
    ) }} AS dimension_contact_key,
    C.ID AS CONTACT_ID,
    NULLIF(salutation,'')                  AS TITLE,
    NULLIF(firstname,'')                   AS FIRST_NAME,
    NULLIF(lastname,'')                    AS LAST_NAME,
    NULLIF(email,'')                       AS EMAIL_ADDRESS,
    NULLIF(LINKEDIN_C,'')                  AS LINKEDIN,
    NULLIF(Title,'')                       AS JOB_TITLE,
    NVL(ISDELETED, FALSE)                AS IS_ACTIVE,
    C.crm_contact_firm_id_c,
--INDIVIDUAL MAPPING
    COALESCE(IndividualMaster.dimension_individual_key, '-1') AS CONFORMED_DIMENSION_INDIVIDUAL_KEY,
--FIRM MAPPING
    COALESCE(AC.DIMENSION_ACCOUNT_KEY, '-1') AS DIMENSION_ACCOUNT_KEY,
    COALESCE(AC.CONFORMED_DIMENSION_FIRM_KEY, '-1') AS CONFORMED_DIMENSION_FIRM_KEY,
    2 AS Datasource_ID
FROM {{ source('salesforce', 'contact') }} C 
LEFT
JOIN {{ ref('salesforce_dimension_account') }} AC
    ON C.ACCOUNTID = AC.ACCOUNT_ID
---21-10-2020 # issue with Rivery sync of crm_contact_firm_id_c on contact, no records will map until this is resolved.
LEFT JOIN {{ ref('preqin_dimension_individual') }} IndividualMaster
    ON C.crm_contact_firm_id_c = IndividualMaster.CONTACTFIRM_ID        
