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
    C.STATUS                               AS IS_ACTIVE,
    C.crm_contact_firm_id_c,
    C.HasOptedOutOfEmail,
    C.DoNotCall,
    C.Phone,
    C.MobilePhone,
    C.TOTAL_COMMS_C                        AS TOTAL_COMMS,
    C.TOTAL_COMMS_LAST_12_MONTHS_C         AS TOTAL_COMMS_L12M,
--INDIVIDUAL Primary MAPPING
    COALESCE(IndividualMaster.dimension_individual_key, '-1') AS CONFORMED_DIMENSION_PRIMARY_INDIVIDUAL_KEY,
--FIRM Primary MAPPING
    COALESCE(AC.DIMENSION_ACCOUNT_KEY, '-1') AS DIMENSION_PRIMARY_ACCOUNT_KEY,
    COALESCE(AC.CONFORMED_DIMENSION_FIRM_KEY, '-1') AS CONFORMED_PRIMARY_DIMENSION_FIRM_KEY,
    2 AS Datasource_ID
FROM {{ source('salesforce', 'contact') }} C 
LEFT
JOIN {{ ref('salesforce_dimension_account') }} AC
    ON C.ACCOUNTID = AC.ACCOUNT_ID
LEFT 
JOIN {{ ref('preqin_dimension_individual') }} IndividualMaster
    ON C.crm_contact_firm_id_c = IndividualMaster.CONTACTFIRM_ID        
