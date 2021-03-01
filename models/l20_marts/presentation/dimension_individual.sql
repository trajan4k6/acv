{{ config(
    materialized = 'table',
    unique_key = [],
    tags = []
) }}

with current_paid_individual AS (
    SELECT  ps.dimension_individual_key
    FROM {{ ref('fact_paid_subscriber_daily')}} ps
      JOIN {{ ref('dimension_date')}} d USING(Date_Key)
    WHERE 
        NVL(d.Is_Today, false) = true
    GROUP BY 1
),


salesforce_individual AS (

SELECT  
        ca.*,
{{ dbt_utils.star(from=ref('salesforce_dimension_contact'), except=["DATASOURCE_ID", "IS_ACTIVE"], relation_alias="c") }}

FROM {{ ref('salesforce_bridge_contact_to_contact_account')}} c2ca

JOIN {{ ref('salesforce_dimension_contact')}} c
    ON c2ca.dimension_contact_key = c.dimension_contact_key

JOIN {{ ref('salesforce_dimension_contact_account')}} ca
    ON c2ca.dimension_contact_account_key = ca.dimension_contact_account_key
),

conformed_individual_details AS (

SELECT '-1' AS DIMENSION_INDIVIDUAL_KEY, NULL AS CONTACTFIRM_ID, NULL AS CRM_Contact_ID, NULL CONTACT_TITLE, NULL CONTACT_FIRSTNAME, NULL AS CONTACT_LASTNAME, NULL AS EMAIL_ADDRESS, NULL AS LINKEDIN, FALSE AS IS_ACTIVE, FALSE AS Is_Primary_Firm, NULL AS HasOptedOutOfEmail, NULL AS  DoNotCall, NULL AS PHONE, NULL AS PHONE_MOBILE, NULL AS JOB_TITLE, NULL AS TOTAL_COMMS, NULL AS TOTAL_COMMS_L12M, '-1' AS DIMENSION_FIRM_KEY,  NULL AS DATASOURCE_ID
UNION
--1.Primary Contact list from Core
SELECT DIMENSION_INDIVIDUAL_KEY, TO_CHAR(I.CONTACTFIRM_ID), I.CONTACT_ID, I.CONTACT_TITLE, I.CONTACT_FIRSTNAME, I.CONTACT_LASTNAME, I.EMAIL_ADDRESS, I.LINKEDIN, I.IS_ACTIVE, C.Is_Primary_Firm, C.HasOptedOutOfEmail, C.DoNotCall, I.PHONE, I.MOBILE, I.JOB_TITLE, C.TOTAL_COMMS, C.TOTAL_COMMS_L12M, I.DIMENSION_FIRM_KEY, I.DATASOURCE_ID
FROM {{ ref('preqin_dimension_individual') }} I
--JOIN TO Salesforce Contact Dimension FOR Salesforce mastered attributes
LEFT
JOIN salesforce_individual C
    ON I.DIMENSION_INDIVIDUAL_KEY = C.CONFORMED_DIMENSION_INDIVIDUAL_KEY

--2 + any Unmapped Salesforce Contacts to Primary\Master Contact
UNION
SELECT DIMENSION_CONTACT_ACCOUNT_KEY, TO_CHAR(crm_contact_firm_id), NULL, TITLE, FIRST_NAME, LAST_NAME, EMAIL_ADDRESS, LINKEDIN, IS_ACTIVE, Is_Primary_Firm, HasOptedOutOfEmail, DoNotCall, Phone, MobilePhone, JOB_TITLE, C.TOTAL_COMMS, C.TOTAL_COMMS_L12M,
IFF(CONFORMED_DIMENSION_FIRM_KEY = '-1',DIMENSION_ACCOUNT_KEY, CONFORMED_DIMENSION_FIRM_KEY ) AS DIMENSION_FIRM_KEY,
DATASOURCE_ID
FROM salesforce_individual C
WHERE 
    C.CONFORMED_DIMENSION_INDIVIDUAL_KEY ='-1'

--3 + any Unmapped Heap User to Primary\Master Contact
UNION
SELECT DIMENSION_USER_KEY, Identity CONTACTFIRM_ID, NULL, TITLE, FIRST_NAME, LAST_NAME, NULL, NULL, FALSE, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
IFF(CONFORMED_DIMENSION_FIRM_KEY = '-1',DIMENSION_FIRM_KEY, CONFORMED_DIMENSION_FIRM_KEY ) AS DIMENSION_FIRM_KEY,
DATASOURCE_ID
FROM {{ ref('heap_dimension_user_integrated') }} U
WHERE 
    U.CONFORMED_DIMENSION_INDIVIDUAL_KEY ='-1'
AND U.CONFORMED_DIMENSION_FIRM_KEY <> '-1'
)


SELECT 
id.DIMENSION_INDIVIDUAL_KEY,
id.CONTACTFIRM_ID,
id.CRM_CONTACT_ID,
id.CONTACT_TITLE,
id.CONTACT_FIRSTNAME,
id.CONTACT_LASTNAME,
id.EMAIL_ADDRESS,
id.LINKEDIN,
id.PHONE,
id.PHONE_MOBILE,
id.JOB_TITLE,
id.IS_ACTIVE,
id.Is_Primary_Firm,
id.HASOPTEDOUTOFEMAIL,
id.DONOTCALL,
id.TOTAL_COMMS,
id.TOTAL_COMMS_L12M,
CASE WHEN cpi.dimension_individual_key IS NOT NULL THEN true ELSE false END is_paid_subscriber,
id.DIMENSION_FIRM_KEY,
id.DATASOURCE_ID
FROM conformed_individual_details id
LEFT
JOIN current_paid_individual cpi
    ON id.dimension_individual_key = cpi.dimension_individual_key