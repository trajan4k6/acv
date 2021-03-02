{{ config(materialized='table') }}

SELECT
c.dimension_contact_key,
ca.dimension_contact_account_key
FROM {{ source('salesforce', 'accountcontactrelationship') }} acr
JOIN {{ ref('salesforce_dimension_contact')}} c
    ON acr.contactid = c.contact_id
JOIN {{ ref('salesforce_dimension_contact_account')}} ca
    ON acr.id = ca.contact_account_id
WHERE
    acr.isdeleted = FALSE
AND acr.crm_contact_firm_id_c IS NOT NULL