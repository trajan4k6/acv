{{ config(materialized='table', alias='heap_dimension_firm') }}

SELECT
    account_id,
    MAX(firm_id::int) AS legacy_firm_id,
    MAX(contact_firm_name_text__C) AS firm_name,
    COUNT(distinct contact_id) AS associated_contacts,
    MIN(joindate) AS heap_date_created,
    MAX(last_modified) AS heap_last_modified,
    MAX(contact_lastactivitydate) AS salesforce_last_activity_date,
    MAX(CASE WHEN contact_subscription_status_text__C LIKE 'Active %' THEN 1 ELSE 0 END) AS has_active_sub,
    MAX(CASE WHEN contact_subscription_status_text__C = 'Active (Paid)' THEN 1 ELSE 0 END) AS has_paid_sub
FROM {{ source('heap', 'users') }}
WHERE account_id IS NOT NULL
GROUP BY 1
