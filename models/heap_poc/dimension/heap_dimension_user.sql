{{ config(materialized='table', alias='dimension_user') }}

SELECT
    LOWER(user_id) AS user_id,
    LOWER(identity) AS identity,
    LOWER(contact_email) AS email,
    account_id,
    contact_id,
    joindate AS heap_date_created,
    last_modified AS heap_last_modified,
    -- use more recently modified record to account for dups (one contact_id occasionally has 2 user_ids associated with it)
    LAST_VALUE(contact_lastactivitydate) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS salesforce_last_activity_date,
    LAST_VALUE(contact_firm_name_text__C) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS firm_name,
    -- some account_ids have multiple legacy firm_ids associated with them, take the largest id (most recent)
    LAST_VALUE(firm_id) OVER (partition BY account_id ORDER BY firm_id::int nulls first) AS legacy_firm_id,
    LAST_VALUE(contact_firstname) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS first_name,
    LAST_VALUE(contact_lastname) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS last_name,
    first_name || ' ' || last_name AS contact_name,
    LAST_VALUE(firm_type_) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS firm_type,
    LAST_VALUE(contact_department) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS contact_department,
    LAST_VALUE(contact_status__c) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS status,
    LAST_VALUE(contact_subscription_status_text__C) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS subscription_status,
    LAST_VALUE(contact_title) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS title,
    LAST_VALUE(contact_mailingcountrycode) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS country_code,
    LAST_VALUE(contact_sales_region__c) OVER (PARTITION BY contact_id ORDER BY heap_last_modified) AS sales_region
FROM {{ source('heap', 'users') }}
