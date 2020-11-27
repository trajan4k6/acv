SELECT *, 
FIRM_C AS ACCOUNT_ID
FROM  {{ source('salesforce', 'competitor__c') }}