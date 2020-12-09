SELECT 
Firm_Type,
Firm_Category
FROM  {{ ref('FirmType_To_FirmCategory' )}}