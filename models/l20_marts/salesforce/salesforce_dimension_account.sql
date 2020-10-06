{{ config(
    materialized = 'incremental',
    unique_key = 'ACCOUNT_ID',
    tags = ["firm"]
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [2,'ID']
    ) }} AS dimension_ACCOUNT_key,
  ID as ACCOUNT_ID,
  NAME AS ACCOUNT_NAME,
  CRM_FIRM_ID_C,
  CLASSIFICATION_C,
  ISDELETED,
  REGION_C,
  COALESCE(FIRMMASTER.dimension_firm_key, NULL) AS CONFORMED_DIMENSION_KEY
 , 2 AS DATASOURCE_ID
FROM
    {{ ref('account_snapshot') }} A
    
LEFT JOIN {{ ref('preqin_dimension_firm') }} FirmMaster
    ON A.CRM_FIRM_ID_C = TO_CHAR(FirmMaster.FIRM_ID)
WHERE
    A.dbt_valid_to IS NULL
