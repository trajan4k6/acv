{{ config(materialized='table') }}



SELECT
    {{ dbt_utils.surrogate_key(
        [2,'ID']
    ) }}                                        AS DIMENSION_ACCOUNT_KEY,
  ID                                            AS ACCOUNT_ID,
  NAME                                          AS ACCOUNT_NAME,
  TYPE_C                                        AS ACCOUNT_TYPE,
  STATUS_C                                      AS ACCOUNT_STATUS,
  CRM_FIRM_ID_C                                 AS CRM_FIRM_ID,
  a.TYPE_C                                      AS CRM_FIRM_TYPE,
  COALESCE(rt.DIMENSION_REGION_TEAM_KEY, '-1')  AS DIMENSION_REGION_TEAM_KEY,
  COALESCE(r.DIMENSION_REGION_KEY, '-1') AS DIMENSION_REGION_KEY,
  COALESCE(ac.DIMENSION_ACCOUNT_CLASSIFICATION_KEY, '-1') AS DIMENSION_ACCOUNT_CLASSIFICATION_KEY,
--Dedup CRM_Firm_Id mapping SaleforceAccount(M)toPreqinFirm(1)
  CASE WHEN a.createddate = LAST_VALUE(createddate) OVER (PARTITION BY CRM_FIRM_ID_C ORDER BY CREATEDDATE) THEN FIRMMASTER.dimension_firm_key ELSE '-1' END AS CONFORMED_DIMENSION_FIRM_KEY,
  CAST(YEAR((a.CREATEDDATE::DATE)) || RIGHT('0' || MONTH((a.CREATEDDATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((a.CREATEDDATE::DATE)), 2) AS INT) AS CREATED_DATE_KEY,
  2 AS DATASOURCE_ID
FROM
    {{ source('salesforce', 'account') }} a
    
LEFT JOIN {{ ref('preqin_dimension_firm') }} FirmMaster
    ON a.CRM_FIRM_ID_C = TO_CHAR(FirmMaster.FIRM_ID)

LEFT JOIN {{ ref('salesforce_dimension_account_classification') }} ac
    ON a.CLASSIFICATION_C = ac.ACCOUNT_CLASSIFICATION

LEFT JOIN {{ ref('salesforce_dimension_region') }} r
    ON a.REGION_C = r.REGION_NAME

LEFT JOIN {{ ref('salesforce_dimension_region_team') }} rt
    ON a.LEAD_TEAM_C = rt.REGION_TEAM_NAME