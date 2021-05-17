{{ config(materialized='table') }}

WITH cte_opportunity as  (
      SELECT row_number() over (partition by O.ID order by o.systemmodstamp desc  ) rn,
      * 
      FROM {{ source('salesforce', 'opportunity') }} O 
      WHERE ISDELETED = FALSE
        AND stagename = 'Closed Won'
    )

    ,cte_opportunity_latest as (
      SELECT * FROM cte_opportunity WHERE RN=1
    )

    ,cte_opportunity_detail as (
      SELECT row_number() over (partition by OLI.ID order by OLI.systemmodstamp desc  ) rn
      ,* 
      FROM {{ source('salesforce', 'opportunitylineitem') }} OLI WHERE ISDELETED = FALSE
    )

    ,cte_opportunity_detail_latest as (
      SELECT * FROM cte_opportunity_detail WHERE RN=1
    )
    
 // --Filter for Closed Won - Preqin pricing model SUBSCRIPTION opportunity records
  ,cte_package AS (
    SELECT 
      O.ID AS OPPORTUNITYID,
      OLI.ID AS OPPORTUNITY_DETAIL_ID,
      O.AccountID,
      OLI.PACKAGE_PRODUCT_CODE__C,
      o.CLASSIFICATION__C as CLASSIFICATION,
      COALESCE(O.SUBSCRIPTION_START_DATE__C, O.RENEWAL_DATE__C, O.CloseDate) SF_SUBSCRIPTION_START_DATE,
      o.systemmodstamp,
      P.Family AS Package_Family_Name,
      P.Name AS Package_Name, 
      P.ProductCode AS Package_ProductCode
    FROM cte_opportunity_latest O 
      LEFT JOIN cte_opportunity_detail_latest OLI ON O.ID = OLI.OPPORTUNITYID
      LEFT JOIN {{ source('salesforce', 'product2') }} P ON OLI.PRODUCT2ID = P.ID

  )

  SELECT DISTINCT 
      {{ dbt_utils.surrogate_key(
        [2,'']                       
    ) }} AS DIMENSION_SUBSCRIPTION_KEY,
    COALESCE(sda.DIMENSION_ACCOUNT_KEY, '-1')  AS DIMENSION_ACCOUNT_KEY
    o.*, 
    NVL(geography.name,'Global') as Geography_Name, 
    geography.ProductCode as Geography_ProductCode
  from cte_package o
  left join (SELECT OLI.OPPORTUNITYID,OLI.ProductCode,P.Name FROM cte_opportunity_detail_latest OLI 
      JOIN {{ source('salesforce', 'product2') }} P ON OLI.PRODUCT2ID = P.ID
      WHERE P.Family ='Geography') Geography
    ON o.OPPORTUNITYID = Geography.OPPORTUNITYID
  LEFT JOIN {{ ref('salesforce_dimension_account') }} sda ON O.AccountID = sda.ACCOUNT_ID