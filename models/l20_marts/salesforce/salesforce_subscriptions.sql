{{ config(materialized='table') }}

WITH cte_opportunity as  (
    SELECT row_number() over (partition by O.ID order by o.systemmodstamp desc  ) rn,
    * 
    FROM {{ source('salesforce', 'opportunity') }} O WHERE isdeleted = FALSE
      AND stagename = 'Closed Won'-- Added these conditions to support marketing requirement. May deviate from Sandbox logic.
      AND ISDELETED = FALSE
    )

    ,cte_opportunity_latest as (
    SELECT * FROM cte_opportunity WHERE RN=1
    )

    ,cte_opportunity_detail as (
    SELECT row_number() over (partition by OLI.ID order by OLI.systemmodstamp desc  ) rn
    ,* 
    FROM {{ source('salesforce', 'opportunitylineitem') }} OLI WHERE isdeleted = FALSE
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

    O.SUBSCRIPTION_START_DATE__C,
    O.CLOSEDATE,
    O.RENEWAL_DATE__C AS RENEWAL_DATE,
    o.CLASSIFICATION__C as CLASSIFICATION,
    COALESCE(O.SUBSCRIPTION_START_DATE__C, O.CLOSEDATE) SUBSCRIPTION_START_DATE,
    o.systemmodstamp,
    P.Family AS Package_Family_Name,
    P.Name AS Package_Name, 
    P.ProductCode AS Package_ProductCode
  FROM cte_opportunity_latest O 
  LEFT JOIN cte_opportunity_detail_latest OLI ON O.ID = OLI.OPPORTUNITYID
  LEFT JOIN {{ source('salesforce', 'product2') }} P ON OLI.PRODUCT2ID = P.ID
  WHERE
        O.stagename = 'Closed Won'
    AND O.ISDELETED = FALSE
    AND OLI.ISDELETED = FALSE
  )

  SELECT DISTINCT o.*, 
    NVL(geography.name,'Global') as Geography_Name, 
    geography.ProductCode as Geography_ProductCode
  from cte_package o
  left join (SELECT OLI.OPPORTUNITYID,OLI.ProductCode,P.Name FROM cte_opportunity_detail_latest OLI 
      JOIN {{ source('salesforce', 'product2') }} P ON OLI.PRODUCT2ID = P.ID
      WHERE P.Family ='Geography') Geography
    ON o.OPPORTUNITYID = Geography.OPPORTUNITYID