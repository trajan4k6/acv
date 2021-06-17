{{   config(      materialized='incremental',         alias='acv_growth_levers'    ) }}
--{{   config(      materialized='table',         alias='acv_growth_levers'    ) }}

WITH CTE AS 
(SELECT
 (SELECT
 MIN(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY) MIN_AS_AT_DATE
 , MIN(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION) REGION_MIN_AS_AT_DATE --UPSELL_CALC
 , MAX(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION) REGION_MAX_AS_AT_DATE --MODULE__LOCATION_DROP_CALC
  , MIN(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE) STATE_MIN_AS_AT_DATE --NEW_LOCATION_CALC
 , MAX(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE) STATE_MAX_AS_AT_DATE --LOCATION_DROP_CALC
 , MIN(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,FIRM_ID ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID) FIRM_MIN_AS_AT_DATE --NEW_TEAM_CALC
 , LAG(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID,FIRM_ID ORDER BY AS_AT_DATE) FIRM_PRE_AS_AT_DATE --NEW_TEAM_CALC
 , MAX(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,FIRM_ID ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID) FIRM_MAX_AS_AT_DATE -- TEAM_DROP_CALC
 , LEAD(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID,FIRM_ID ORDER BY AS_AT_DATE) FIRM_POST_AS_AT_DATE --TEAM_DROP_CALC
 , MIN(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION) INV_MIN_AS_AT_DATE --CROSSSELL_CALC
 , MAX(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION ORDER BY LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_ID,FIRM_CATEGORY,INVENTORY_DESCRIPTION) INV_MAX_AS_AT_DATE --MODULE_DROP_CALC
 , LAG(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE ORDER BY AS_AT_DATE) STATE_PRE_AS_AT_DATE --NEW_LOCATION_CALC
 , LEAD(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE ORDER BY AS_AT_DATE) STATE_POST_AS_AT_DATE --LOCATION_DROP_CALC
 , LAG(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE ORDER BY AS_AT_DATE) PRE_AS_AT_DATE --PRICE_INC_DEC_CALC
 , LEAD(AS_AT_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE ORDER BY AS_AT_DATE) POST_AS_AT_DATE
 , LOGO.LOGO_MIN_AS_AT_DATE --NEW_LOGO_CALC
 , LOGO.LOGO_MAX_AS_AT_DATE --MODULE_DROP_CALC
 , LOGO.LOGO_PRE_AS_AT_DATE --NEW_LOGO_FLAT_RENEWAL_CALC
 , LOGO.LOGO_POST_AS_AT_DATE --CHURN_CALC
 , LOGO.LOGO_PRE_ACV --FLAT_RENEWAL_CALC
 , LOGO.LOGO_POST_ACV 
 , LOGO.LOGO_CUR_ACV --FLAT_RENEWAL_CALC
 , LAG(ACV_GBP) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE  ORDER BY AS_AT_DATE) PRE_ACV --LAST_MONTH_FLAT_RENEWAL_PRICE_INC_DEC_CALC
 , LEAD(ACV_GBP) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE  ORDER BY AS_AT_DATE) POST_ACV
 , LAG(START_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE  ORDER BY AS_AT_DATE) PRE_START_DATE --FLAT_RENEWAL_CALC
 , LAG(END_DATE) OVER (PARTITION BY FIRM_ID,LOOKUP_ID,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE  ORDER BY AS_AT_DATE) PRE_END_DATE
 , FIRM_ID,LOOKUP_ID,FIRM_NAME,FIRM_TIER
 , PARENT_FIRM_TYPE,FIRM_CATEGORY
 , INVENTORY_DESCRIPTION,ASSET_CLASS
 , PRODUCT_TYPE, AS_AT_DATE
 , REGION,COUNTRY,STATE
 , CURRENCY,ACV_GBP
 , START_DATE,END_DATE
 FROM
 ( SELECT FIRM_ID,LOOKUP_ID,FIRM_NAME,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE,AS_AT_DATE,SUM(ACV_GBP) ACV_GBP
     FROM   {{ref('stg_acv_values')}} 
      WHERE PRODUCT_TYPE ='Subscription'
     GROUP BY FIRM_ID,LOOKUP_ID,FIRM_NAME,FIRM_TIER,PARENT_FIRM_TYPE,FIRM_CATEGORY,INVENTORY_DESCRIPTION,ASSET_CLASS,PRODUCT_TYPE,REGION,COUNTRY,STATE,CURRENCY,START_DATE,END_DATE,AS_AT_DATE)  STG JOIN
 (SELECT LOOKUP_ID LOGO_LOOKUP_ID,
         AS_AT_DATE LOGO_AS_AT_DATE,
         NVL(LAG(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID ORDER BY AS_AT_DATE),AS_AT_DATE) LOGO_PRE_AS_AT_DATE,
         NVL(LEAD(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID ORDER BY AS_AT_DATE),AS_AT_DATE) LOGO_POST_AS_AT_DATE,
         NVL(LAG(ACV) OVER (PARTITION BY LOOKUP_ID ORDER BY AS_AT_DATE),ACV) LOGO_PRE_ACV,
         NVL(LEAD(ACV) OVER (PARTITION BY LOOKUP_ID ORDER BY AS_AT_DATE),ACV) LOGO_POST_ACV,
         MIN(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID ORDER BY LOOKUP_ID) LOGO_MIN_AS_AT_DATE,
         MAX(AS_AT_DATE) OVER (PARTITION BY LOOKUP_ID ORDER BY LOOKUP_ID) LOGO_MAX_AS_AT_DATE,
         ACV LOGO_CUR_ACV
  FROM 
        (SELECT LOOKUP_ID,AS_AT_DATE,SUM(ACV_GBP) ACV FROM   {{ ref('stg_acv_values') }}  WHERE PRODUCT_TYPE ='Subscription' GROUP BY LOOKUP_ID,AS_AT_DATE)
 ) LOGO ON STG.LOOKUP_ID=LOGO.LOGO_LOOKUP_ID AND STG.AS_AT_DATE=LOGO.LOGO_AS_AT_DATE
 ),



ALLNEW AS (
SELECT  --'1' FLAG,
        --LOGO_MIN_AS_AT_DATE,FIRM_MIN_AS_AT_DATE,INV_MAX_AS_AT_DATE,STATE_MAX_AS_AT_DATE,STATE_PRE_AS_AT_DATE,STATE_POST_AS_AT_DATE,
    LOGO_MAX_AS_AT_DATE,FIRM_MAX_AS_AT_DATE,FIRM_POST_AS_AT_DATE,
        FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        AS_AT_DATE,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        ACV_GBP,
        START_DATE,
        END_DATE,
        PRE_ACV ACV_LAST_MONTH, 
        ACV_GBP ACV_CURRENT_MONTH,
        CASE WHEN AS_AT_DATE=LOGO_MIN_AS_AT_DATE   OR MONTHS_BETWEEN(NVL(LOGO_PRE_AS_AT_DATE,AS_AT_DATE),AS_AT_DATE)>12 THEN 1 ELSE 0 END NEW_LOGO,
        0 CHURN,
        CASE WHEN MONTHS_BETWEEN(AS_AT_DATE,LOGO_PRE_AS_AT_DATE)=1 AND LOGO_PRE_ACV=LOGO_CUR_ACV AND PRE_ACV=ACV_GBP AND START_DATE<>PRE_START_DATE THEN 1 ELSE 0 END FLAT_RENEWAL,
        CASE WHEN MONTHS_BETWEEN(AS_AT_DATE,PRE_AS_AT_DATE)=1  AND PRE_ACV=ACV_GBP  THEN 1 ELSE 0 END FOLLOW,
        CASE WHEN FIRM_MIN_AS_AT_DATE<INV_MIN_AS_AT_DATE  AND INV_MIN_AS_AT_DATE=AS_AT_DATE  THEN 1 ELSE 0 END CROSS_SELL,
        CASE WHEN INV_MIN_AS_AT_DATE<REGION_MIN_AS_AT_DATE  AND REGION_MIN_AS_AT_DATE=AS_AT_DATE  THEN 1 ELSE 0 END UPSELL,
        0 MODULE_DROP,
        CASE WHEN (REGION_MIN_AS_AT_DATE<STATE_MIN_AS_AT_DATE  AND STATE_MIN_AS_AT_DATE=AS_AT_DATE) OR MONTHS_BETWEEN(AS_AT_DATE,NVL(STATE_PRE_AS_AT_DATE,AS_AT_DATE))>1 THEN 1 ELSE 0 END NEW_LOCATION,
        0 LOCATION_DROP,
        CASE WHEN MONTHS_BETWEEN(AS_AT_DATE,PRE_AS_AT_DATE)=1 AND PRE_ACV<ACV_GBP THEN 1 ELSE 0 END PRICE_INCREASE,
        CASE WHEN MONTHS_BETWEEN(AS_AT_DATE,PRE_AS_AT_DATE)=1 AND PRE_ACV>ACV_GBP THEN 1 ELSE 0 END PRICE_DECREASE,
        CASE WHEN (LOGO_MIN_AS_AT_DATE<FIRM_MIN_AS_AT_DATE  AND FIRM_MIN_AS_AT_DATE=AS_AT_DATE) OR MONTHS_BETWEEN(AS_AT_DATE,NVL(FIRM_PRE_AS_AT_DATE,AS_AT_DATE))>1 THEN 1 ELSE 0 END NEW_TEAM,
        0 TEAM_DROPPED
FROM    
        CTE
WHERE 
        --LOOKUP_ID='2016' and INVENTORY_DESCRIPTION='Real Estate Online' AND REGION='Europe/Africa' AND
        1=1
--ORDER BY COUNTRY,STATE,AS_AT_DATE

   ),
   
CHURN AS 
(SELECT  --'2' FLAG,
       --LOGO_MIN_AS_AT_DATE,FIRM_MIN_AS_AT_DATE,INV_MAX_AS_AT_DATE,STATE_MAX_AS_AT_DATE,STATE_PRE_AS_AT_DATE,STATE_POST_AS_AT_DATE,
    LOGO_MAX_AS_AT_DATE,FIRM_MAX_AS_AT_DATE,FIRM_POST_AS_AT_DATE,
       FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        ADD_MONTHS(AS_AT_DATE,1) AS_AT_DATE,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        ACV_GBP,
        START_DATE,
        END_DATE,
        PRE_ACV ACV_LAST_MONTH,
        ACV_GBP ACV_CURRENT_MONTH,
        0 NEW_LOGO,
        --CASE WHEN LOGO_PRE_AS_AT_DATE IS NULL THEN ACV_GBP ELSE 0 END CHURN,
        1  CHURN,
        0 FLAT_RENEWAL,
        0 FOLLOW,
        0 CROSS_SELL,
        0 UPSELL,
        0 MODULE_DROP,
        0 NEW_LOCATION,
        0 LOCATION_DROP,
        0 PRICE_INCREASE,
        0 PRICE_DECREASE,
        0 NEW_TEAM,
        0 TEAM_DROPPED
FROM    
        (SELECT  MONTHS_BETWEEN(NVL(LOGO_POST_AS_AT_DATE,CURRENT_DATE),AS_AT_DATE) MONTHS_DIFF,* FROM CTE)
WHERE 
        --LOOKUP_ID='2016' and INVENTORY_DESCRIPTION='Real Estate Online' AND REGION='Europe/Africa' AND
         --PRE_ACV IS NULL AND
         MONTHS_DIFF >1 AND LOGO_POST_AS_AT_DATE < CURRENT_DATE AND
        1=1

), 

TEAMDROPPED AS
(
  SELECT  --'1' FLAG,
        --LOGO_MIN_AS_AT_DATE,FIRM_MIN_AS_AT_DATE,INV_MAX_AS_AT_DATE,STATE_MAX_AS_AT_DATE,STATE_PRE_AS_AT_DATE,STATE_POST_AS_AT_DATE,*/
         LOGO_MAX_AS_AT_DATE,FIRM_MAX_AS_AT_DATE,FIRM_POST_AS_AT_DATE, 
        FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        ADD_MONTHS(AS_AT_DATE,1) AS_AT_DATE,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        ACV_GBP,
        START_DATE,
        END_DATE,
        PRE_ACV ACV_LAST_MONTH, 
        ACV_GBP ACV_CURRENT_MONTH,
        0 NEW_LOGO,
        0 CHURN,
        0 FLAT_RENEWAL,
        0 FOLLOW,
        0 CROSS_SELL,
        0 UPSELL,
        0 MODULE_DROP,
        0 NEW_LOCATION,
        0 LOCATION_DROP,
        0 PRICE_INCREASE,
        0 PRICE_DECREASE,
        0 NEW_TEAM,
        1 TEAM_DROPPED
FROM    
        (SELECT  MONTHS_BETWEEN(NVL(LOGO_MAX_AS_AT_DATE,CURRENT_DATE),FIRM_MAX_AS_AT_DATE) FIRM_LOGO_MONTHS_DIFF,MONTHS_BETWEEN(NVL(FIRM_POST_AS_AT_DATE,AS_AT_DATE),AS_AT_DATE) FIRM_MONTHS_DIFF, * FROM CTE) C
WHERE     (FIRM_MAX_AS_AT_DATE=AS_AT_DATE   AND  FIRM_LOGO_MONTHS_DIFF>1 AND AS_AT_DATE< CURRENT_DATE) OR   
		  (FIRM_MONTHS_DIFF>1 AND 
		   NOT EXISTS (SELECT 1 FROM CHURN CH WHERE CH.LOOKUP_ID= C.LOOKUP_ID AND CH.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1)) 
			)
),   

MODULEDROP AS 
(SELECT  --'1' FLAG,
        --LOGO_MIN_AS_AT_DATE,FIRM_MIN_AS_AT_DATE,INV_MAX_AS_AT_DATE,STATE_MAX_AS_AT_DATE,STATE_PRE_AS_AT_DATE,STATE_POST_AS_AT_DATE,
        LOGO_MAX_AS_AT_DATE,FIRM_MAX_AS_AT_DATE,FIRM_POST_AS_AT_DATE,
        FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        ADD_MONTHS(AS_AT_DATE,1) AS_AT_DATE,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        ACV_GBP,
        START_DATE,
        END_DATE,
        PRE_ACV ACV_LAST_MONTH, 
        ACV_GBP ACV_CURRENT_MONTH,
        0 NEW_LOGO,
        0 CHURN,
        0 FLAT_RENEWAL,
        0 FOLLOW,
        0 CROSS_SELL,
        0 UPSELL,
        1 MODULE_DROP,
        0 NEW_LOCATION,
        0 LOCATION_DROP,
        0 PRICE_INCREASE,
        0 PRICE_DECREASE,
        0 NEW_TEAM,
        0 TEAM_DROPPED
FROM    
        (SELECT  MONTHS_BETWEEN(NVL(FIRM_MAX_AS_AT_DATE,CURRENT_DATE),INV_MAX_AS_AT_DATE) INV_MONTHS_DIFF, MONTHS_BETWEEN(NVL(INV_MAX_AS_AT_DATE,CURRENT_DATE),REGION_MAX_AS_AT_DATE) REGION_MONTHS_DIFF,* FROM CTE) C
WHERE   (INV_MONTHS_DIFF>1 OR REGION_MONTHS_DIFF>1 )   AND (INV_MAX_AS_AT_DATE=AS_AT_DATE OR REGION_MAX_AS_AT_DATE=AS_AT_DATE) 
		 AND NOT EXISTS ( SELECT 1 FROM TEAMDROPPED T WHERE T.LOOKUP_ID= C.LOOKUP_ID AND T.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1) AND T.FIRM_ID=C.FIRM_ID)	
         AND NOT EXISTS (SELECT 1 FROM CHURN CH WHERE CH.LOOKUP_ID= C.LOOKUP_ID AND CH.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1)) 
),

LOCATIONDROP AS 
(SELECT  --'1' FLAG,
        --LOGO_MIN_AS_AT_DATE,FIRM_MIN_AS_AT_DATE,INV_MAX_AS_AT_DATE,STATE_MAX_AS_AT_DATE,STATE_PRE_AS_AT_DATE,STATE_POST_AS_AT_DATE,
        LOGO_MAX_AS_AT_DATE,FIRM_MAX_AS_AT_DATE,FIRM_POST_AS_AT_DATE,
        FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        ADD_MONTHS(AS_AT_DATE,1) AS_AT_DATE,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        ACV_GBP,
        START_DATE,
        END_DATE,
        PRE_ACV ACV_LAST_MONTH, 
        ACV_GBP ACV_CURRENT_MONTH,
        0 NEW_LOGO,
        0 CHURN,
        0 FLAT_RENEWAL,
        0 FOLLOW,
        0 CROSS_SELL,
        0 UPSELL,
        0 MODULE_DROP,
        0 NEW_LOCATION,
        1 LOCATION_DROP,
        0 PRICE_INCREASE,
        0 PRICE_DECREASE,
        0 NEW_TEAM,
        0 TEAM_DROPPED
FROM    
        (SELECT  MONTHS_BETWEEN(NVL(REGION_MAX_AS_AT_DATE,CURRENT_DATE),STATE_MAX_AS_AT_DATE) REGION_STATE_MONTHS_DIFF,MONTHS_BETWEEN(NVL(STATE_POST_AS_AT_DATE,AS_AT_DATE),AS_AT_DATE) STATE_MONTHS_DIFF, * FROM CTE) C
WHERE     (STATE_MAX_AS_AT_DATE=AS_AT_DATE   AND   REGION_STATE_MONTHS_DIFF>1 AND AS_AT_DATE< CURRENT_DATE) OR 
		  (STATE_MONTHS_DIFF>1 AND
		   NOT EXISTS (SELECT 1 FROM MODULEDROP MD WHERE MD.LOOKUP_ID= C.LOOKUP_ID AND MD.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1) AND MD.FIRM_ID = C.FIRM_ID AND MD.INVENTORY_DESCRIPTION=C.INVENTORY_DESCRIPTION AND MD.REGION=C.REGION )
           AND NOT EXISTS ( SELECT 1 FROM TEAMDROPPED T WHERE T.LOOKUP_ID= C.LOOKUP_ID AND T.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1) AND T.FIRM_ID=C.FIRM_ID)	
         AND NOT EXISTS (SELECT 1 FROM CHURN CH WHERE CH.LOOKUP_ID= C.LOOKUP_ID AND CH.AS_AT_DATE = ADD_MONTHS(C.AS_AT_DATE,1)) 
			)
),



FINAL AS(
 SELECT 
        CASE WHEN NEW_LOGO<>0 THEN 'NEW_LOGO'
             WHEN CHURN<>0 THEN 'CHURN'
             WHEN FLAT_RENEWAL<>0 THEN 'FLAT_RENEWAL'
             WHEN FOLLOW<>0 THEN 'NO_CHANGE'
             WHEN CROSS_SELL<>0 THEN 'CROSS_SELL'
             WHEN UPSELL<>0 THEN 'UPSELL' 
             WHEN MODULE_DROP<>0 THEN 'MODULE_DROP'
             WHEN NEW_LOCATION<>0 THEN 'NEW_LOCATION'
             WHEN LOCATION_DROP<>0 THEN 'LOCATION_DROP'
             WHEN PRICE_INCREASE<>0 THEN 'PRICE_INCREASE' 
             WHEN PRICE_DECREASE<>0 THEN 'PRICE_DECREASE' 
             WHEN NEW_TEAM<>0 THEN 'NEW_TEAM' 
             WHEN TEAM_DROPPED<>0 THEN 'TEAM_DROPPED' 
             ELSE 'TBD'
        END ROW_TYPE,                    
 *
  FROM (
SELECT * FROM ALLNEW 
UNION ALL
SELECT * FROM CHURN 
UNION ALL
SELECT * FROM MODULEDROP 
UNION ALL
SELECT * FROM LOCATIONDROP 
UNION ALL
SELECT * FROM TEAMDROPPED   
 ) FIN
--WHERE LOOKUP_ID='2016' and INVENTORY_DESCRIPTION='Real Estate Online' AND REGION='Europe/Africa' 
--ORDER BY COUNTRY,STATE,AS_AT_DATE
)


SELECT ROW_TYPE,
        --MIN_AS_AT_DATE,PRE_ACV,PRE_AS_AT_DATE,
        FIRM_ID,
        LOOKUP_ID,
        FIRM_NAME,
        FIRM_TIER,
        PARENT_FIRM_TYPE,
        FIRM_CATEGORY,
        INVENTORY_DESCRIPTION,
        ASSET_CLASS,
        PRODUCT_TYPE,
        AS_AT_DATE,
        EXTRACT ('YEAR', AS_AT_DATE) AS_AT_YEAR,
        REGION,
        COUNTRY,
        STATE,
        CURRENCY,
        NVL(ACV_LAST_MONTH,ACV_GBP) ACV_LAST_MONTH,
        ACV_GBP,
        START_DATE,
        END_DATE
FROM     FINAL 
--WHERE LOOKUP_ID='2050'
--AND ROW_TYPE='NEW_LOGO'
--and INVENTORY_DESCRIPTION='Real Estate Online' AND REGION='Europe/Africa'


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where AS_AT_YEAR >= (select max(AS_AT_YEAR) from {{ this }})

{% endif %}