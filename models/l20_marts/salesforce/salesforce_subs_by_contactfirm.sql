{{ config(materialized='table') }}

select distinct
      --COALESCE(ind.dimension_individual_key, '-1')  AS DIMENSION_INDIVIDUAL_KEY
      cf.contactfirm_id
    , c.contact_title
    , c.contact_firstname
    , c.contact_surname
    , COALESCE(cf.cf_email, u.user_email) email
    , f.firm_id
    , f.firm_name
    , Ft.Firm_Type
    , fa.Country as Contact_Country
    , s.PACKAGE_NAME
    , s.CLASSIFICATION 
    , s.SF_SUBSCRIPTION_START_DATE
    , COALESCE(ms.TreatAsNew
              ,CASE WHEN s.CLASSIFICATION = 'New Logo' THEN TRUE ELSE FALSE END) AS TreatAsNew
    , o.OPPORTUNITYID
    , o.ORDERID
FROM {{ ref('salesforce_subscriptions') }} s 
  JOIN {{ ref('bridge_opportunity_to_order') }} o ON o.OpportunityID = s.OpportunityID
  JOIN {{ ref('stg_tbluser_subscription') }} us ON us.orderid = o.orderid
  JOIN {{ ref('stg_tbluser_details') }} u ON us.user_id = u.user_id
  JOIN {{ ref('stg_tblcontactfirm') }} cf ON u.contactfirm_id = cf.contactfirm_id AND cf.CF_Status = 1
  JOIN {{ ref('stg_tblcontact') }} c ON cf.contact_id = c.contact_id
  JOIN {{ ref('stg_tblfirm') }} f ON f.firm_id = cf.firm_id
  JOIN {{ ref('stg_tblfirm_address') }} fa ON cf.firm_Address_ID = fa.firm_Address_ID
  JOIN {{ ref('stg_tblfirm_type') }} ft ON f.Firm_Type_ID = ft.Firm_Type_ID
  LEFT JOIN {{ ref('marketing_subs') }} ms ON ms.user_ID = u.user_ID
  
  --LEFT JOIN {{ ref('preqin_dimension_individual') }} ind ON cf.contactfirm_id = ind.CONTACTFIRM_ID
WHERE PACKAGE_FAMILY_NAME = 'Bundle'  
ORDER BY s.SF_SUBSCRIPTION_START_DATE, cf.contactfirm_id