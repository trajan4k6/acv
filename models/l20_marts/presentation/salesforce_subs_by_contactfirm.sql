{{ config(materialized='table') }}

select distinct
      ind.DIMENSION_INDIVIDUAL_KEY
    , ind.contactfirm_id
    , ind.contact_title
    , ind.contact_firstname
    , ind.contact_lastname
    , ind.email
    , f.firm_id
    , f.firm_name
    , f.Firm_Type
    , ind.Contact_Country
    , s.PACKAGE_NAME
    , s.CLASSIFICATION 
    , s.SF_SUBSCRIPTION_START_DATE
    , COALESCE( ind.TreatAsNew, CASE WHEN s.CLASSIFICATION = 'New Logo' THEN TRUE ELSE FALSE END ) AS TreatAsNew
    , s.OPPORTUNITYID
FROM {{ ref('preqin_fact_paid_subscriber_daily')}} psub 
JOIN {{ ref('salesforce_dimension_subscription') }} s ON s.DIMENSION_SUBSCRIPTION_KEY = psub.DIMENSION_SUBSCRIPTION_KEY
LEFT 
JOIN {{ ref('preqin_dimension_firm') }} f ON psub.DIMENSION_FIRM_KEY = f.dimension_firm_key
LEFT 
JOIN {{ ref('preqin_dimension_individual') }} ind ON psub.DIMENSION_INDIVIDUAL_KEY = ind.DIMENSION_INDIVIDUAL_KEY
LEFT 
JOIN {{ ref('preqin_dimension_product') }} prod ON psub.DIMENSION_PRODUCT_KEY = prod.DIMENSION_PRODUCT_KEY

WHERE PACKAGE_FAMILY_NAME = 'Bundle'  
  AND SF_SUBSCRIPTION_START_DATE BETWEEN DATEADD(DAY,-7,CURRENT_DATE) AND CURRENT_DATE
  AND prod.Product_Family_Name !='Feeds'
ORDER BY s.SF_SUBSCRIPTION_START_DATE, ind.contactfirm_id