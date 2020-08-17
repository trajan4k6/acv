{{ config(materialized='table') }}

{{ config(schema='preqin') }}


WITH dimension_user AS (
SELECT
{{ dbt_utils.surrogate_key(
      'cf.ContactFirm_ID'
  ) }} as dimension_user_KEY,

--BK
cf.ContactFirm_ID,
--
ud.user_id,
c.Contact_ID,

cf.Firm_ID,

{{ dbt_utils.surrogate_key(
      'cf.Firm_ID'
  ) }} as dimension_FIRM_KEY,

cf.cf_Tel,
cf.cf_Mob,
cf.cf_Fax,
cf.cf_JobTitle,
cf.cf_Email,
cf.InsertedBy as cf_InsertedBy, --DimMaintainUserCfInsertedByKey
cf.InsertedDate as cf_InsertedDate, --DimDateCfInsertedDateKey
cf.UpdatedBy as cf_UpdatedBy, --DimMaintainUserCfUpdatedByKey
cf.cf_LinkedIn,
cf.cf_Status,
cf.cf_Hide,
cf.cf_Email_Hide,
cf.cf_Tel_Hide,
cf.cf_SendEmail,
cf.cf_SendSnailMail,
cf.DateChecked as cf_DateChecked, --DimDateCfCheckedDateKey
cf.DateCheckedBy as cf_DateCheckedBy,--DimMaintainUserCfCheckedByKey


ud.user_email,
ud.tandc user_tandc,
ud.tandc_date user_tandc_date,
ud.date_registered user_date_registered,
ud.Last_accessed user_last_accessed, --DimDateUserLastAccessedDateKey
ud.user_source_id,  --??lookup??
ud.user_TrialPageLimit_Total,
ud.user_TrialPageLimit_InvestorProfiles,
ud.referrer_id, --??lookup??
ud.PasswordLocked as user_PasswordLocked,
ud.SharedAccount as user_SharedAccount,
ud.InsertedBy as user_InsertedBy, --DimMaintainUserInsertedByKey
ud.InsertedOn as user_InsertedOn, --DimDateUserInsertOnKey
ud.user_Guid,
ud.Guid_Date as user_Guid_Date, --DimDateUserGuidDateKey
ud.PageCountOff as user_PageCountOff,

c.contact_status,
c.contact_title,
c.contact_initials,
c.contact_firstname,
c.contact_surname,
c.contact_suffix,
c.contact_send_email,
c.contact_insertdate, --DimDateContactInsertedDateKey
c.contact_updated, --DimDateContactUpdatedDateKey
c.contact_email_hide,
c.contact_hide,
c.Contact_Send_SnailMail,
c.Inserted_By contact_Inserted_By, --DimMaintainUserContactInsertedByKey
c.DateChecked contact_DateChecked, --DimDateContactDateCheckedKey
c.DateCheckedBy contact_CheckedBy, --DimMaintainUserContactCheckedByKey
c.Contact_Tel_Hide,

COALESCE(ca.firm_address_id, fa.firm_address_id) as address_id,
COALESCE(ca.firm_address_1, fa.firm_address_1)  as address_1,
COALESCE(ca.firm_address_2, fa.firm_address_2) as address_2,
COALESCE(ca.firm_City, fa.firm_City) as city,
COALESCE(ca.firm_State, fa.firm_State) as state,
COALESCE(ca.firm_Country, fa.firm_Country) as country,
COALESCE(ca.firm_zip_code, fa.firm_zip_code) as zip_Code,
c.IsPseudonymised,
d.DivisionName,
t.TeamName

FROM DB_RAW.PREQIN01_FIVETRAN_DBO.tblContactFirm cf
LEFT JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblUser_Details ud
	ON cf.ContactFirm_ID = ud.ContactFirm_ID
JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblContact c
	ON cf.Contact_ID = c.contact_id
LEFT JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblfirm_Address ca 
ON cf.Firm_Address_ID = ca.firm_address_id

LEFT JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblFirm_Address fa
	ON cf.Firm_ID = fa.Firm_ID
	AND fa.Main_Address = 1
LEFT JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblContactFirm_DivisionLookUp d
	ON cf.DivisionId = d.DivisionId
LEFT JOIN DB_RAW.PREQIN01_FIVETRAN_DBO.tblContactFirm_TeamLookUp t
	ON cf.TeamId = t.TeamId
)


SELECT * FROM dimension_user
