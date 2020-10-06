{{
    config(
        materialized='incremental',
        unique_key='ContactFirm_ID'
    )
}}

WITH dimension_user AS (

    SELECT
        {{ dbt_utils.surrogate_key(
            'cf.ContactFirm_ID'
        ) }} AS dimension_user_KEY,-- bk cf.contactfirm_id,-- ud.user_id,
        cf.ContactFirm_ID,
        C.contact_id,
        cf.firm_id,
        {{ dbt_utils.surrogate_key(
            'cf.Firm_ID'
        ) }} AS dimension_FIRM_KEY,
        cf.cf_Tel,
        cf.cf_Mob,
        cf.cf_Fax,
        cf.cf_JobTitle,
        cf.cf_Email,
        cf.insertedby AS cf_InsertedBy,-- dimmaintainusercfinsertedbykey cf.inserteddate AS cf_InsertedDate,-- dimdatecfinserteddatekey cf.updatedby AS cf_UpdatedBy,-- dimmaintainusercfupdatedbykey cf.cf_LinkedIn,
        cf.cf_Status,
        cf.cf_Hide,
        cf.cf_Email_Hide,
        cf.cf_Tel_Hide,
        cf.cf_SendEmail,
        cf.cf_SendSnailMail,
        cf.datechecked AS cf_DateChecked,-- dimdatecfcheckeddatekey cf.datecheckedby AS cf_DateCheckedBy,-- dimmaintainusercfcheckedbykey ud.user_email,
        ud.tandc user_tandc,
        ud.tandc_date user_tandc_date,
        ud.date_registered user_date_registered,
        ud.last_accessed user_last_accessed,-- dimdateuserlastaccesseddatekey ud.user_source_id,-- ? ? lookup ? ? ud.user_TrialPageLimit_Total,
        ud.user_TrialPageLimit_InvestorProfiles,
        ud.referrer_id,-- ? ? lookup ? ? ud.passwordlocked AS user_PasswordLocked,
        ud.sharedaccount AS user_SharedAccount,
        ud.insertedby AS user_InsertedBy,-- dimmaintainuserinsertedbykey ud.insertedon AS user_InsertedOn,-- dimdateuserinsertonkey ud.user_Guid,
        ud.guid_date AS user_Guid_Date,-- dimdateuserguiddatekey ud.pagecountoff AS user_PageCountOff,
        C.contact_status,
        C.contact_title,
        C.contact_initials,
        C.contact_firstname,
        C.contact_surname,
        C.contact_suffix,
        C.contact_send_email,
        C.contact_insertdate,-- dimdatecontactinserteddatekey C.contact_updated,-- dimdatecontactupdateddatekey C.contact_email_hide,
        C.contact_hide,
        C.contact_send_snailmail,
        C.inserted_by contact_Inserted_By,-- dimmaintainusercontactinsertedbykey C.datechecked contact_DateChecked,-- dimdatecontactdatecheckedkey C.datecheckedby contact_CheckedBy,-- dimmaintainusercontactcheckedbykey C.contact_tel_hide,
        COALESCE(
            ca.firm_address_id,
            fa.firm_address_id
        ) AS address_id,
        COALESCE(
            ca.firm_address_1,
            fa.firm_address_1
        ) AS address_1,
        COALESCE(
            ca.firm_address_2,
            fa.firm_address_2
        ) AS address_2,
        COALESCE(
            ca.firm_City,
            fa.firm_City
        ) AS city,
        COALESCE(
            ca.firm_State,
            fa.firm_State
        ) AS state,
        COALESCE(
            ca.firm_Country,
            fa.firm_Country
        ) AS country,
        COALESCE(
            ca.firm_zip_code,
            fa.firm_zip_code
        ) AS zip_Code,
        C.ispseudonymised,
        d.divisionname,
        t.teamname
    FROM
        db_raw.preqin01_fivetran_dbo.tblContactFirm cf
        LEFT JOIN db_raw.preqin01_fivetran_dbo.tblUser_Details ud
        ON cf.contactfirm_id = ud.contactfirm_id
        JOIN db_raw.preqin01_fivetran_dbo.tblContact C
        ON cf.contact_id = C.contact_id
        LEFT JOIN db_raw.preqin01_fivetran_dbo.tblfirm_Address ca
        ON cf.firm_address_id = ca.firm_address_id
        LEFT JOIN db_raw.preqin01_fivetran_dbo.tblFirm_Address fa
        ON cf.firm_id = fa.firm_id
        AND fa.main_address = 1
        LEFT JOIN db_raw.preqin01_fivetran_dbo.tblContactFirm_DivisionLookUp d
        ON cf.divisionid = d.divisionid
        LEFT JOIN db_raw.preqin01_fivetran_dbo.tblContactFirm_TeamLookUp t
        ON cf.teamid = t.teamid
)
SELECT
    *
FROM
    dimension_user
