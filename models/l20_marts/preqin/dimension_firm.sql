--{{ config(materialized='table') }}

--{{ config(schema='preqin') }}
/*
{{
    config(
        materialized='incremental',
        unique_key='FIRM_ID'
    )
}}
*/

with dimension_firm as (
select {{ dbt_utils.surrogate_key(
      'FIRM_ID'
  ) }} as DIMENSION_FIRM_KEY
,Firm_ID														AS FirmId,
NULLIF(Firm_Name,'')												AS FirmName,

Firm_Tel  AS Telephone,
--NULLIF((Firm_Tel + ISNULL('('  + Firm_Tel_Extension + ')', '')),'') AS Telephone, 


/*NULLIF(Firm_Fax,'')													AS Fax,
NULLIF(Firm_Web_address,'')											AS WebAddress,
Firm_email															AS EmailAddress,
NULLIF(Firm_established,'')											AS YearFirmEstablished,
CAST(Firm_About AS varchar)									AS FirmAbout,
NULLIF(FirmType,'')													AS FirmType ,
firm_status															AS IsFirmActive,
firm_address_id														AS PrimaryAddressId,
NULLIF(firm_address_1, '')											AS PrimaryAddress1,
NULLIF(firm_address_2, '')											AS PrimaryAddress2,
NULLIF(firm_City, '')												AS PrimaryAddressCity,
NULLIF(firm_State,'')												AS PrimaryAddressState,
NULLIF(firm_Country,'')												AS PrimaryAddressCountry,
NULLIF(firm_zip_code,'')											AS PrimaryAddressZipCode,*/
Firm_PressRelease													AS IsFirmPressRelease,
Firm_PressReleaseNotes												AS PressReleaseNotes,
Firm_Liquidated														AS IsFirmLiquidated,
Firm_Industry														AS PrimaryIndustry,
Alt_AssetsMan														AS TotalAltAssetsManaged,
ActiveFMP_PE														AS IsActiveFmpPE,
Filed_Bankruptcy													AS IsFiledBankrupt,
Filed_BankruptcyDateEstimated										AS IsBankruptDateEstimated,
Firm_Is_Competitor													AS IsCompetitor,
Deals_VCKeyIndustryText												AS Deals_VCKeyIndustryText,
VentureDeals_DoNotEstimateFunds										AS VentureDealsDoNoEstimateFunds,
Firm_Established_Quarter											AS FirmEstablishedQuarter,
FiscalYearEnd														AS FiscalYearEnd,
isWomenOwned														AS IsWomenOwned,
isMinorityOwned														AS IsMinorityOwned,
--SECFullLegalName													AS  SECFullLegalName,
--SECFileNumber														AS  SECFileNumber,
CRDNumber															AS CRDNumber,
Firm_StaffCount_Total												AS TotalStaff,
Firm_StaffCount_Mgmt												AS TotalMgmtStaff,
Firm_StaffCount_Inv													AS TotalInvStaff,
Total_No_Clients													AS TotalClients,
NumberAlt_Clients													AS TotalAltClients

from DB_RAW.PREQIN01_FIVETRAN_DBO.tblfirm
)

select *
from dimension_firm


