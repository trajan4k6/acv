{{ config(
    materialized = 'incremental',
    unique_key = 'FIRM_ID',
    tags = ["firm"]
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [1,'FIRM_ID']
    ) }} AS dimension_firm_key,
    firm_id AS firm_id,
    NULLIF(
        firm_name,
        ''
    ) AS firmname,
    firm_pressrelease AS isfirmpressrelease,
    firm_pressreleasenotes AS pressreleasenotes,
    firm_liquidated AS isfirmliquidated,
    firm_industry AS primaryindustry,
    alt_assetsman AS totalaltassetsmanaged,
    activefmp_pe AS isactivefmppe,
    filed_bankruptcy AS isfiledbankrupt,
    filed_bankruptcydateestimated AS isbankruptdateestimated,
    firm_is_competitor AS iscompetitor,
    deals_vckeyindustrytext AS deals_vckeyindustrytext,
    venturedeals_donotestimatefunds AS venturedealsdonoestimatefunds,
    firm_established_quarter AS firmestablishedquarter,
    fiscalyearend AS fiscalyearend,
    isWomenOwned AS iswomenowned,
    isMinorityOwned AS isminorityowned,-- secfulllegalname AS secfulllegalname,-- secfilenumber AS secfilenumber,
    crdnumber AS crdnumber,
    firm_staffcount_total AS totalstaff,
    firm_staffcount_mgmt AS totalmgmtstaff,
    firm_staffcount_inv AS totalinvstaff,
    total_no_clients AS totalclients,
    numberalt_clients AS totalaltclients, 
    1 AS DATASOURCE_ID
FROM
    {{ ref('tblfirm_snapshot') }}
    f
WHERE
    f.dbt_valid_to IS NULL
