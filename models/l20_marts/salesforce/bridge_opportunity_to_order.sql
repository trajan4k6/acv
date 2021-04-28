{{ config(materialized='table') }}

select ORDERID, COLLATE(regexp_substr(NOTES, '\\SF\\sID:\\W+(\\w+)', 1, 1, 'ime', 1),'upper') as "OPPORTUNITYID" 
FROM {{ source('preqin', 'tblorders') }}
WHERE regexp_substr(NOTES, '\\SF\\sID:\\W+(\\w+)', 1, 1, 'ime', 1) IS NOT NULL