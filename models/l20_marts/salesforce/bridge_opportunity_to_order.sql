{{ config(materialized='table') }}

select ORDERID, COLLATE(regexp_substr(NOTES, '\\SF\\sID\\W+(\\w+)', 1, 1, 'ime', 1),'upper') as "OPPORTUNITYID" 
FROM {{ ref('stg_tblorders') }} -- orders can be linked to Preqin subs. (which in turn gives us the contacts), whilst the opportunityID links to SF data.
WHERE regexp_substr(NOTES, '\\SF\\sID\\W+(\\w+)', 1, 1, 'ime', 1) IS NOT NULL