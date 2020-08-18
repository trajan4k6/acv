--{{ config(materialized='table') }}

--{{ config(schema='preqin') }}
/*
{{
    config(
        materialized='incremental',
        unique_key='Comm_Direction'
    )
}}
*/

with dimension_communication_direction as (
select DISTINCT {{ dbt_utils.surrogate_key(
      'Comm_Direction'
  ) }} as dimension_communication_direction_key
,Comm_Direction														AS communication_Direction_Name


from DB_RAW.PREQIN01_FIVETRAN_DBO.tblCommunication
WHERE 
NULLIF(Comm_Direction,'') is not null

)

select *
from dimension_communication_direction