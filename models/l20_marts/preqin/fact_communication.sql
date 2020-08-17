{{ config(materialized='view') }}

--{{ config(schema='preqin') }}

with fact_communication as (
select 

{{ dbt_utils.surrogate_key(
      'Communication_ID'
  ) }} as RowID

,Communication_ID		AS CommunicationId
,to_char(add_months(C.Communication_Date,-1 ), 'YYYYMMDD') as Dimension_Date_Communication_Created_Date_Key

,ifnull(CD.dimension_communication_direction_KEY,
{{ dbt_utils.surrogate_key(
      '-1'
  ) }}) AS dimension_communication_direction_KEY

from DB_RAW.PREQIN01_FIVETRAN_DBO.tblCommunication C
left
join {{ ref('dimension_communication_direction') }} CD ON C.Comm_Direction = cd.communication_Direction_Name

)

select *
from fact_communication