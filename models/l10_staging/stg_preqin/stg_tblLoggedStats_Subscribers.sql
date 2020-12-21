
--14/Dec/2020 once off migration of tblLoggedStats_Subscribers to pull across historic paid subscribers
select
LOGGEDSUBSCRIBERID as log_id,
SUBSCRIBERFIRMID as firm_id,
SUBSCRIBERCONTACTFIRMID as contactfirm_id,
PRODUCTID as product_id,
CAST(YEAR((LOGGEDDATE)) || RIGHT('0' || MONTH((LOGGEDDATE)), 2) || RIGHT('0' || DAYOFMONTH((LOGGEDDATE)), 2) AS INT) as date_key
from  {{ source('preqin', 'tblLoggedStats_Subscribers') }}
