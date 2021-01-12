
--14/Dec/2020 once off migration of tblLoggedStats_Subscribers to pull across historic paid subscribers
select
LOGGEDSUBSCRIBERID as log_id,
SUBSCRIBERFIRMID as firm_id,
SUBSCRIBERCONTACTFIRMID as contactfirm_id,
PRODUCTID as product_id,
CAST(YEAR((LOGGEDDATE)) || RIGHT('0' || MONTH((LOGGEDDATE)), 2) || RIGHT('0' || DAYOFMONTH((LOGGEDDATE)), 2) AS INT) as date_key
from  {{ source('preqin', 'tblLoggedStats_Subscribers') }}
where  
--filter records from the end of the month-year of paid subs being captured historically
LOGGEDDATE >= '31-Dec-2014'
--remove outlier records captured on 30-Mar-2016
and LOGGEDDATE::DATE <> '30-Mar-2016'