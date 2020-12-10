
select
currency_id,
'USD' as base_currency_code,
currency_type as currency_code,
currency_name,
currency_rate,
rate_date as start_date,
dateadd( second, -1, LEAD(rate_date, 1, dateadd(d, 1, getdate()::date )) over ( partition by currency_type order by rate_date)) as end_date
FROM  {{ source('preqin', 'tblcurrency_rates_history') }}