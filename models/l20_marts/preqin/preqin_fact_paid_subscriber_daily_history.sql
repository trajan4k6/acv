{{
    config(
        materialized='incremental',
        cluster_by=['date_key']
    ) 
}}

with cte_daily_snapshot as (
select 
max(log_id) as log_id,
date_key,
firm_id,
contactfirm_id,
product_id
from {{ ref ('stg_tblLoggedStats_Subscribers') }}
group by 2, 3, 4, 5
)

select 
subs.log_id,
subs.date_key,
ind.dimension_individual_key    as dimension_individual_key,
firm.dimension_firm_key         as dimension_firm_key,
prod.dimension_product_key      as dimension_product_key,
prod.dimension_asset_class_key
from cte_daily_snapshot subs
join {{ ref('preqin_dimension_individual') }} ind
    ON subs.contactfirm_id = ind.contactfirm_id
join {{ ref('preqin_dimension_firm') }} firm
    ON subs.firm_id = firm.firm_id
join {{ ref('preqin_dimension_product') }} prod
    ON subs.product_id = prod.product_id


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and log_id > (select max(log_id) from {{ this }})

{% endif %}