{{
    config(
        materialized='incremental', 
        alias='fact_pro_key_actions'
    ) 
}}

SELECT 
    event_id,
    time AS event_time,
    user_id,
    session_id,
    event_table_name,
    split_part(event_table_name, '_pro_key_actions_', 2) AS event_name
FROM {{ source('heap', 'all_events') }}
WHERE event_table_name ILIKE '%_pro_key_actions_%'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and event_time > (select max(event_time) from {{ this }})

{% endif %}
