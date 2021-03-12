{{ config(materialized='table', alias='heap_app_sessions') }}

with session_data as
(
    select
        user_id,
        session_id,
        event_id,
        -- because it's possible for the ip/location data to change throughout the session, we will pull the data from the start of the session
        first_value(ip) ignore nulls over (partition by session_id order by event_time) as ip,
        first_value(city) ignore nulls over (partition by session_id order by event_time) as ip_city,
        first_value(region) ignore nulls over (partition by session_id order by event_time) as ip_region,
        first_value(country) ignore nulls over (partition by session_id order by event_time) as ip_country,
        first_value(event_time) ignore nulls over (partition by session_id order by event_time) as session_start_time,
        last_value(event_time) ignore nulls over (partition by session_id order by event_time) as session_end_time
    from {{ ref('heap_fact_app_page_viewed') }} as app_page_viewed
)       
select
    contact_id,
    account_id,
    firm_name,
    legacy_firm_id,
    contact_name,
    sales_region,
    session_id,
    ip,
    ip_city,
    ip_region,
    ip_country,
    session_start_time,
    session_end_time,
    datediff('minute', session_start_time, session_end_time) AS session_length_mins,
    count(distinct event_id) AS app_pageview_count
from session_data
join {{ ref('heap_dimension_user') }} as users
    on session_data.user_id = users.user_id
where contact_id is not null
{{ dbt_utils.group_by(n=14) }}
