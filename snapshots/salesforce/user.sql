{% snapshot user_snapshot %}

{{
    config(
      target_database='db_raw',
      target_schema='salesforce_rivery_uat',
      unique_key='ID',

      strategy='timestamp',
      updated_at='lastmodifieddate'
  

    )
}}

select * from {{source('salesforce', 'user')}}

{% endsnapshot %}

