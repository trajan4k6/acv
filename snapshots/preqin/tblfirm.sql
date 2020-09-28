{% snapshot tblfirm_snapshot %}

{{
    config(
      target_database='db_raw',
      target_schema='PREQIN01_FIVETRAN_DBO_DEV',
      unique_key='Firm_Id',

      strategy='check',
      check_cols='all'
  

    )
}}

select * from {{source('preqin', 'tblfirm')}}

{% endsnapshot %}
