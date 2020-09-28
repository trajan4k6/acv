{% snapshot testsb_snaphot %}

{{
    config(
      target_database='DB_ANALYTICS',
      target_schema='DBT_SBADCOCK',
      unique_key='Firm_Id',

      strategy='check',
      check_cols='all'
  

    )
}}

--select * from {{ ref('dimension_firm') }}
select * from {{source('preqin', 'tblfirm')}}

{% endsnapshot %}
