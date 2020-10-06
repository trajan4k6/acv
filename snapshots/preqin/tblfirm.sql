{% snapshot tblfirm_snapshot %}
  {{ config(
    target_database = 'db_raw',
    target_schema = 'PREQIN01_PROD_RIVERY',
    unique_key = 'Firm_Id',
    strategy = 'check',
    check_cols = 'all'
  ) }}

  SELECT
    *
  FROM
    {{ source(
      'preqin',
      'tblfirm'
    ) }}
{% endsnapshot %}
