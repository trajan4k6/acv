{% snapshot account_snapshot %}
  {{ config(
    target_database = 'db_raw',
    target_schema = 'salesforce_rivery',
    unique_key = 'ID',
    strategy = 'timestamp',
    updated_at = 'lastmodifieddate'
  ) }}

  SELECT
  *  
  FROM
    {{ source(
      'salesforce',
      'account'
    ) }}
{% endsnapshot %}
