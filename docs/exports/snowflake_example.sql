USE DATABASE WAREHOUSE;
create or replace file format public.gooten_export_csv_format
  type = csv field_delimiter = '|' null_if = ('NULL', 'null', 'Null') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"';

create or replace stage public.gooten_snowflake_integration url='s3://gooten-snowflake-integration-exports/'
  credentials=(aws_key_id='' aws_secret_key='')
  file_format = public.gooten_export_csv_format;
 
grant usage on stage public.gooten_snowflake_integration to role dbt_transforms;
grant usage on file format public.gooten_export_csv_format to role dbt_transforms;

BEGIN;

copy into @public.gooten_snowflake_integration/{{ export_name }}/{{export_name}}_ 
from (
    select * 
    from {{ table_reference }}
    WHERE 
        email is not null
        AND DBT_UPDATED_AT > COALESCE((SELECT max(last_export) 
                                     FROM {{ ref("export_history") }} 
                                     WHERE table_name = '{{table_reference}}')
                                     , '2000-01-01')
) 
file_format=(format_name='public.gooten_export_csv_format', compression='none') 
overwrite = true 
max_file_size=500000
header = true;

INSERT INTO {{ ref('export_history') }} (table_name, last_export) VALUES( '{{ table_reference }}' , CURRENT_TIMESTAMP::timestamp_ntz);
COMMIT;
