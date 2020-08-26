{% macro export_data_to_s3(source_name, table_name, export_name) %}
{% set table_reference = source(source_name, table_name) %}

{%- call statement(auto_begin=False) -%}

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

{%- endcall -%}

{%- endmacro %}