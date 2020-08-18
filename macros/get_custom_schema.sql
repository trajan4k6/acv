--https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas/#advanced-custom-schema-configuration

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}