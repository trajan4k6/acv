{% macro grant_select_on_schemas(schemas, role) %}
  {% for schema in schemas %}
-- NOTE  requires executing role to have the following:
-- use role securityadmin;
-- grant manage grants on account to role dbt_transforms;

    --grant usage on schema {{ schema }} to role {{ role }} with grant option;
    grant select on all tables in schema {{ schema }} to role {{ role }};
    grant select on all views in schema {{ schema }} to role {{ role }};
    --grant select on future tables in schema {{ schema }} to role {{ role }};
    --grant select on future views in schema {{ schema }} to role {{ role }};
  {% endfor %}
{% endmacro %}