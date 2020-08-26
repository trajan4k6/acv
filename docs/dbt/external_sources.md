# External Sources
### Reference
https://docs.getdbt.com/docs/using-sources

## What are they?
External sources allow you to define a reference to data that is not controled by DBT.  

* Segment data
* Fivetran data
* Stitch data
* Salesforce
* Operational database
* Snapshots
* etc

### Snapshots
These are a special sort of external reference.  Even though they are controled by DBT, they still need to be exposed via a source.

## How to create a source
The DBT docs say that sources go into the `schemas.yml` file, but in actuality, they can go into **any** yml file within the models folder structure.

We recommend splitting up `schema.yml` files into model specific files.  It doesn't make sense to put sources in those files.  

Instead, create a `sources.yml` file in the root of the `models` folder.

### Example

```yaml
version: 2
sources:
  - name: segment_clickfunnels
    database: SEGMENT_INGEST
    schema: ECOURSE_JS
    loader: segment
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: PAGES
        loaded_at_field: received_at
```

**database**: specify the DB the source exists in.
**schema**: specify the schema the source exists in.
**tables**: a list of tables to expose via the source.
**loader**: an informational value specifying the source of the data.
**freshness**: global freshness check parameters for all tables.  This can be overridden in individual tables.


### Using a source
Internally, you use the `source('source_name', 'table_name')` function in your SQL.  This is similar to `ref('model_name')`.

We have a helper macro called `smart_source` which adds further functionality and convenience:

```jinja2
{% macro smart_source(source_name, table_name, timestamp_col = None, lookback_window = 30) %}
  {% if target.name == 'no_data' %}
    {# For CI builds, we will only validate the schema, so sources are always WHERE 1=0 which will ignore data #}
    (select * from {{ source(source_name, table_name) }} where 1 = 0) as __dbt_source_{{ table_name }}
  {% elif target.name != 'prod' and timestamp_col %}
    {# For non-prod builds (dev), we want to limit based on a timerange #}
    (select * from {{ source(source_name, table_name) }} where {{ timestamp_col }} >= current_timestamp - interval '{{ lookback_window }} days' ) as __dbt_source_{{ table_name }}
  {% else %}
    {{ source(source_name, table_name) }}
  {% endif %}
{% endmacro %}
```

#### Usage
You use just like the `source()` function except you can optionally specify a timestamp column and lookback value.  If in a non-prod environment, it will only select limited amount of data, speeding up development.

```sql
SELECT
	ID AS account_id,									
	Customer_Number,				
	Firstname as first_name,
	LastName as last_name,							
	Name,						
	PRIMARY_EMAIL,
	PHONE,								
	PERSONHOMEPHONE as person_home_phone,				
	PERSONMOBILEPHONE as person_mobile_phone,
	CREATEDDATE as created_date,		
	LASTMODIFIEDDATE as last_modified_date,			
	ACQUISITION_PROGRAM,
	ACQUISITION_DATE,					
	ARCID as arc_id,	
	CreatedByID as created_by_id,
    _FIVETRAN_SYNCED as synced_at
FROM
    {{ smart_source('salesforce_snapshot', 'account_snapshot', '_FIVETRAN_SYNCED')}}
```