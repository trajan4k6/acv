# Exporting Data
## Use Cases
Most use cases revolve around the idea of dependency management.  By assigning the data warehouse with the responsibility of extracting the data, the warehouse can be locked down to unknown / cloud IP addresses.  Clients can be DB agnostic and not need to worry about storing crednetials / secure information.

* Simplicity
* Security
* Control

### API Integrations
Rather than having clients pull data from the data warehouse, it makes sense to push data into external systems to process.  This can trigger a Lambda, or simply be staged for another process to run at a determiend time.

### Data Science
Notebook can more easily load data from S3 or other cloud storage.  Data Scientists can focus on their process, rather than connectivity issues.

### External Client Exports
Easier to push data to external clients than trust them to store database credentials.

## Requirements
### Snowflake
In order to export to S3, the following DB objects are necessary:
#### File Format
```sql
create or replace file format public.gooten_export_csv_format
  type = csv field_delimiter = '|' null_if = ('NULL', 'null', 'Null') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"';
```
#### Named External Stage
```sql
create or replace stage public.gooten_snowflake_integration url='s3://gooten-snowflake-integration-exports/'
  credentials=(aws_key_id='' aws_secret_key='')
  file_format = public.gooten_export_csv_format;
```
#### Unload SQL
This command is the same syntax whether you are Loading or Unloading.  If you copy into a table, then you are loading.  If you copy into a named stage, then you are unloading

```sql
copy into @public.gooten_snowflake_integration/my_exports/export_prefix_ 
from (
    select * 
    from source_table
) 
file_format=(format_name='public.gooten_export_csv_format', compression='none') 
overwrite = true 
max_file_size=500000
header = true;
```

## Production Ready Integrations w/ DBT
Most of the time, we will want to export only the data that has updated since the last export.

We accomplish this through a series of featues in DBT

### Snapshots
Often we want to export data when certain columns change.  We can leverage the [Snapshot](https://docs.getdbt.com/docs/snapshots) feature in DBT.

We treat the Snapshot SQL as a gold model and use the `check` snapshot strategy. [See example](./snapshot_example.sql)

### Export History Table
We want to keep track of each time the snapshot runs.  A simple table can be used to store the timestamp of each run:

```SQL
CREATE TABLE integrations.export_history(
    TABLE_NAME text,
    LAST_EXPORT timestamp
);
```

### Export Macro
Exports can be made more generic by using a DBT macro to handle the common cases.  This macro accepts a source name and table, and the target name of the integration.

[export_data_to_s3.sql](/macros/export_data_to_s3.sql)

### Executing via DBT Operation

DBT allows you to execute a single macro with input parameters.  We can leverage this feature in a DBT Cloud Job.  We want to run the specific snapshot for this export, then execute the export command

eg:

``` bash
1. dbt snapshot --select contacts_snapshot
2. dbt run-operation export_data_to_s3 --args '{source_name: "hubspot_updates", table_name: "contacts_snapshot", export_name: "hubspot_contact_updates"}'
```

This can be run as frequently as the project requires since it doesn't affect any other DBT run.
