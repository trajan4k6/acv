# Ingesting External Data into Snowflake

### Using DBT

Often a client has data which cannot be ingested into Snowflake via a service (Stitch, Fivetran).  Instead the data is landed on S3 to be ingested into Snowflake.

## Ingest Methods


Snowflake has two ways of ingesting data:  COPY INTO and Snowpipes. These two methods are mutually exclusive.

Both methods leverage a STAGE which manages access to resources such as a S3 bucket.

### Snowpipe

<https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe.html>

Snowpipes are for continuously ingesting data.  An example use-case for this would be if there was a Kinesis Firehose continuously dumping data into S3.  They monitor an S3 bucket or subscribe to a SQS queue of event notifications. Alternatively the Snowpipe can be triggered via REST api call.

The Snowpipe is a thin wrapper around a COPY INTO command.  The purpose is to have an external system trigger the ingest.

![](https://lh5.googleusercontent.com/pqiFRGY9l56KB15OLR9DmeRMfI8wgAndXWoL_VEIvTxDgdrvcU-RttX5ptqEsg8p0oy2I89msmub9_XE2G62q2XctUHGiK1UviN1TSD271bH3SKuR3uuiG6bjBaqjBkREhiocfa-)

If there's a need to continuously load data, and all downstream models are views and don't require a DBT run for that data to be visible, then this method could be used.  However, most of the time it is preferable for DBT to control when data is loaded as further transforms may be needed. In most cases, it is recommended to call COPY INTO directly from DBT

### COPY 

<https://docs.snowflake.net/manuals/user-guide/data-load-s3.html>

Like the Snowpipe method, COPY requires a 'stage' to be created which controls access to file resources such as S3.

The COPY command can read and write to the stage.  In this document, we are focusing on reading.

![](https://lh3.googleusercontent.com/XxQ6XDGO5V7YQtVpDZjnK3KvymWKHHEMx4w_qNQzcsxzblvfLOe6RPd4TIT62fVms7HGoij19Z5MkiVqdMzCYAGg3dmc4aApj11VedLeoomBSFRxNSYzej7d-S2_dYd7t-zFt_4h)

When using the COPY command, Snowflake will keep track of which files are successfully loaded and only process once.  Subsequent executions will not read from previously loaded files.

The COPY command will return a report of which files were processed, and any errors reading the files.

## Ingest Targets

We want to ingest the data as if it was loaded via an external service (Stitch, Fivetran).  This means creating a database external to the regular Warehouse / DBT project database. It is recommended that a single database be created for this use case.  Ie EXTERNAL_INGEST

Each ingest source should have its own schema.  For example, if loading data from an Adjust webhook event, create schema EXTENRAL_INGEST.ADJUST.  If loading data from CSV exports from a client's mySQL database: create schema EXTERNAL_INGEST.MAGENTO.

Each individual event/table from that source should have its own table within the schema.  This will have to be manually created and the schema based on the ingested data.

### Security/Permissions

When manually creating databases, schemas, and tables, permissions will need to be manually granted to the role which DBT will run as.

```sql 
CREATE DATABASE IF NOT EXISTS EXTERNAL_INGEST;

GRANT USAGE, CREATE SCHEMA ON DATABASE EXTERNAL_INGEST TO ROLE DBT_TRANSFORMS;

CREATE SCHEMA IF NOT EXISTS EXTERNAL_INGEST.magento;

GRANT USAGE ON SCHEMA EXTERNAL_INGEST.magento TO ROLE DBT_TRANSFORMS;

CREATE OR REPLACE TABLE external_ingest.magento_stage.admin_user_stage (

CREATED TIMESTAMP NULL,

EMAIL TEXT NULL,

EXTRA TEXT NULL,

FAILURES_NUM TEXT NULL,

FIRSTNAME TEXT NULL,

FIRST_FAILURE TEXT NULL,

INTERFACE_LOCALE TEXT NULL,

IS_ACTIVE TEXT NULL,

LASTNAME TEXT NULL,

LOGDATE TEXT NULL,

LOGNUM TEXT NULL,

MODIFIED TEXT NULL,

PASSWORD TEXT NULL,

RELOAD_ACL_FLAG TEXT NULL,

RP_TOKEN TEXT NULL,

RP_TOKEN_CREATED_AT TEXT NULL,

USERNAME TEXT NULL,

USER_ID TEXT NOT NULL

);

GRANT SELECT,INSERT,DELETE ON TABLE external_ingest.magento_stage.admin_user_stage TO DBT_TRANSFORMS;
```

## Snowflake Stages

### File Format

The file format should be source specific.  Each set within a source should share the common file format.  In cases where the format varies between sets, multiple file formats can be defined.  Different sources will likely have different file format requirements.

<https://docs.snowflake.net/manuals/sql-reference/sql/create-file-format.html>

File formats describe how Snowflake should load and parse the file.  

-   CSV vs JSON

-   gzip vs raw

-   UTF8 vs ASCII

-   comma vs tabs

-   date/time formats

If timestamps are consistent, then one can specify the timestamp format.  If they are NOT, then the file will fail to load if a format that cannot be parsed is provided.  Often it's best to parse the timestamp in the COPY command.

Eg:

create or replace file format external_ingest.magento_stage.magento_csv_format

  type = csv field_delimiter = ',' null_if = ('NULL', 'null', 'Null','') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = AUTO;

### External Stage

The external stage points to an S3 bucket / prefix in which to look for files to load.  The stage can be associated with the file format to simplify the COPY command. The stage should also be specific to the source and the S3 path as specific as possible.

#### Authentication

A programmatic IAM user (does not need to log into AWS console) should be created in the client's AWS account which only has the required access for any import/export of data.  For simplicity sake, a single user can be used for any ingest/export.

The user's Key and Secret are needed

Eg:

create or replace stage external_ingest.magento_stage.magento_imports url='s3://mammothgrowth-nutrafol-snowflake-imports/magento/'

  credentials=(aws_key_id='AKIATPYPJBNPYTVXXXX' aws_secret_key='u7TiHu8EdlwktK+lRs01d5UGDfUJELXXXXXXX')

  file_format = external_ingest.magento_stage.magento_csv_format;

The stage and file format should be created manually within Snowflake's UI.  The Key and Secret should NOT be stored in Github or any other common file. They should be stored as a Secure Note in Lastpass.

Permissions will need to be granted to the role which DBT runs under:

GRANT USAGE ON FILE FORMAT external_ingest.magento_stage.magento_csv_format TO ROLE dbt_transforms;

GRANT USAGE ON STAGE external_ingest.magento_stage.magento_imports TO ROLE dbt_transforms;

## Ingesting from DBT

We trigger the ingest via DBT's run-operation command.  This runs an arbitrary macro without needing to define any models.  This command can then be run independently from the main run.

### DBT Operation

<https://docs.getdbt.com/docs/using-operations>

The operation needs to be enclosed in a call statement() function

<https://docs.getdbt.com/docs/statement-blocks>

```sql
{% macro ingest_linkedin_leadgen() %}

{%- call statement() -%}

<MACRO>

{%- endcall -%}

{%- endmacro %}

With Snowflake, the SQL should also be wrapped in a BEGIN/COMMIT SQL block.

BEGIN;

COPY INTO {{ source( "marketo_ingest", "campaigns" )}}

(

campaign_id,

is_active,

is_system,

name,

status,

TYPE,

updated_at,

created_at,

original_json

) FROM (

select

  $1:id,

  $1:isActive,

  $1:isSystem,

  $1:name,

  $1:status,

  $1:type,

  to_timestamp($1:updatedAt::text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"+TZHTZM'),

  to_timestamp($1:createdAt::text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"+TZHTZM'),

  $1

from @EXTERNAL_INGEST.MARKETO_INGEST.marketo_asset_stage/campaigns

)

ON_ERROR =CONTINUE

;

COMMIT;
```

While this may have been fixed, the way DBT runs the statement block, it gets rolled back and the end of the statement if not explicitly committed.

### Transforms

Don't do any transforms at this stage other than basic cleanup.  Snowflake will create problems if there are any complex transforms/joins.

Try and keep any source file format options within the FILE FORMAT rather than within the COPY command.  Each source should have a consistent file format across data sets, but every source will have a different file format requirement.

## Running in DBT Cloud

Operations are run using dbt run-operation <macro name>

There should be a seperate job for every specific operation and any supporting commands.  For example, once the data is imported, other models might need to be triggered to refresh.  You would want this to happen outside of the normal run - on a different schedule and not within the CI builds.

![](https://lh5.googleusercontent.com/aj-_VjqdOCAK2OcgKBbFKFu9vLtpjq8k2jRRDO3wE_lVhIn54oEhWJaCtnSQegQlyhE3MeSBBTeMVytjRNhIuBi76JNnA0nbmSitpFpT1jD-QPqO3H4HAtTpwWSX2EnDkI3x1hyb)

<https://cloud.getdbt.com/#/accounts/1469/projects/1218/jobs/2389/settings/>

In this example, the hubspot exports depend on leads ingesting, those leads getting built into the users model, a snapshot taken to define what needs to be exported to S3, then the exports run.  This is a complex orchestration which requires each step to be run before the final command.

## Ingest Tips

### Use Source

Use a DBT source as the target of your ingest.  Eg:

COPY INTO {{ source( "marketo_ingest", "campaigns" )}}

This way you are ensuring that the ingest goes into the same place your models will eventually build from.

### Clean up old files

In most cases, you will want to clean up files after you have ingested them.  We want to treat the EXTERNAL_INGEST data as the 'data lake' or source of truth.  There may be cases where the client is treating S3 as their source of truth. DO NOT clean up files in this case.

Make sure this is approved by the client.

There are two methods to clean up files:

-   Configure S3 to auto-delete after X days.  

-   Bucket level policy, so should only use when there is an explicit bucket for ingesting data.

-   Have Snowflake PURGE on successful file load.  

-   <https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>

Since data could potentially be expired in S3 before loading if DBT is malfunctioning, using the PURGE option is preferred. Be careful to ensure that we WANT to clean up ingested files.

### Source Freshness

An easy way to alert to when there's a problem with the ingest pipeline is to leverage DBT's Source Freshness command

<https://docs.getdbt.com/docs/using-sources#section-snapshotting-source-data-freshness>

Eg:

version: 2

sources:

  - name: snowplow

    database: raw

    loader: emr # optional, informational only

    freshness:

      warn_after: {count: 12, period: hour}

      error_after: {count: 24, period: hour}

    tables:

      - name: event

        loaded_at_field: collector_tstamp # required for freshness snapshotting

If the max value of the specified timestamp is ever > 24 hours old, the command will fail on that source.

![](https://lh3.googleusercontent.com/MkSrpP42iHp4K297LjHtFLzk83eVFX-FWLPvgdMj8zQCNQ7V9wfoK-Jlp-Zi6YH0dSy-ohAtuTw5kTX5h946CU9fanNnUHj_8kT6iJ8nSvqOTh6j4Ey82MkTaSVqzv1Vx-zmgMZC)

#### Create a Job

Create a job which runs dbt source snapshot-freshness command.  Choose a schedule which will take into account when you expect your sources to be populated each day.

![](https://lh6.googleusercontent.com/73sWPYP7xpQhSpXsaWamXldK195Lhy-IJoo2vXnZScDd8aqan0gZPtFTQyJTOg8ExNFP6pdGzjDlv8yYVOrcymoIwBcOSOMW5Wp6YhId4vKGua4gIfZ2IFxP5Dbe2NU3_6gT3RTS)

#### Configure Slack/Email notifications

![](https://lh3.googleusercontent.com/MC200bzF5jjQzTmQwCCSrksGh58j2PMQuXRqDlH-gCmMxOBHIHb01eKqPvEhRed7K_U9A4U1oa9xPeKdsS3bGoUxOOicXiZ5d-ltWEvZX8fJDogfXgbIPC6-yYugUCMWaB1LTr-A)

When the Source Freshness job fails, trigger an Email/Slack notification.

Now if an upstream process is broken, we will be notified after 24 hours.