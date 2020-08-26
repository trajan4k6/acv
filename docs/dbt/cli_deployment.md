# Testing DBT locally

DBT can be run two ways:

1. __Locally:__ Local DBT tests are run via the CLI for testing new models and improvements
2. __In production__ using DBT Cloud once local tests have been cleared and pushed to GitHub. Refer to the [documentation](docs/review_process.md) detailing the review process.

# CLI Setup

A __local__ setup of DBT will include:
* Installing DBT via `pip`
* Configuring or cloning a DBT project
* Customizing your `profiles.yml` file.

See the project [README.md](/README.md) file for additional information and the source documentation [here](https://docs.getdbt.com/docs/installation).

# Profile Config

Your `profiles.yml` file installs automatically when DBT is downloaded, but will need some configuration. In order to locate this file, run this command in the command line console: `$ dbt debug --config-dir`. You will be able to locate your `profiles.yml` file from there. Alternately, you can navigate to its default installation location in the `~/.dbt/` folder.

This file can contain multiple profiles: That is, it can point to multiple databases under different circumstances. In general, each __profile__ within the `profiles.yml` file should point to a unique `dbt_project.yml` file __of the same name__.

## Profile Best Practices

The profiles below are configured to connect to Snowflake databases, but can connect to any [supported databases](https://docs.getdbt.com/docs/supported-databases). There are a few important elements of a local DBT profile configuration:

* Your database credentials
  * You will be connecting to and testing in a `_DEV` database. Bronze models will deploy into a `BASE_DEV` database, while Silver and Gold     models will deploy in an `ANALYSIS_DEV` database.
* To keep your work separate from other developers working in the same database, you will specify a custom schema. This will generally be your name to allow you to test models in the appropriate `_DEV` database as `NAME_MODEL`.

## Profile Examples

Below are two profiles contained within the same `profiles.yml` file. Each is named to reflect the database it references - `base` for Bronze models and `analysis` for Silver and Gold models.

For more information on any of the fields below, see the [DBT profile documentation](https://docs.getdbt.com/docs/profile-snowflake)

```
CLIENT-snowflake:
  target: dev *the profile target is named `dev` here and defined below; another target may also be specified*
  outputs:
    dev:
      type: snowflake
      account: mf16282     *the url prefix for your snowflake connection*
      user: mg_erin        *the username for your Snowflake account*
      password:            *the password for your Snowflake account*
      role: SYSADMIN
      database: WAREHOUSE_DEV
      warehouse: DBT_TRANSFORMS
      schema: erin
      threads: 1
      client_session_keep_alive: False

```

# CLI Commands

After your `profiles.yml` file is configured and your model(s) is/are set up, you are ready to run DBT tests locally. This is a relatively simple process involving a few commands:

1. `dbt deps`
2. `dbt seed`
3. `dbt compile`
4. `dbt test`
5. `dbt run`

The [documentation](https://docs.getdbt.com/docs/command-line-interface) for each of these commands gives more detail on each of them.
