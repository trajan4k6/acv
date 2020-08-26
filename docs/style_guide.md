# Mammoth DBT Style Guide & Best Practices

# Overarching Principles
While there are some hard-fast rules, a guide like this can never cover every scenario - nor should it.  When in doubt, weigh your code against the following principles.  By doing so, you will feel more confident in the quality of your code and not have to rely on this document as much.

## Readability
* readable means that someone unfamiliar with the code (reviewer) can easily read the raw source code and understand what is intended
* Points of complexity are obvious
* code has flow and structure to it
* consistent formatting

## Maintainability
* often go hand-in-hand with readability
* maintainable code is typically highly readable
* maintainable means that it adheres to good design such as 
  * [Open Closed Principle](https://en.wikipedia.org/wiki/Open%E2%80%93closed_principle)
  * [Single-responsibility Principle](https://en.wikipedia.org/wiki/Single-responsibility_principle)

## Testability
* Testable code does not have hidden / latent bugs or join fan-outs - produces tight code
* core models that are highly tested have fewer bugs which can ripple through the entire BI stack
* Tested code allows you to make incremental changes to legacy code with confidence
* Tested code allows new people to join a project and feel confident and productive out of the gate
* Tested code sets the example for everyone else.  Be a leader :100:

# Best Practices

## Model configuration

- Model-specific attributes (like sort/dist/partition keys) should be specified in the model.
- If a particular configuration applies to all models in a directory, it should be specified in the `dbt_project.yml` file.
- In-model configurations should be specified like this:

```python
{{
  config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
  )
}}
```


## dbt conventions
* Only `BRONZE` models should select from `source`.
* All other models should only `ref` other models.

## Testing

- Schema definitions should live in a `schemas` subfolder
- At a minimum, unique and not_null tests should be applied to the primary key of each model.  All models should have a unique group of columns to test.

## Naming and field conventions

**Hard Rules** :1st_place_medal:- Violating these will result in a rejected PR
Example: 
> * testability issues
> * failing builds
> * maintainability issues
> 

**Soft Rules** :2nd_place_medal:- One or two should not hold up a PR, but gross violations will need to be fixed
Example: 
> * readability issues
> * formatting issues

**Unimportant**:3rd_place_medal:- things without any strong feelings and should never hold up a PR.  The guide may point this out simply to note that they are not of significant importance.
Example: 
> * mixing upper/lower case on SQL statements
> 

## SQL Guidelines

### Formatting and Style :2nd_place_medal:
*DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEWLINES ARE CHEAP, BRAIN TIME IS EXPENSIVE*

* Schema, table and column names should be in `snake_case`.
* Use names based on the _business_ terminology, rather than the source terminology.
* Table names should be plurals, e.g. `accounts`.
* Timestamp columns should be named `<event>_at`, e.g. `created_at`, and should be in UTC. If a different timezone is being used, this should be indicated with a suffix, e.g `created_at_pt`.  Non-UTC timestamps should be normalized in `Bronze`.
* Booleans should be prefixed with `is_` or `has_`.
* Price/revenue fields should be in decimal currency (e.g. `19.99` for $19.99; many app databases store prices as integers in cents). If non-decimal currency is used, indicate this with suffix, e.g. `price_in_cents`.
* Avoid reserved words as column names
* Consistency is key! Use the same field names across models where possible, e.g. a key to the `customers` table should be named `customer_id` rather than `user_id`.
* Indents should be four spaces
* Long lines should be broken up over multiple lines if improves readability (CASE)
* Field names and function names should all be lowercase
* The `as` keyword should be used when aliasing a field or table
* Fields should be stated before aggregates / window functions.  ie: Group By columns are always listed first
* If joining two or more tables, _always_ prefix your column names with the table alias. If only selecting from one table, prefixes are not needed.
  - Easier to understand which table the columns are referencing - is it on the right or left table in a JOIN?
* Final select should always explicitly list columns - no `s.*`
  - This improves readability and usability when building models which consume core models.
  - You shouldn't have to spend minutes digging around for the right columns
- Any clause with more than one item should be listed on new lines and indented
- Single items can be inline eg: `WHERE foo = bar`
- CASE statemens should begin/end with `CASE / END`.  The rest should be indented
- Multiple BOOL conditions should be on different lines

```sql
...
case 
    when something
        and another
        and even_more = 1
    then result
end as my_col,
...
```

- or conditions should be enclosed in parenthese `()` and extra care must be taken to ensure and/or do not get mixed up

```sql

where
    col1 = 1
    and col2 = 2
    and (
        col3 = 4
        or col4 = 4
    )
```

### SQL Language and Features :1st_place_medal:

- :-1: use of `SELECT DISTINCT` is not allowed.  Exceptions require architect approval.
- :3rd_place_medal: Ordering and grouping by a number (eg. group by 1, 2) is preferred. 
  - :2nd_place_medal:Note that *if you are grouping by more than a few columns, it may be worth revisiting your model design*.
- Prefer `union all` to `union` [*](http://docs.aws.amazon.com/redshift/latest/dg/c_example_unionall_query.html) 
  - :100: understand the difference

#### Joins
- default to `inner join` rather than `left join`
  - Use `left join` only when the right-side table may not have matches and you still want to select  everything from the left-side.
  - this is often the case, but it shouldn't be your default join
- :-1: `right join` is not allowed.  Re-write to use `left join`
- Any pre-filtering on a table in a JOIN should happen within a CTE before the join
- Do not filter on the right-side of a LEFT join within the WHERE predicate
  - This will result in filtering out any NULL values which negates the purpose of a `LEFT OUTER` join
  - Instead either filter in a CTE or alternatively filter in the join predicate
  - `LEFT JOIN right ON left.id = right.id AND right.column = 'foo'`
- Any complicated filtering on a joined table should happen in a CTE before the join
- Specify join keys - do not use `using`. Certain warehouses have inconsistencies in `using` results (specifically Snowflake).

#### CTEs
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do
- CTEs with confusing or noteable logic should be commented
- CTEs that are duplicated across models should be pulled out into their own models or macros


### Example SQL
```sql
with

my_data as (

    select * from {{ ref('my_data') }}

),

some_cte as (

    select * 
    from {{ ref('some_cte') }}
    WHERE foo = 'bar'

),

select
    my_data.field_1,
    my_data.field_2,
    my_data.field_3,

    -- use line breaks to visually separate calculations into blocks
    case
        when my_data.cancellation_date is null 
            and my_data.expiration_date is not null then expiration_date
        when my_data.cancellation_date is null then my_data.start_date + 7
        else my_data.cancellation_date
    end as cancellation_date,

    -- use a line break before aggregations
    sum(some_cte.field_4),
    max(some_cte.field_5)

from 
    my_data
    left join some_cte  
        on my_data.id = some_cte.id

where 
    my_data.field_1 = 'abc'
    and (
        my_data.field_2 = 'def' or
        my_data.field_2 = 'ghi'
    )

group by 1, 2, 3, 4
having count(*) > 1
qualify row_number() over(partition by id order by timestamp) = 1
```
## Model Definition :1st_place_medal:

* Always keep models in subfolders relative to its use and source
  * `models/bronze/salesforce/`
  * `models/silver/dims/`
  * `models/gold/marketing/`
* Name models with the folder structure prefixed
  * `bronze_salesforce_opportunity.sql`
  * `silver_dims_opporutnity.sql`
  * `gold_marketing_opportunity_conversion_dashboard.sql`
* Alias the table name in the model config
```python
#gold_marketing_opportunity_conversion_dashboard.sql
{{
    config(
        alias="opportunity_conversion_dashboard"
    )
}}
```
* Specify schemas at the folder level in the project file
* Schemas should be named after the folder structure
* Schema name should be the same as what was removed from the table alias

```yaml
models:
	client_warehouse:
		gold:
			marketing:
				schema: gold_marketing
			finance:
				schema: gold_finance

```

## Testability :1st_place_medal:

* Each model should have a unique key defined in its schema.yml file.
* Consider what is necessary to make a model unique - often this consists of several columns
```yaml
version: 2
models:
- name: my_model_name
  description: ''
  tests:
  - unique:
      column_name: "concat(user_id, event_name, timestamp)"

```

**More TBD**


## Jinja style guide :3rd_place_medal:

* When using Jinja delimiters, use spaces on the inside of your delimiter, like `{{ this }}` instead of `{{this}}`
* Use newlines to visually indicate logical blocks of Jinja

**More TBD**

## Review Guide

### Mentorship
* Always assume the PR requester is doing their best
* PRs should be seen as a growth opportunity by the requestor, not a failure opportunity
* How are you helping the requester to grow?

### Keepers of standards
* Code should adhere to the core principals laid out in the style guide
* Change requests should be limited to:
  * bugs
  * poor readability
  * poor maintainability
  * insufficient tests
* The need for revisions should stem from the questions:  
  * How would a new person make a change to this code?
  * How obvious is it to alter a rule/logic
  * How likely would this result in bugs?

### Respecting the reviewer's time
* Reviewers are not testers or bug fixers
* Requesters should respect the reviewers time by ensuring all necessary tests have been performed, example SQL provided, and the staging build passes
* A review should not proceed until those conditions are met
* In most cases, Pull Requests should be brief and minimal code changes.  A massive dump of changes will need architecture approval

### Review request process
* set "reviewers":  Primary reviewer, Secondary reviewer
* Assign review to Primary reviewer
* If primary reviewer is unavailable, assign to secondary

### Review turnaround 
* Reviewers are not required to immediately drop everything
* If unable to review within an hour, communicate back to the requester the ETA on a first review via Slack
* While requester should give themselves sufficient time to have the review completed, merged and deployed, the reviewer should also take into consideration the turnaround which may be required for revisions
* If a review is requested in the AM, it's reasonable to expect it to be reviewed by the PM.  
* If requested in the PM, then it's reasonable to expect it to be reviewed by the following AM
* If unable to review in a timely manor, re-assign the PR to the secondary reviewer

