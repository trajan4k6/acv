{% docs __overview__ %}
# Moneyball Project

## Background
Discussions with stakeholders across Preqin identified a number of challenges gaining access 
to information required for decision making.

* Gaps in data availability (predominately ACV and Pro usage).
* Challenges linking data from different sources (e.g. usage versus ACV, revenue attribution).
* Large amount of manual effort producing reports – often on a regular basis.
* Lack of clarity around “source of truth” for key data points, with reports from different 
  sources often not agreeing.

## Objective
To improve both the breadth and quality of management and operational reporting, by establishing 
a data platform of key Preqin data, surfaced on PowerBI dashboards and significantly reducing time 
spent manually producing reports via Excel.


To run the project in DBT:
1. Clone [this repo](https://github.com/Preqin-DataEngineering/snowflake-poc).
2. Create a profile named `profiles.yml` in your .dbt directory, and point to the Preqin Snowflake instance.
3. Run `dbt deps`.
4. Run `dbt seed`.
5. Run `dbt run`.
6. Run `dbt test`.

{% enddocs %}