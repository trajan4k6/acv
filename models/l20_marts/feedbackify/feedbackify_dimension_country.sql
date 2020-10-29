{{ config(materialized='table') }}

WITH country_map AS (
        SELECT distinct
            region_name,
            trim(trim(REGEXP_SUBSTR(REGION_NAME,'\\(([^)]*)\\)'),'('),')')          as country
        FROM  "DB_ANALYTICS_TEST1"."FEEDBACKIFY"."FEEDBACKIFY_DIMENSION_REGION"),

country_transform AS (
        SELECT distinct
            decode(country,null,region_name,country)  as country
            FROM country_map),

country_rename AS (
        SELECT 
            case when country = 'United States' then 'United States of America'
            when country = 'Russian Federation' then 'Russia'
            when country = 'Korea, Republic of' then 'South Korea'
            else country
            end as country
        FROM country_transform)

SELECT 
            {{ dbt_utils.surrogate_key(
                [4,'country']
            ) }}                          as dimension_country_key,
            country                       as country_name,
            5                             as datasource_id
FROM 
country_rename 
WHERE 
country is not null