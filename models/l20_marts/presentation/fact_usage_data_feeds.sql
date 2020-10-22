{{ config(
    materialized = 'view',
    unique_key = [],
    tags = []
) }}

SELECT 
  /*{{ dbt_utils.star(
            from = ref('preqin_fact_usage_data_feeds'),
            except = []
        ) }},*/
    Fact.*,
    1 AS PAGELOG_COUNT
FROM {{ ref('preqin_fact_usage_data_feeds') }} Fact

