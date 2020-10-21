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
    Fact.*
FROM {{ ref('preqin_fact_usage_data_feeds') }} Fact

