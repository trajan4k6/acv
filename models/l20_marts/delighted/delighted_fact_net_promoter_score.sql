{{ config(materialized='table') }}

SELECT
 n.ID AS feedback_id,
 to_varchar(created_at_timestamp, 'YYYYMMDD')::int AS date_created_key,
  COALESCE(b.dimension_browser_key,'-1')    AS dimension_browser_key,
  COALESCE(o.dimension_operating_system_key,'-1')         AS dimension_operating_system_key,
  COALESCE(c.dimension_country_key,'-1')        AS dimension_country_key,
  COALESCE(i.dimension_individual_key, '-1')    AS conformed_dimension_individual_key,
  COALESCE(f.dimension_firm_key, '-1')          AS conformed_dimension_firm_key,
  score AS feedback_score,
  comment AS feedback_comment,
  additional_response AS feedback_additional_response,
  permalink AS feedback_url,
  6 AS datasource_id
FROM
    {{ ref('stg_delighted_net_promoter_score') }} n

LEFT JOIN {{ ref('delighted_dimension_browser') }} b
    ON n.browser = b.browser

LEFT JOIN {{ ref('delighted_dimension_operating_system') }} o
    ON n.os = o.operating_system

LEFT JOIN {{ ref('delighted_dimension_country') }} c
    ON  n.browser_country = c.country_name

LEFT JOIN {{ ref('preqin_dimension_individual') }} i
    ON TRY_TO_NUMERIC(n.contactfirmid) = i.Contactfirm_id

LEFT JOIN {{ ref('preqin_dimension_firm') }} f
    ON TRY_TO_NUMERIC(n.firmid) = f.firm_id
WHERE
    surveytype = 'nps'