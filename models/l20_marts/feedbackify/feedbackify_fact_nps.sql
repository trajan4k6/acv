{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.surrogate_key(
        'FEEDBACK_ID'
    ) }}                                    AS ROW_ID,
  FEEDBACK_ID,
  CAST(YEAR((DATE::DATE)) || RIGHT('0' || MONTH((DATE::DATE)), 2) || RIGHT('0' || DAYOFMONTH((DATE::DATE)), 2) AS INT) AS DATE_KEY,    
  COALESCE(B.DIMENSION_BROWSER_KEY,'-1')    AS DIMENSION_BROWSER_KEY,
  COALESCE(O.DIMENSION_OS_KEY,'-1')         AS DIMENSION_OS_KEY,
  COALESCE(R.DIMENSION_REGION_KEY,'-1')     AS DIMENSION_REGION_KEY,
  SCORE
FROM
    {{ source('feedbackify', 'net_promoter_score') }} n

LEFT JOIN {{ ref('feedbackify_dimension_browser') }} b
    ON n.browser = b.browser

LEFT JOIN {{ ref('feedbackify_dimension_os') }} o
    ON n.os = o.os

LEFT JOIN {{ ref('feedbackify_dimension_region') }} r
    ON n.trim(trim(REGEXP_SUBSTR(location,'\\(([^)]*)\\)'),'('),')') = r.country_name