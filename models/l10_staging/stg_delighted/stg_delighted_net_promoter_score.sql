--https://app.delighted.com/docs/api/listing-survey-responses

WITH source AS (
SELECT
id,
person,
surveytype,
score,
comment,
permalink,
TO_TIMESTAMP_LTZ(created_at) AS created_at_timestamp,
TO_TIMESTAMP_LTZ(updated_at) AS updated_at_timestamp,
PARSE_JSON(additional_answers) AS additional_answers,
PARSE_JSON(person_properties) AS person_properties
FROM  {{ source('delighted', 'net_promoter_score') }}
)

SELECT 
id,
person,
surveytype,
score,
comment,
permalink,
created_at_timestamp,
updated_at_timestamp,
(additional_answers)[0].question.text::VARCHAR AS additional_question,
(additional_answers)[0].value.free_response::VARCHAR AS additional_response,
--Person properties
(person_properties):"Delighted Browser"::VARCHAR AS browser,
(person_properties):"Delighted Country"::VARCHAR AS browser_country,
(person_properties):"Delighted Device Type"::VARCHAR AS device_type,
(person_properties):"Delighted Operating System"::VARCHAR AS OS,
(person_properties):"Delighted Page"::VARCHAR AS delighted_page,
(person_properties):"Delighted Page URL"::VARCHAR AS delighted_page_url,
(person_properties):"Delighted Referrer URL"::VARCHAR AS delighted_referrer_url_referrer_URL,
(person_properties):"Delighted Source URL"::VARCHAR AS delighted_source_URL,
(person_properties):contactFirmId::VARCHAR AS ContactFirmId,
(person_properties):firmId::VARCHAR AS FirmId
FROM source