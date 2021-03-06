{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        [3,'user.user_id']
    ) }} AS dimension_user_key,
    /*{{ dbt_utils.star(
            from = ref('heap_dimension_firm'),
            except = []
        ) }},*/
    user.*, 
    COALESCE(preqinInd.dimension_individual_key, '-1') AS conformed_dimension_individual_key,
    COALESCE(heapFirm.dimension_firm_key, '-1')  AS dimension_firm_key,
    COALESCE(preqinInd.dimension_firm_key, '-1')  AS conformed_dimension_firm_key,
    3 AS datasource_id
FROM
    {{ ref('heap_dimension_user') }} user
    LEFT
    JOIN {{ ref('heap_dimension_firm_integrated') }} heapFirm
        ON heapFirm.account_id= user.Account_ID
    LEFT 
    JOIN {{ ref('preqin_dimension_individual') }} preqinInd
        ON user.Identity = TO_CHAR(preqinInd.ContactFirm_Id)