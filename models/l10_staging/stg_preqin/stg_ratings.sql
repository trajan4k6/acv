{{ config(materialized='table') }}

with stg_ratings as (

    select 
    RATINGID,
    RATINGDESC,
    SORTORDER,
    FITCHDESC,
    SANDPDESC,
    MOODYSDESC
    from
    DB_RAW.PREQIN01_FIVETRAN_DBO.TBLCLORATINGS

)

select *
from stg_ratings