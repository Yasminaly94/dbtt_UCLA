{{ config(materialized='table') }}

select * from {{ source('mysql_sakila','address') }}
where _fivetran_deleted = FALSE
