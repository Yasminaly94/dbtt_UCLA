{{ config(materialized='table') }}

select * from {{ source('mysql_sakila','actor') }}
where _fivetran_deleted = FALSE
