{{ config(materialized='table') }}

select * from {{ source('mysql_sakila','staff') }}
where _fivetran_deleted = FALSE