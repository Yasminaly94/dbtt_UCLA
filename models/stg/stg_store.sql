{{ config(materialized='table') }}

select * from {{ source('mysql_sakila','store') }}
where _fivetran_deleted = FALSE