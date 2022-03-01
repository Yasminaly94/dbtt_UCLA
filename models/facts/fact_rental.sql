{{ config(materialized='table') }}

select 
    {{ dbt_utils.surrogate_key(['d.DATE_KEY'])}} as RENTAL_DATE_KEY,
    {{ dbt_utils.surrogate_key(['od.TIMEOFDAY_KEY'])}} as RENTAL_TIMEOFDAY_KEY,
    //HOUR(r.rental_date),
    //r.rental_date,
    r.rental_id,
    c.CUSTOMER_KEY,
    f.FILM_KEY,
    sd.staff_key,
    sl.STORE_LOCATION_KEY,
    1 as rental_quantity,
    p.amount
from
    {{ ref('stg_rental') }} r
inner join {{  source('DBT_YALY','DIM_DATE') }} d
on to_number(to_varchar(to_date(r.rental_date),'YYYYMMDD')) = d.DATE_KEY
inner join {{  source('DBT_YALY','DIM_TIMEOFDAY') }} tod
on HOUR(r.rental_date) = tod.HROFDAY AND MINUTE(r.rental_date) = tod.MINOFDAY
inner join {{ ref('dim_customer') }} c
on r.customer_id = c.customer_id
inner join {{ ref('stg_inventory') }} i
on r.inventory_id = i.inventory_id
inner join {{ ref('dim_store') }} sl
on i.store_id = sl.STORE_ID
inner join {{ ref('dim_film') }} f
on f.film_id = i.film_id
inner join {{ ref('dim_staff') }} sd
on r.staff_id = sd.STAFF_ID
inner join {{ ref('stg_payment') }} p
on r.rental_id = p.rental_id
WHERE
    to_date(r.rental_date) < '2020-01-01'