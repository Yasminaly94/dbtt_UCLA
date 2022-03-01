{{ config(materialized='table') }}

select 
    {{ dbt_utils.surrogate_key(['s.store_id'])}} as STORE_LOCATION_KEY,
    s.store_id,
	a.postal_code,
	c.city,
	a.district,
	co.country,
	a.phone
from 
    {{ ref('stg_store')}} s
inner join {{ ref('stg_address')}} a    
on s.address_id = a.address_id
inner join {{ ref('stg_city')}} c    
on a.city_id = c.city_id   
left join {{ ref('stg_country')}} co
on c.country_id = co.country_id