{{ config(materialized='table') }}

SELECT 
    {{ dbt_utils.surrogate_key(['f.film_id']) }} as FILM_KEY,
    f.film_id,
	f.title AS film_title,
	l.name AS LANGUAGE,
	c.name AS category_name,
	f.rental_duration,
	f.RENTAL_RATE,
	f.REPLACEMENT_COST,
	f.LENGTH
from {{ ref('stg_film')}} f
inner join {{ ref('stg_language')}} l
on f.language_id = l.language_id
inner join {{ ref('stg_film_category')}} fc 
on f.film_id = fc.film_id
inner join {{ ref('stg_category')}} c 
on fc.category_id = c.category_id