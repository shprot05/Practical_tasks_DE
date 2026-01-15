select film.film_id 
from film
left join inventory
ON film.film_id = inventory.film_id
where inventory_id is NULL
order by film.film_id
