SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(rental.rental_id) AS rental_count
FROM actor JOIN film_actor fa ON actor.actor_id = fa.actor_id
JOIN film film ON fa.film_id = film.film_id
JOIN inventory i ON film.film_id = i.film_id
JOIN rental ON i.inventory_id = rental.inventory_id
GROUP BY  actor.actor_id, actor.first_name, actor.last_name
ORDER BY  rental_count DESC
LIMIT 10;