(SELECT 'Города на а' AS group_name, category.name,
 COUNT(*) AS rentals_count 
 FROM category
 JOIN film_category ON category.category_id = film_category.category_id
 JOIN film ON film_category.film_id = film.film_id
 JOIN inventory ON film.film_id = inventory.film_id
 JOIN rental ON inventory.inventory_id = rental.inventory_id
 JOIN customer ON rental.customer_id = customer.customer_id
 JOIN address ON customer.address_id = address.address_id
 JOIN city ON address.city_id = city.city_id
 WHERE city.city LIKE 'a%' 
 GROUP BY category.name
 ORDER BY rentals_count DESC 
 LIMIT 1)

UNION ALL 

(SELECT 'Города с -' AS group_name, category.name, COUNT(*) AS rentals_count
 FROM category
 JOIN film_category ON category.category_id = film_category.category_id
 JOIN film ON film_category.film_id = film.film_id
 JOIN inventory ON film.film_id = inventory.film_id
 JOIN rental ON inventory.inventory_id = rental.inventory_id
 JOIN customer ON rental.customer_id = customer.customer_id
 JOIN address ON customer.address_id = address.address_id
 JOIN city ON address.city_id = city.city_id
 WHERE city.city LIKE '%-%' 
 GROUP BY category.name
 ORDER BY rentals_count DESC
 LIMIT 1);
