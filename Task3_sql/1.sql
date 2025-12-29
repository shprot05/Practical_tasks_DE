SELECT name, count(film_id) AS film_count 
FROM category JOIN film_category
ON category.category_id = film_category.category_id
GROUP BY name
ORDER BY film_count DESC
