SELECT city.city,
COUNT(*) FILTER (WHERE customer.active = 1) AS active_customers,  
COUNT(*) FILTER (WHERE customer.active = 0) AS inactive_customers
FROM customer 
JOIN address ON customer.address_id = address.address_id
JOIN city ON address.city_id = city.city_id
GROUP BY city.city_id, city.city
ORDER BY inactive_customers DESC;
