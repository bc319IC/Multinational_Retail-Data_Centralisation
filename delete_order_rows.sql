/*
SELECT store_code 
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
*/
/*
SELECT user_uuid, COUNT(*) AS value_count
FROM orders_table 
GROUP BY user_uuid
ORDER BY value_count DESC;
*/

DELETE FROM orders_table 
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

DELETE FROM orders_table 
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);
DELETE FROM dim_card_details 
WHERE card_number NOT IN (SELECT card_number FROM orders_table);

DELETE FROM orders_table 
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

DELETE FROM orders_table 
WHERE product_code NOT IN (SELECT product_code FROM dim_products);

DELETE FROM orders_table 
WHERE date_uuid NOT IN (SELECT date_uuid FROM dim_date_times);