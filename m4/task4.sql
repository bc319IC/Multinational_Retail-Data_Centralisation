SELECT 
	COUNT(o.product_code) AS number_of_sales,
	SUM(o.product_quantity) AS product_quantity_count,
    CASE 
        WHEN o.store_code LIKE 'WEB-%' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table o
JOIN 
    dim_products p ON o.product_code = p.product_code
WHERE 
    p.still_available = TRUE
GROUP BY 
    location;