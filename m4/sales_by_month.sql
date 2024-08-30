SELECT
	ROUND(SUM(o.product_quantity * p.product_price)::numeric, 2) AS total_sales ,
	d.month AS month
FROM 
	orders_table o  
JOIN 
	dim_date_times d ON o.date_uuid = d.date_uuid 
JOIN 
	dim_products p ON o.product_code = p.product_code  
WHERE 
	p.still_available = TRUE 
GROUP BY 
	d.month 
ORDER BY 
	total_sales DESC;