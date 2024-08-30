SELECT 
	country_code, COUNT(*) AS total_no_stores
FROM 
	dim_store_details 
WHERE
	locality IS NOT NULL
GROUP BY 
	country_code
ORDER BY 
	total_no_stores DESC;
