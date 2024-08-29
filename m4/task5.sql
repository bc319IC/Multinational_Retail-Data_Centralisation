WITH sales_summary AS (
    SELECT
        s.store_type AS store_type,
        ROUND(SUM(o.product_quantity * p.product_price)::numeric, 2) AS total_sales
    FROM 
        orders_table o
    JOIN 
        dim_products p ON o.product_code = p.product_code
    LEFT JOIN 
        dim_store_details s ON o.store_code = s.store_code
    WHERE 
        p.still_available = TRUE
    GROUP BY 
        store_type
),
total_sales AS (
    SELECT 
        SUM(total_sales) AS grand_total
    FROM 
        sales_summary
)
SELECT
    store_type,
    total_sales,
    ROUND((total_sales / grand_total) * 100, 2) AS percentage_total
FROM
    sales_summary,
    total_sales
ORDER BY
    total_sales DESC;