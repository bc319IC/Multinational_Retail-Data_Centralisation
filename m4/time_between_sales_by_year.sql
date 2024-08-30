WITH sales_times AS (
    SELECT
        d.year,
		-- change form to yyyy-mm-dd hh:mm:ss
        (d.date || ' ' || d.timestamp)::timestamp AS full_timestamp, 
		-- get next timestamp
        LEAD((d.date || ' ' || d.timestamp)::timestamp) 
		-- partition by year and order by year for LEAD function
        OVER (PARTITION BY d.year ORDER BY (d.date || ' ' || d.timestamp)::timestamp) AS next_timestamp
    FROM
        orders_table o
    JOIN
        dim_date_times d ON o.date_uuid = d.date_uuid
)
SELECT
    year,
    TO_CHAR(
	-- extract time difference and avg result and convert to interval from seconds
        INTERVAL '1 second' * AVG(EXTRACT(EPOCH FROM (next_timestamp - full_timestamp))),
        '"hours": HH24, "minutes": MI, "seconds": SS'
    ) AS actual_time_taken
FROM
    sales_times
WHERE
    next_timestamp IS NOT NULL
GROUP BY
    year
ORDER BY
    actual_time_taken DESC;