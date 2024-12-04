-- find how many stores does busisness have and which country
SELECT country_code AS country, count(country_code) AS total_no_stores FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores ASC;

-- Which location currently have many stores?
SELECT locality, count(locality) as total_no_stores FROM public.dim_store_details
GROUP BY locality 
ORDER BY total_no_stores DESC;

-- Which month produce large amount of sales?
SELECT sum(d.product_price *ot.product_quantity) AS total_sales , month
FROM public.dim_date_times t
LEFT JOIN orders_table ot
ON t.date_uuid = ot.date_uuid
LEFT JOIN dim_products d
ON ot.product_code = d.product_code
GROUP BY month
ORDER BY total_sales DESC;

ALTER TABLE orders_table ADD location varchar(7);

UPDATE orders_table
SET location = CASE
	when store_code like 'WEB%' THEN 'WEB'
	ELSE 'offline'
	END;

-- find out howmany sales are coming from online
SELECT count(product_quantity) AS number_of_sales, 
	SUM(product_quantity) AS product_quantity_count,
	location
FROM orders_table
GROUP BY location
ORDER BY number_of_sales ASC;

-- find percentage of sales come through each type of store
WITH revenue_per_store_type AS (
    SELECT 
        ds.store_type, 
        COUNT(ot.user_uuid) AS total_sales_count, 
        SUM(d.product_price * ot.product_quantity) AS total_sales
    FROM orders_table ot
    LEFT JOIN dim_products d 
        ON ot.product_code = d.product_code
    LEFT JOIN dim_store_details ds
        ON ot.store_code = ds.store_code
    GROUP BY ds.store_type
),
total_summary AS (
    SELECT 
        SUM(total_sales_count) AS total_sales_count_overall
    FROM revenue_per_store_type rps
)
SELECT 
    rps.store_type,
    rps.total_sales,
    round((rps.total_sales_count / ts.total_sales_count_overall * 100),2) AS "sales_made(%)"
FROM revenue_per_store_type rps
CROSS JOIN total_summary ts
ORDER BY "sales_made(%)" DESC;

-- which month in each year produced the highest cost of sales
SELECT sum(d.product_price *ot.product_quantity) AS total_sales , year, month
FROM public.dim_date_times t
LEFT JOIN orders_table ot
ON t.date_uuid = ot.date_uuid
LEFT JOIN dim_products d
ON ot.product_code = d.product_code
GROUP BY month, year
ORDER BY total_sales DESC;

--find staff headcount
SELECT sum(staff_numbers) as total_staff_numbers,
	country_code
FROM public.dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--which german store type is the selling most 
WITH revenue_per_store_type AS (
    SELECT 
        ds.store_type, 
		ds.country_code,
        SUM(d.product_price * ot.product_quantity) AS total_sales
    FROM orders_table ot
    LEFT JOIN dim_products d 
        ON ot.product_code = d.product_code
    LEFT JOIN dim_store_details ds
        ON ot.store_code = ds.store_code
	WHERE ds.country_code= 'DE'
    GROUP BY ds.store_type, ds.country_code
	ORDER BY total_sales ASC
	
)
SELECT 
    rps.total_sales,
	rps.store_type,
    rps.country_code
FROM revenue_per_store_type rps;


ALTER TABLE dim_date_times
ADD COLUMN full_timestamp TIMESTAMP; 

-- Create full timestamp
UPDATE dim_date_times
SET full_timestamp = TO_TIMESTAMP(
(
 
CONCAT(year, '-',month,'-',day,'-',timestamp)
),
 'YYYY-MM-DD HH24:MI:SS'
);

-- find how quickly is the company making sales
WITH timeinterval AS(
SELECT
    year,
    full_timestamp,
    EXTRACT(EPOCH FROM (LEAD(full_timestamp) OVER (PARTITION BY year ORDER BY full_timestamp) - full_timestamp)) AS timestamp_diff
FROM 
    public.dim_date_times
WHERE 
    full_timestamp IS NOT NULL
	)
SELECT year,
	CONCAT(
        '"hours": ', FLOOR(AVG(timestamp_diff) / 3600), ', ',
        '"minutes": ', FLOOR((AVG(timestamp_diff) % 3600) / 60), ', ',
        '"seconds": ', FLOOR(AVG(timestamp_diff) % 60), ', ',
        '"milliseconds": ', ((AVG(timestamp_diff) - FLOOR(AVG(timestamp_diff))) * 1000)
    ) AS actual_time_taken
	FROM timeinterval
	GROUP BY year
	ORDER BY actual_time_taken DESC;
	
	





