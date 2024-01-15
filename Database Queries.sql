-- 1. Returns how many stores the business has and in which countries
SELECT
    country_code,
    COUNT(country_code)
FROM 
    dim_store_details
GROUP BY
    country_code

-- 2. Returns the number of stores in each location
SELECT
    locality,
    COUNT(locality) AS store_count
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    store_count DESC

-- 3. Returns the number of sales for each month
SELECT
    dim_date_times.month,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
FROM
    dim_date_times
INNER JOIN
    orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY
    dim_date_times.month
ORDER BY
    total_sales DESC

-- 4. Adds Web and Offline location column to orders_table
ALTER TABLE orders_table
ADD COLUMN location TEXT;

UPDATE orders_table
SET location =
CASE
    WHEN store_code = 'WEB-1388012W' THEN 'Web'
    ELSE 'Offline'
END;

-- 4. Returns the number of sales coming from Web and Offline purchases
SELECT
    location,
    SUM(product_quantity) AS product_quantity,
    COUNT(index) AS number_of_sales
FROM
    orders_table
GROUP BY
    location

-- 5. Returns percentage of sales coming from each store type
WITH cte1 AS(
    SELECT
        dim_store_details.store_type,
        SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
    FROM
        dim_store_details
    INNER JOIN
        orders_table ON orders_table.store_code = dim_store_details.store_code
    INNER JOIN
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY
        dim_store_details.store_type

)
 
SELECT
    dim_store_details.store_type,
    total_sales,
    (total_sales * 1.0/(SELECT SUM(total_Sales) FROM cte1) * 100) AS percentage_total
FROM
    cte1
INNER JOIN
    dim_store_details ON dim_store_details.store_type = cte1.store_type
GROUP BY
    dim_store_details.store_type,
    cte1.total_sales

-- 6. Returns which months in which years have the highest sales
SELECT
    dim_date_times.month,
    dim_date_times.year,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
FROM
    dim_date_times
INNER JOIN
    orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY
    dim_date_times.month,
    dim_date_times.year
ORDER BY
    total_sales DESC

-- 7. Returns the total staff numbers for each international location of the business
SELECT
    SUM(staff_numbers),
    country_code
FROM
    dim_store_details
GROUP BY
    country_code

-- 8. Returns the store type which is generating the most sales in Germany
SELECT
    dim_store_details.store_type,
    dim_store_details.country_code,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
FROM
    dim_store_details
INNER JOIN
    orders_table ON orders_table.store_code = dim_store_details.store_code
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
WHERE
    dim_store_details.country_code = 'DE'
GROUP BY
    dim_store_details.store_type,
    dim_store_details.country_code
ORDER BY
    total_sales DESC


-- 9. Returns the average time taken between each sales by year
WITH cte1 AS (
    SELECT
        day,
        month,
        year,
        timestamp,
        CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS event_time
    FROM dim_date_times

    
),

cte2 AS (
    SELECT
        day,
        month,
        year,
        event_time,
        LEAD(event_time) OVER (ORDER BY event_time) AS time_2
    FROM cte1
),

cte3 AS(
    SELECT
        year,
        time_2 - event_time AS actual_time_taken
    FROM
        cte2
    GROUP BY
        year,
        time_2,
        event_time
)

SELECT
    year,
    AVG(actual_time_taken) AS average_actual_time_taken
FROM
    cte3
GROUP BY
    year
ORDER BY
    average_actual_time_taken DESC






