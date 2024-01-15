
-- Returns the data types and column names from a selected table.
SELECT 
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME, 
COLUMN_NAME, 
DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_name = 'dim_store_details'

-- Changes orders_table data types to correct data types
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(20);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(15);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(15);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING ("user_uuid"::TEXT::UUID);

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING ("date_uuid"::TEXT::UUID);

-- Changes dim_users types to correct data types
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR;

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR;

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING ("date_of_birth"::TEXT::DATE);

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING ("join_date"::TEXT::DATE);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING ("user_uuid"::TEXT::UUID);

-- Drops incorrect staff numbers from dim_store_details table
UPDATE dim_store_details
SET staff_numbers =
CASE
    WHEN staff_numbers = 'J78' THEN NULL
    WHEN staff_numbers = 'A97' THEN NULL
    WHEN staff_numbers = '3n9' THEN NULL
    WHEN staff_numbers = '80R' THEN NULL
    WHEN staff_numbers = '30e' THEN NULL
    ELSE staff_numbers
END;

-- Drops empty lat column from dim_store_details table
ALTER TABLE dim_store_details
DROP COLUMN lat;

-- Changes dim_store_details table data types to correct data types
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING ("longitude"::TEXT::FLOAT);

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(15);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING ("staff_numbers"::TEXT::SMALLINT);

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING ("opening_date"::TEXT::DATE);

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(15);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING ("longitude"::TEXT::FLOAT);

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(5);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);


-- Removes £ from price column in dim_products table
UPDATE dim_products 
SET product_price = REPLACE(product_price, '£', '')

-- Adds weight_class and weight_range_kg columns to dim_products
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15)

UPDATE dim_products
SET weight_class =
CASE 
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

ALTER TABLE dim_products
ADD COLUMN weight_range_kg VARCHAR(20)

UPDATE dim_products
SET weight_range_kg = 
CASE 
    WHEN weight < 2 THEN '< 2'
    WHEN weight >= 2 AND weight < 40 THEN '>= 2 - < 40'
    WHEN weight >= 40 AND weight < 140 THEN '>= 40 - < 140'
    WHEN weight >= 140 THEN '=> 140'
END;

-- Configure still_available column for data type conversion
ALTER TABLE dim_products
RENAME removed TO still_available;

UPDATE dim_products
SET still_available =
CASE
    WHEN still_available = 'Still_avaliable' THEN 'Still_available'
    WHEN still_available = 'Removed' THEN 'Removed'
END;

UPDATE dim_products
SET still_available =
CASE
    WHEN still_available = 'Still_available' THEN TRUE
    WHEN still_available = 'Removed' THEN FALSE
END;

-- Changes dim_products table data types to correct data types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING ("product_price"::TEXT::FLOAT);

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING ("weight"::TEXT::FLOAT);

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(20);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(15);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING ("date_added"::TEXT::DATE);

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING ("uuid"::TEXT::UUID);

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING ("still_available"::TEXT::BOOLEAN);

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(15);

-- Changes dim_date_times data types to correct data types
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(5);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(5);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(5);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING ("date_uuid"::TEXT::UUID);

-- Changes dim_card_details table data types to correct data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(20);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(20);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING ("date_payment_confirmed"::TEXT::DATE);










