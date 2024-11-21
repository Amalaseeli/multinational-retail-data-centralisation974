/*casting */

-- cast the columns of the orders_table
SELECT * FROM orders_table 

SELECT 
    MAX(LENGTH(card_number::TEXT)) AS max_length_card_number,
    MAX(LENGTH(store_code)) AS max_length_of_storecode,
    MAX(LENGTH(product_code)) AS max_length_productcode
FROM orders_table

ALTER TABLE orders_table
    DROP COLUMN level_0,
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT 
    column_name, data_type, character_maximum_length 
FROM information_schema.columns 
WHERE table_name = 'orders_table';

-- cast the column of the dim_users
SELECT * FROM dim_users

SELECT MAX(LENGTH(country_code)) as max_length_of_countrycode
FROM dim_users

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN  user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE; 

-- view alter datatypes
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users'

-- Update dim_store_details table
UPDATE dim_store_details
SET latitude = CASE
                  WHEN lat IS NOT NULL THEN lat
                  ELSE latitude
               END;

-- Drop the old lat column
ALTER TABLE dim_store_details
DROP COLUMN lat;

SELECT 
    MAX(LENGTH(store_code)) AS max_length_store_code,
    MAX(LENGTH(country_code)) AS max_length_country_code
FROM dim_store_details

UPDATE dim_store_details
SET longitude = NULL,
    latitude = NULL
WHERE longitude !~'^\d+(\.\d+)?$' OR latitude ='N/A'

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE NUMERIC USING latitude:: NUMERIC,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

SELECT column_name, data_type 
FROM information_schema.columns
WHERE table_name = 'dim_store_details'

-- update dim_products table
SELECT product_price
FROM dim_products

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%'

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR

UPDATE dim_products
SET weight_class = CASE 
    WHEN weight < 2 THEN 'light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >=40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END 

ALTER TABLE dim_products
    RENAME removed TO still_available; 

SELECT 
    MAX(LENGTH("EAN")) AS max_length_EAN,
    MAX(LENGTH("product_code")) AS max_length_product_code,
    MAX(LENGTH("weight_class")) AS max_length_of_weight_class
FROM dim_products;

SELECT column_name 
FROM information_schema.columns
WHERE table_name = 'dim_products'

SELECT "EAN", LENGTH("EAN")
FROM dim_products

SELECT DISTINCT still_available
FROM dim_products

UPDATE dim_products
SET still_available = 
CASE 
    WHEN still_available = 'still_available' THEN TRUE
    ELSE False
END

ALTER TABLE dim_products
    ALTER COLUMN "product_price" TYPE NUMERIC USING product_price::NUMERIC,
    ALTER COLUMN "weight" TYPE NUMERIC USING weight::NUMERIC,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN "product_code" TYPE VARCHAR(11),
    ALTER COLUMN "date_added" TYPE DATE USING date_added::DATE,
    ALTER COLUMN "uuid" TYPE UUID USING uuid::UUID,
    ALTER COLUMN "still_available" TYPE BOOL USING still_available::BOOL,
    ALTER COLUMN "weight_class" TYPE VARCHAR(14);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products'

-- alter dim_date_times table
SELECT 
    MAX(LENGTH(CAST("month" AS TEXT))) AS max_length_month,
    MAX(LENGTH(CAST("day" AS TEXT))) AS max_length_day,
    MAX(LENGTH(CAST("year" AS TEXT))) AS max_length_year,
    MAX(LENGTH(CAST("time_period" AS TEXT))) AS max_length_time_period 
FROM dim_date_times;

ALTER TABLE dim_date_times
    ALTER COLUMN "month" TYPE VARCHAR(2),
    ALTER COLUMN "day" TYPE VARCHAR(2),
    ALTER COLUMN "year" TYPE VARCHAR(4),
    ALTER COLUMN "time_period" TYPE VARCHAR(10),
    ALTER COLUMN "date_uuid" TYPE UUID USING date_uuid::UUID;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name='dim_date_times';

-- Alter dim_card_details
SELECT 
    MAX(LENGTH("card_number")) AS max_length_card_number,
    MAX(LENGTH("expiry_date":: TEXT)) AS max_length_expiry_date
FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN  date_payment_confirmed TYPE DATE;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details' 

SELECT table_name , column_name
FROM information_schema.columns
WHERE table_name LIKE 'dim_%'

SELECT *
FROM information_schema.columns
WHERE table_name = 'orders_table';

-- Create the primary keys in the dimensions table
SELECT c1.column_name, c2.table_name
FROM information_schema.columns c1
JOIN information_schema.columns c2
ON c1.column_name = c2.column_name
WHERE c1.table_name = 'orders_table'
AND c2.table_name LIKE 'dim%'

SELECT card_number, expiry_date, card_provider, date_payment_confirmed
FROM dim_card_details
WHERE card_number IS NULL

select count(*)
from dim_card_details

ALTER TABLE dim_card_details
ADD CONSTRAINT dim_card_pk PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD CONSTRAINT dim_date_pk PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD CONSTRAINT dim_store_pk PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD CONSTRAINT dim_user_pk PRIMARY KEY (user_uuid);

-- Add foreign_key constraints for card_number
ALTER TABLE orders_table
ADD CONSTRAINT card_number_fk 
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

-- Add foreign_key constraints for date_uuid
ALTER TABLE orders_table
ADD CONSTRAINT date_uuid_fk 
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

-- Add foreign_key constraints for product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

-- Add foreign key constraint for store_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

-- Add foreign key constraint for user_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

SELECT DISTINCT(user_uuid)
FROM orders_table

SELECT DISTINCT (ord.user_uuid)
FROM orders_table ord 
WHERE NOT EXISTS (
    SELECT 1 FROM dim_users
    WHERE dim_users.user_uuid= ord.user_uuid
)

SELECT user_uuid
FROM orders_table
WHERE user_uuid NOT IN (
    SELECT user_uuid
    FROM dim_users
);


select count(*)
from dim_users

select count(*)
from orders_table