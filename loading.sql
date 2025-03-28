USE ROLE beauty_role;
USE DATABASE beauty_purchase_db;

/*
insert data into fact and dimension table from raw data
*/

-- insert unique stores into store dimension table
INSERT INTO beauty_purchase_dim_store.store (store_name)
SELECT DISTINCT store
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_store.store;

-- insert unique dates into the date dimension table
INSERT INTO beauty_purchase_dim_date.date (date, day, month, quarter, year, day_of_week, week_of_year)
SELECT DISTINCT
    date_bought,
    EXTRACT(DAY FROM date_bought) AS day,
    EXTRACT(MONTH FROM date_bought) AS month,
    EXTRACT(QUARTER FROM date_bought) AS quarter,
    EXTRACT(YEAR FROM date_bought) AS year,
    DAYNAME(date_bought) AS day_of_week,
    EXTRACT(WEEK FROM date_bought) AS week_of_year
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_date.date;

-- insert unique brands into the brand dimension table
INSERT INTO beauty_purchase_dim_product.brand (brand_name)
SELECT DISTINCT brand
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_product.brand;

-- insert unique product purposes into the purpose dimension table
INSERT INTO beauty_purchase_dim_product.product_purpose (purpose_name)
SELECT DISTINCT product_purpose
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_product.product_purpose;

-- insert unique product types into the type dimension table
INSERT INTO beauty_purchase_dim_product.product_type (type_name)
SELECT DISTINCT product_type
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_product.product_type;

-- insert unique product categories into the category dimension table
INSERT INTO beauty_purchase_dim_product.product_category (category_name)
SELECT DISTINCT product_category
FROM beauty_purchase_cleaned.raw_data;

SELECT * FROM beauty_purchase_dim_product.product_category;

-- insert unique products into the product dimension table
MERGE INTO beauty_purchase_dim_product.product AS target
USING (
    SELECT DISTINCT
        raw.product_name,
        category.category_id,
        type.type_id,
        purpose.purpose_id,
        brand.brand_id
    FROM beauty_purchase_cleaned.raw_data AS raw
    JOIN beauty_purchase_dim_product.product_category AS category
        ON raw.product_category = category.category_name
    JOIN beauty_purchase_dim_product.product_type AS type
        ON raw.product_type = type.type_name
    JOIN beauty_purchase_dim_product.product_purpose AS purpose
        ON raw.product_purpose = purpose.purpose_name
    JOIN beauty_purchase_dim_product.brand AS brand
        ON raw.brand = brand.brand_name
) AS source
ON target.product_name = source.product_name
WHEN NOT MATCHED THEN
    INSERT (product_name, category_id, type_id, purpose_id, brand_id)
    VALUES (source.product_name, source.category_id, source.type_id, source.purpose_id, source.brand_id);

SELECT distinct * FROM beauty_purchase_dim_product.product;

-- insert data into the fact_purchase table
INSERT INTO beauty_purchase_fact.fact_purchase (product_id, store_id, date_id, quantity, unit_price, total_price, price_category)
SELECT 
    p.product_id,
    s.store_id,
    d.date_id,
    r.quantity,
    r.unit_price,
    r.total_price,
    r.price_category
FROM beauty_purchase_cleaned.raw_data r
JOIN beauty_purchase_dim_product.product p ON r.product_name = p.product_name
JOIN beauty_purchase_dim_store.store s ON r.store = s.store_name
JOIN beauty_purchase_dim_date.date d ON r.date_bought = d.date;

SELECT * FROM beauty_purchase_fact.fact_purchase;