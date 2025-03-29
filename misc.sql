USE ROLE beauty_role;
USE DATABASE beauty_purchase_db;

/*
stored procedure to insert data into fact and dimension table from raw data
*/

CREATE OR REPLACE PROCEDURE beauty_purchase_procs.load_data_into_fact_and_dim_tables()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    -- insert unique stores into store dimension table
    MERGE INTO beauty_purchase_dim_store.store AS target
    USING (
        SELECT DISTINCT store
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.store_name = source.store
    WHEN NOT MATCHED THEN
        INSERT (store_name)
        VALUES (source.store);

        
    -- insert unique dates into the date dimension table
    MERGE INTO beauty_purchase_dim_date.date AS target
    USING (
        SELECT DISTINCT 
            date_bought AS date, 
            EXTRACT(DAY FROM date_bought) AS day,
            EXTRACT(MONTH FROM date_bought) AS month,
            EXTRACT(QUARTER FROM date_bought) AS quarter,
            EXTRACT(YEAR FROM date_bought) AS year,
            DAYNAME(date_bought) AS day_of_week,
            EXTRACT(WEEK FROM date_bought) AS week_of_year
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.date = source.date
    WHEN NOT MATCHED THEN
        INSERT (date, day, month, quarter, year, day_of_week, week_of_year)
        VALUES (source.date, source.day, source.month, source.quarter, source.year, source.day_of_week, source.week_of_year);

        
    -- insert unique brands into the brand dimension table
    MERGE INTO beauty_purchase_dim_product.brand AS target
    USING (
        SELECT DISTINCT brand
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.brand_name = source.brand
    WHEN NOT MATCHED THEN
        INSERT (brand_name)
        VALUES (source.brand);

        
    -- insert unique product purposes into the purpose dimension table
    MERGE INTO beauty_purchase_dim_product.product_purpose AS target
    USING (
        SELECT DISTINCT product_purpose
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.purpose_name = source.product_purpose
    WHEN NOT MATCHED THEN
        INSERT (purpose_name)
        VALUES (source.product_purpose);

        
    -- insert unique product types into the product type dimension table
    MERGE INTO beauty_purchase_dim_product.product_type AS target
    USING (
        SELECT DISTINCT product_type
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.type_name = source.product_type
    WHEN NOT MATCHED THEN
        INSERT (type_name)
        VALUES (source.product_type);

        
    -- insert unique product categories into the product category dimension table
    MERGE INTO beauty_purchase_dim_product.product_category AS target
    USING (
        SELECT DISTINCT product_category
        FROM beauty_purchase_cleaned.raw_data
    ) AS source
    ON target.category_name = source.product_category
    WHEN NOT MATCHED THEN
        INSERT (category_name)
        VALUES (source.product_category);


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


    -- insert data into the fact_purchase table
    MERGE INTO beauty_purchase_fact.fact_purchase AS target
    USING (
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
        JOIN beauty_purchase_dim_date.date d ON r.date_bought = d.date
    ) AS source
    ON target.product_id = source.product_id
       AND target.store_id = source.store_id
       AND target.date_id = source.date_id
    WHEN NOT MATCHED THEN
        INSERT (product_id, store_id, date_id, quantity, unit_price, total_price, price_category)
        VALUES (source.product_id, source.store_id, source.date_id, source.quantity, source.unit_price, source.total_price, source.price_category);

    RETURN 'Data inserted into fact and dimension tables successfully';
END;
$$;


-- -- insert unique stores into store dimension table
-- INSERT INTO beauty_purchase_dim_store.store (store_name)
-- SELECT DISTINCT store
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique dates into the date dimension table
-- INSERT INTO beauty_purchase_dim_date.date (date, day, month, quarter, year, day_of_week, week_of_year)
-- SELECT DISTINCT
--     date_bought,
--     EXTRACT(DAY FROM date_bought) AS day,
--     EXTRACT(MONTH FROM date_bought) AS month,
--     EXTRACT(QUARTER FROM date_bought) AS quarter,
--     EXTRACT(YEAR FROM date_bought) AS year,
--     DAYNAME(date_bought) AS day_of_week,
--     EXTRACT(WEEK FROM date_bought) AS week_of_year
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique brands into the brand dimension table
-- INSERT INTO beauty_purchase_dim_product.brand (brand_name)
-- SELECT DISTINCT brand
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique product purposes into the purpose dimension table
-- INSERT INTO beauty_purchase_dim_product.product_purpose (purpose_name)
-- SELECT DISTINCT product_purpose
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique product types into the type dimension table
-- INSERT INTO beauty_purchase_dim_product.product_type (type_name)
-- SELECT DISTINCT product_type
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique product categories into the category dimension table
-- INSERT INTO beauty_purchase_dim_product.product_category (category_name)
-- SELECT DISTINCT product_category
-- FROM beauty_purchase_cleaned.raw_data;

-- -- insert unique products into the product dimension table
-- MERGE INTO beauty_purchase_dim_product.product AS target
-- USING (
--     SELECT DISTINCT
--         raw.product_name,
--         category.category_id,
--         type.type_id,
--         purpose.purpose_id,
--         brand.brand_id
--     FROM beauty_purchase_cleaned.raw_data AS raw
--     JOIN beauty_purchase_dim_product.product_category AS category
--         ON raw.product_category = category.category_name
--     JOIN beauty_purchase_dim_product.product_type AS type
--         ON raw.product_type = type.type_name
--     JOIN beauty_purchase_dim_product.product_purpose AS purpose
--         ON raw.product_purpose = purpose.purpose_name
--     JOIN beauty_purchase_dim_product.brand AS brand
--         ON raw.brand = brand.brand_name
-- ) AS source
-- ON target.product_name = source.product_name
-- WHEN NOT MATCHED THEN
--     INSERT (product_name, category_id, type_id, purpose_id, brand_id)
--     VALUES (source.product_name, source.category_id, source.type_id, source.purpose_id, source.brand_id);

-- -- insert data into the fact_purchase table
-- INSERT INTO beauty_purchase_fact.fact_purchase (product_id, store_id, date_id, quantity, unit_price, total_price, price_category)
-- SELECT 
--     p.product_id,
--     s.store_id,
--     d.date_id,
--     r.quantity,
--     r.unit_price,
--     r.total_price,
--     r.price_category
-- FROM beauty_purchase_cleaned.raw_data r
-- JOIN beauty_purchase_dim_product.product p ON r.product_name = p.product_name
-- JOIN beauty_purchase_dim_store.store s ON r.store = s.store_name
-- JOIN beauty_purchase_dim_date.date d ON r.date_bought = d.date;