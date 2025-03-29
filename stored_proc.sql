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
    INSERT INTO beauty_purchase_dim_store.store (store_name)
    SELECT DISTINCT store
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_store.store s
        WHERE s.store_name = beauty_purchase_cleaned.raw_data.store
    );

        
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
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_date.date d
        WHERE d.date = beauty_purchase_cleaned.raw_data.date_bought
    );

        
    -- insert unique brands into the brand dimension table
    INSERT INTO beauty_purchase_dim_product.brand (brand_name)
    SELECT DISTINCT brand
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_product.brand b
        WHERE b.brand_name = beauty_purchase_cleaned.raw_data.brand
    );

        
    -- insert unique product purposes into the purpose dimension table
    INSERT INTO beauty_purchase_dim_product.product_purpose (purpose_name)
    SELECT DISTINCT product_purpose
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_product.product_purpose pp
        WHERE pp.purpose_name = beauty_purchase_cleaned.raw_data.product_purpose
    );

        
    -- insert unique product types into the product type dimension table
    INSERT INTO beauty_purchase_dim_product.product_type (type_name)
    SELECT DISTINCT product_type
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_product.product_type pt
        WHERE pt.type_name = beauty_purchase_cleaned.raw_data.product_type
    );

        
    -- insert unique product categories into the product category dimension table
    INSERT INTO beauty_purchase_dim_product.product_category (category_name)
    SELECT DISTINCT product_category
    FROM beauty_purchase_cleaned.raw_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_product.product_category pc
        WHERE pc.category_name = beauty_purchase_cleaned.raw_data.product_category
    );


    -- insert unique products into the product dimension table
    INSERT INTO beauty_purchase_dim_product.product (product_name, category_id, type_id, purpose_id, brand_id)
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
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_dim_product.product p
        WHERE p.product_name = raw.product_name
    );


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
    JOIN beauty_purchase_dim_date.date d ON r.date_bought = d.date
    WHERE NOT EXISTS (
        SELECT 1
        FROM beauty_purchase_fact.fact_purchase fp
        WHERE fp.product_id = p.product_id
        AND fp.store_id = s.store_id
        AND fp.date_id = d.date_id
    );

    RETURN 'Data inserted into fact and dimension tables successfully';
END;
$$;