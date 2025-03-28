-- set the role
USE ROLE accountadmin;

-- create the warehouse
CREATE OR REPLACE WAREHOUSE beauty_warehouse
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60           
    AUTO_RESUME = TRUE            
    INITIALLY_SUSPENDED = TRUE;

-- create the role
CREATE OR REPLACE ROLE beauty_role;

-- create the database
CREATE OR REPLACE DATABASE beauty_purchase_db;

-- show grants on warehouse
SHOW GRANTS ON WAREHOUSE beauty_warehouse;

-- grant access to roles
GRANT USAGE ON WAREHOUSE beauty_warehouse TO ROLE beauty_role;
GRANT ROLE beauty_role TO USER glowcodes;
GRANT ALL ON DATABASE beauty_purchase_db TO ROLE beauty_role;

USE ROLE beauty_role;

-- create the schemas
CREATE OR REPLACE SCHEMA beauty_purchase_fact; -- fact table schema
CREATE OR REPLACE SCHEMA beauty_purchase_dim_product; -- product dimension schema
CREATE OR REPLACE SCHEMA beauty_purchase_dim_store; -- store dimension schema
CREATE OR REPLACE SCHEMA beauty_purchase_dim_date; -- date dimension schema

/*
fact and dimension tables creation
*/

-- date dimension table
CREATE OR REPLACE TABLE beauty_purchase_dim_date.date (
    date_id INT AUTOINCREMENT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    day_of_week VARCHAR(255),
    week_of_year INT
);

-- store dimension table
CREATE OR REPLACE TABLE beauty_purchase_dim_store.store (
    store_id INT AUTOINCREMENT PRIMARY KEY,
    store_name VARCHAR(16777216)
);

-- brand table
CREATE OR REPLACE TABLE beauty_purchase_dim_product.brand (
    brand_id INT AUTOINCREMENT PRIMARY KEY,
    brand_name VARCHAR(16777216)
);

-- product type table
CREATE OR REPLACE TABLE beauty_purchase_dim_product.product_type (
    type_id INT AUTOINCREMENT PRIMARY KEY,
    type_name VARCHAR(16777216)
);

-- product purpose table
CREATE OR REPLACE TABLE beauty_purchase_dim_product.product_purpose (
    purpose_id INT AUTOINCREMENT PRIMARY KEY,
    purpose_name VARCHAR(16777216)
);

-- product category table
CREATE OR REPLACE TABLE beauty_purchase_dim_product.product_category (
    category_id INT AUTOINCREMENT PRIMARY KEY,
    category_name VARCHAR(16777216)
);

-- product dimension table
CREATE OR REPLACE TABLE beauty_purchase_dim_product.product (
    product_id INT AUTOINCREMENT PRIMARY KEY,
    product_name VARCHAR(16777216),
    category_id INT,
    type_id INT,
    purpose_id INT,
    brand_id INT,
    FOREIGN KEY (category_id) REFERENCES beauty_purchase_dim_product.product_category(category_id),
    FOREIGN KEY (purpose_id) REFERENCES beauty_purchase_dim_product.product_purpose(purpose_id),
    FOREIGN KEY (brand_id) REFERENCES beauty_purchase_dim_product.brand(brand_id),
    FOREIGN KEY (type_id) REFERENCES beauty_purchase_dim_product.product_type(type_id)
);

-- fact table
CREATE OR REPLACE TABLE beauty_purchase_fact.fact_purchase (
    purchase_id INT AUTOINCREMENT PRIMARY KEY,
    product_id INT,
    store_id INT,
    date_id INT,
    quantity INT,
    unit_price NUMBER(38,2),
    total_price NUMBER(38,2),
    price_category VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES beauty_purchase_dim_product.product(product_id),
    FOREIGN KEY (store_id) REFERENCES beauty_purchase_dim_store.store(store_id),
    FOREIGN KEY (date_id) REFERENCES beauty_purchase_dim_date.date(date_id)
);