-- ============================================================
-- 01_create_schema.sql
-- Project : Retail Sales Analytics Pipeline
-- Purpose : Create all dimension and fact tables
--           Star Schema design in retail_dw database
-- Author  : Manohar Marepally
-- ============================================================

USE retail_dw;

-- ============================================================
-- TABLE 1: DIM_DATE
-- Purpose : Time dimension — enables all date-based analysis
--           Always build this first in any data warehouse
--           No ETL needed — populated by a Python script
-- ============================================================
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key        INT             PRIMARY KEY,  -- YYYYMMDD format e.g. 20170901
    full_date       DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         TINYINT         NOT NULL,     -- 1 to 4
    quarter_name    VARCHAR(6)      NOT NULL,     -- Q1, Q2...
    month_num       TINYINT         NOT NULL,     -- 1 to 12
    month_name      VARCHAR(12)     NOT NULL,     -- January...
    month_short     VARCHAR(3)      NOT NULL,     -- Jan, Feb...
    week_of_year    TINYINT         NOT NULL,     -- 1 to 53
    day_of_month    TINYINT         NOT NULL,
    day_of_week     TINYINT         NOT NULL,     -- 1=Monday, 7=Sunday
    day_name        VARCHAR(10)     NOT NULL,     -- Monday...
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE,
    is_month_start  BOOLEAN         NOT NULL DEFAULT FALSE,
    is_month_end    BOOLEAN         NOT NULL DEFAULT FALSE,
    `year_month`      VARCHAR(7)      NOT NULL      -- 2017-09 format 
);


-- ============================================================
-- TABLE 2: DIM_CUSTOMER
-- Purpose : Customer dimension with SCD Type 2 support
--           Tracks historical changes to customer attributes
--           Mirrors Informatica SCD Type 2 mapping logic
-- ============================================================
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_key            INT             AUTO_INCREMENT PRIMARY KEY,
    customer_id             VARCHAR(50)     NOT NULL,       -- business/natural key
    customer_unique_id      VARCHAR(50),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(10),
    customer_zip_prefix     VARCHAR(10),
    -- SCD Type 2 tracking columns
    effective_start_date    DATE            NOT NULL,
    effective_end_date      DATE            NULL,           -- NULL = current record
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    record_version          TINYINT         NOT NULL DEFAULT 1,
    etl_load_date           DATE            NOT NULL
);

-- Index for fast lookup by business key
CREATE INDEX idx_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_customer_current ON dim_customer(customer_id, is_current);


-- ============================================================
-- TABLE 3: DIM_PRODUCT
-- Purpose : Product dimension with category and price band
-- ============================================================
DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
    product_key             INT             AUTO_INCREMENT PRIMARY KEY,
    product_id              VARCHAR(50)     NOT NULL,       -- business key
    product_category        VARCHAR(100),
    price_band              VARCHAR(20),    -- Budget / Mid-Range / Premium / Luxury
    weight_category         VARCHAR(20),    -- Light / Medium / Heavy / Bulky
    product_weight_g        DECIMAL(10,2),
    product_length_cm       DECIMAL(8,2),
    product_height_cm       DECIMAL(8,2),
    product_width_cm        DECIMAL(8,2),
    etl_load_date           DATE            NOT NULL
);

CREATE INDEX idx_product_id ON dim_product(product_id);


-- ============================================================
-- TABLE 4: DIM_SELLER
-- Purpose : Seller/vendor dimension
-- ============================================================
DROP TABLE IF EXISTS dim_seller;

CREATE TABLE dim_seller (
    seller_key              INT             AUTO_INCREMENT PRIMARY KEY,
    seller_id               VARCHAR(50)     NOT NULL,       -- business key
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(10),
    seller_zip_prefix       VARCHAR(10),
    etl_load_date           DATE            NOT NULL
);

CREATE INDEX idx_seller_id ON dim_seller(seller_id);


-- ============================================================
-- TABLE 5: FACT_SALES
-- Purpose : Central fact table — one row per order line item
--           All measures (revenue, quantity, delivery metrics)
--           All foreign keys point to dimension surrogate keys
-- ============================================================
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
    fact_id                 BIGINT          AUTO_INCREMENT PRIMARY KEY,
    -- Foreign keys to dimensions (surrogate keys)
    date_key                INT             NOT NULL,
    customer_key            INT             NOT NULL,
    product_key             INT             NOT NULL,
    seller_key              INT             NOT NULL,
    -- Degenerate dimensions (no dimension table needed)
    order_id                VARCHAR(50)     NOT NULL,
    order_item_seq          TINYINT         NOT NULL,   -- line item number
    -- Measures
    unit_price              DECIMAL(10,2)   NOT NULL,
    freight_value           DECIMAL(10,2)   NOT NULL DEFAULT 0,
    gross_revenue           DECIMAL(12,2)   NOT NULL,
    total_cost              DECIMAL(12,2)   NOT NULL,
    -- Delivery metrics
    delivery_days           INT,
    delivery_delay_days     INT,
    is_on_time              TINYINT         NOT NULL DEFAULT 0,
    -- Payment info (from payments aggregation)
    primary_payment_type    VARCHAR(30),
    payment_installments    TINYINT,
    total_payment_value     DECIMAL(12,2),
    -- Order status
    order_status            VARCHAR(30),
    status_category         VARCHAR(30),
    -- ETL metadata
    etl_load_date           DATE            NOT NULL,
    -- Foreign key constraints
    FOREIGN KEY (date_key)     REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
    FOREIGN KEY (seller_key)   REFERENCES dim_seller(seller_key)
);

-- Indexes for common query patterns
CREATE INDEX idx_fact_date     ON fact_sales(date_key);
CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_product  ON fact_sales(product_key);
CREATE INDEX idx_fact_order    ON fact_sales(order_id);


-- ============================================================
-- TABLE 6: ETL_CONTROL_LOG
-- Purpose : Track every ETL run — watermark for incremental loads
--           Mirrors Autosys job logging you did previously
-- ============================================================
DROP TABLE IF EXISTS etl_control_log;

CREATE TABLE etl_control_log (
    log_id              INT         AUTO_INCREMENT PRIMARY KEY,
    target_table        VARCHAR(100) NOT NULL,
    run_start_ts        DATETIME    NOT NULL,
    run_end_ts          DATETIME,
    records_loaded      INT         DEFAULT 0,
    records_rejected    INT         DEFAULT 0,
    last_loaded_ts      DATETIME,
    status              VARCHAR(20) NOT NULL,   -- RUNNING / SUCCESS / FAILED
    error_message       TEXT,
    run_by              VARCHAR(50) DEFAULT 'etl_pipeline'
);