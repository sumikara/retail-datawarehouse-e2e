-- 18_MAPPING LAYER  - DDLs
DROP TABLE IF EXISTS stg.mapping_customers;
DROP TABLE IF EXISTS stg.mapping_stores;
DROP TABLE IF EXISTS stg.mapping_products;
DROP TABLE IF EXISTS stg.mapping_promotions;
DROP TABLE IF EXISTS stg.mapping_deliveries;
DROP TABLE IF EXISTS stg.mapping_engagements;
DROP TABLE IF EXISTS stg.mapping_employees;
DROP TABLE IF EXISTS stg.mapping_transactions;

CREATE TABLE IF NOT EXISTS stg.mapping_customers (
    customer_id_nk     VARCHAR(20),                       -- raw source customer key
    gender             VARCHAR(20),                       -- standardized text
    marital_status     VARCHAR(20),                       -- standardized text
    birth_of_dt        DATE,                              -- parsed from raw date_of_birth
    membership_dt      DATE,                              -- parsed from raw membership_date
    customer_zip_code  VARCHAR(30),                       -- retained for downstream joins
    customer_city      VARCHAR(100),                      -- retained for city resolution in nf
    customer_state     VARCHAR(100),                      -- retained for state-aware city resolution in nf
    last_purchase_dt   TIMESTAMP,                         -- parsed from raw last_purchase_date
    customer_src_id    VARCHAR(255),                      -- derived business key
    source_system      VARCHAR(100),                      -- sl_online_retail / sl_offline_retail
    source_table       VARCHAR(100),                      -- src_online_retail / src_offline_retail
    insert_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_stores (
    store_src_id        VARCHAR(255),                     -- derived store natural key
    store_name          VARCHAR(100),
    store_zip_code      VARCHAR(30),
    store_city          VARCHAR(100),
    store_state         VARCHAR(100),
    store_location_nk   VARCHAR(100),
    source_system       VARCHAR(100),                     -- sl_offline_retail
    source_table        VARCHAR(100),                     -- src_offline_retail
    insert_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_products (
    product_id_nk            VARCHAR(100),                -- raw source product key
    product_src_id           VARCHAR(255),                -- derived product natural key (product name+category+brand+material)
    product_category         VARCHAR(100),                -- standardized category
    product_name             VARCHAR(100),                -- standardized product name
    product_brand            VARCHAR(100),                -- standardized brand
    product_stock            INTEGER,                     -- parsed numeric value
    product_material         VARCHAR(100),                -- standardized material
    product_manufacture_dt   TIMESTAMP,                   -- parsed manufacture datetime
    product_expiry_dt        TIMESTAMP,                   -- parsed expiry datetime
    source_system            VARCHAR(100),                -- sl_online_retail / sl_offline_retail
    source_table             VARCHAR(100),                -- src_online_retail / src_offline_retail
    insert_dt                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_promotions (
    promotion_id_nk     VARCHAR(100),                     -- raw source promotion key
    promotion_type      VARCHAR(100),                     -- standardized promotion type
    promotion_channel   VARCHAR(100),                     -- standardized promotion channel
    promotion_start_dt  TIMESTAMP,                        -- parsed promotion start timestamp
    promotion_end_dt    TIMESTAMP,                        -- parsed promotion end timestamp
    promotion_src_id    VARCHAR(255),                     -- derived promotion natural key (promotions_type+channel+start_dt+end_dt)
    source_system       VARCHAR(100),                     -- sl_online_retail / sl_offline_retail
    source_table        VARCHAR(100),                     -- src_online_retail / src_offline_retail
    insert_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_deliveries (
    delivery_id_nk     VARCHAR(100),
    delivery_type      VARCHAR(100),
    delivery_status    VARCHAR(100),
    shipping_partner   VARCHAR(100),
    delivery_src_id    VARCHAR(255),                      -- derived delivery src id (type+shippingpartner)
    source_system      VARCHAR(100),                      -- sl_online_retail / sl_offline_retail
    source_table       VARCHAR(100),                      -- src_online_retail / src_offline_retail
    insert_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_engagements (
    engagement_id_nk           VARCHAR(100),            -- no change, use as is, src id
    customer_support_calls     INTEGER,
    website_address            VARCHAR(250),
    order_channel              VARCHAR(100),
    customer_support_method    VARCHAR(100),
    issue_status               VARCHAR(100),
    app_usage                  VARCHAR(100),
    website_visits             INTEGER,
    social_media_engagement    VARCHAR(100),
    source_system              VARCHAR(100),              -- sl_online_retail
    source_table               VARCHAR(100),              -- src_online_retail
    insert_dt                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.mapping_employees (
    employee_src_id     VARCHAR(100) NOT NULL,
    employee_name_nk    VARCHAR(100),
    employee_position   VARCHAR(100),
    employee_salary     NUMERIC(10,2),
    employee_hire_date  DATE,
    observed_ts         TIMESTAMP,
    source_system       VARCHAR(100),
    source_table        VARCHAR(100),
    insert_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS stg.mapping_transactions (
    transaction_id          VARCHAR(100),
    transaction_dt          TIMESTAMP,
    total_sales             NUMERIC(10,2),
    payment_method          VARCHAR(100),
    quantity                INTEGER,
    unit_price              NUMERIC(10,2),
    discount_applied        NUMERIC(10,2),
    day_of_week             VARCHAR(100),
    week_of_year            INTEGER,
    month_of_year           INTEGER,

    /* raw business keys */
    customer_id_nk          VARCHAR(100),
    product_id_nk           VARCHAR(100),
    promotion_id_nk         VARCHAR(100),
    delivery_id_nk          VARCHAR(100),
    engagement_id_nk        VARCHAR(100),
    employee_name_nk        VARCHAR(100),
    employee_hire_date      DATE,
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(100),
    store_zip_code          VARCHAR(30),
    store_city              VARCHAR(100),
    store_state             VARCHAR(100),
    store_location_nk       VARCHAR(100),

    /* product key components */
    product_name            VARCHAR(100),
    product_category        VARCHAR(100),
    product_brand           VARCHAR(100),
    product_material        VARCHAR(100),

    /* promotion components */
    promotion_type          VARCHAR(100),
    promotion_channel       VARCHAR(100),
    promotion_start_dt      TIMESTAMP,
    promotion_end_dt        TIMESTAMP,

    /* delivery key components */
    delivery_type           VARCHAR(100),
    shipping_partner        VARCHAR(100),

    /* derived source keys for downstream nf joins */
    customer_src_id         VARCHAR(255),
    product_src_id          VARCHAR(255),
    promotion_src_id        VARCHAR(255),
    delivery_src_id         VARCHAR(255),
    store_src_id            VARCHAR(255),
    city_src_id             VARCHAR(255),
    employee_src_id         VARCHAR(255),
    row_sig                 TEXT,

    source_system           VARCHAR(100),
    source_table            VARCHAR(100),
    insert_dt               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_customer_src
ON stg.mapping_transactions (customer_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_customers_nk
ON stg.mapping_customers (customer_id_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_customers_src
ON stg.mapping_customers (customer_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_stores_nk
ON stg.mapping_stores (store_location_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_store_src
ON stg.mapping_transactions (store_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_store_src
ON stg.mapping_transactions (store_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_products_nk
ON stg.mapping_products (product_id_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_product_src
ON stg.mapping_transactions (product_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_product_src
ON stg.mapping_transactions (product_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_promotions_nk
ON stg.mapping_promotions (promotion_id_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_promotions_type
ON stg.mapping_promotions (promotion_type);

CREATE INDEX IF NOT EXISTS idx_mapping_promotions_channel
ON stg.mapping_promotions (promotion_channel);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_promotion_src
ON stg.mapping_transactions (promotion_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_promotion_src
ON stg.mapping_transactions (promotion_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_deliveries_nk
ON stg.mapping_deliveries (delivery_id_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_deliveries_partner
ON stg.mapping_deliveries (shipping_partner);

CREATE INDEX IF NOT EXISTS idx_mapping_deliveries_type
ON stg.mapping_deliveries (delivery_type);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_delivery_src
ON stg.mapping_transactions (delivery_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_delivery_src
ON stg.mapping_transactions (delivery_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_engagements_nk
ON stg.mapping_engagements (engagement_id_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_employees_nk
ON stg.mapping_employees (employee_name_nk, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_employees_src
ON stg.mapping_employees (employee_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_employees_version
ON stg.mapping_employees (
    employee_src_id,
    employee_position,
    employee_hire_date
);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_employee_src
ON stg.mapping_transactions (employee_src_id);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_employee_src
ON stg.mapping_transactions (employee_src_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_src
ON stg.mapping_transactions (transaction_id, source_system, source_table);

CREATE INDEX IF NOT EXISTS idx_mapping_transactions_dt
ON stg.mapping_transactions (transaction_dt);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mapping_transactions_rowsig
ON stg.mapping_transactions (row_sig);

