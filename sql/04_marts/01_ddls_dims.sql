
-- # REPORTING DM LAYER - seq + ddls + indexes

CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_customer_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_store_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_product_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_promotion_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_delivery_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_engagement_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_employee_id START 1;
-- DATE DIM

CREATE TABLE IF NOT EXISTS dim.dim_customers (
    customer_surr_id     BIGINT PRIMARY KEY,
    customer_src_id      BIGINT NOT NULL,                 -- 3NF surrogate key
    source_system        VARCHAR(100) NOT NULL,
    source_table         VARCHAR(100) NOT NULL,
    gender               VARCHAR(20)  NOT NULL,
    marital_status       VARCHAR(20)  NOT NULL,
    birth_of_dt          DATE,
    membership_dt        DATE,
    last_purchase_dt     TIMESTAMP,
    customer_zip_code    VARCHAR(30)  NOT NULL,
    customer_city        VARCHAR(100) NOT NULL,
    customer_state       VARCHAR(100) NOT NULL,
    insert_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_stores (
    store_surr_id        BIGINT PRIMARY KEY,
    store_src_id         BIGINT NOT NULL,                 -- 3NF surrogate key
    source_system        VARCHAR(100) NOT NULL,
    source_table         VARCHAR(100) NOT NULL,
    store_name           VARCHAR(100),
    store_zip_code       VARCHAR(30)  NOT NULL,
    store_city           VARCHAR(100) NOT NULL,
    store_state          VARCHAR(100) NOT NULL,
    store_location       VARCHAR(100) NOT NULL,
    insert_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_products (
    product_surr_id          BIGINT PRIMARY KEY,
    product_src_id           BIGINT NOT NULL,             -- 3NF surrogate key
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    product_category         VARCHAR(100) NOT NULL,
    product_name             VARCHAR(100) NOT NULL,
    product_brand            VARCHAR(100) NOT NULL,
    product_stock            INTEGER,
    product_material         VARCHAR(100) NOT NULL,
    product_manufacture_dt   TIMESTAMP,
    product_expiry_dt        TIMESTAMP,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_promotions (
    promotion_surr_id     BIGINT PRIMARY KEY,
    promotion_src_id      BIGINT NOT NULL,               -- 3NF surrogate key
    source_system         VARCHAR(100) NOT NULL,
    source_table          VARCHAR(100) NOT NULL,
    promotion_channel     VARCHAR(100) NOT NULL,
    promotion_type        VARCHAR(100) NOT NULL,
    promotion_start_dt    TIMESTAMP,
    promotion_end_dt      TIMESTAMP,
    insert_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_deliveries (
    delivery_surr_id        BIGINT PRIMARY KEY,
    delivery_src_id         BIGINT NOT NULL,             -- 3NF surrogate key
    source_system           VARCHAR(100) NOT NULL,
    source_table            VARCHAR(100) NOT NULL,
    delivery_type           VARCHAR(100) NOT NULL,
    delivery_status         VARCHAR(100) NOT NULL,
    shipping_partner        VARCHAR(100) NOT NULL,
    insert_dt               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_engagements (
    engagement_surr_id         BIGINT PRIMARY KEY,
    engagement_src_id          BIGINT NOT NULL,          -- 3NF surrogate key
    source_system              VARCHAR(100) NOT NULL,
    source_table               VARCHAR(100) NOT NULL,
    customer_support_calls     INTEGER,
    website_address            VARCHAR(250) NOT NULL,
    order_channel              VARCHAR(100) NOT NULL,
    customer_support_method    VARCHAR(100) NOT NULL,
    issue_status               VARCHAR(100) NOT NULL,
    app_usage                  VARCHAR(100) NOT NULL,
    website_visits             INTEGER,
    social_media_engagement    VARCHAR(100) NOT NULL,
    insert_dt                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_employees_scd (
    employee_surr_id   BIGINT PRIMARY KEY,
    employee_src_id    BIGINT NOT NULL,                  -- 3NF employee_id
    employee_name      VARCHAR(100),
    employee_position  VARCHAR(100),
    employee_salary    NUMERIC(10,2),
    employee_hire_date DATE,
    start_dt           TIMESTAMP NOT NULL,
    end_dt             TIMESTAMP NOT NULL,
    is_active          BOOLEAN NOT NULL,
    source_system      VARCHAR(100) NOT NULL,
    source_table       VARCHAR(100) NOT NULL,
    insert_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_dates (
    date_surr_id        BIGINT PRIMARY KEY,      -- yyyymmdd
    full_date           DATE NOT NULL UNIQUE,
    day_of_month        INTEGER NOT NULL,
    month_of_year       INTEGER NOT NULL,
    year_of_date        INTEGER NOT NULL,
    quarter_of_year     INTEGER NOT NULL,
    week_of_year        INTEGER NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_month_start      BOOLEAN NOT NULL,
    is_month_end        BOOLEAN NOT NULL,
    is_quarter_start    BOOLEAN NOT NULL,
    is_quarter_end      BOOLEAN NOT NULL,
    is_year_start       BOOLEAN NOT NULL,
    is_year_end         BOOLEAN NOT NULL,
    insert_dt           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.fct_transactions_dd_dd (
    transaction_src_id   VARCHAR(100) NOT NULL,
    total_sales          NUMERIC(10,2) NOT NULL DEFAULT 0,
    quantity             INTEGER NOT NULL DEFAULT 0,
    unit_price           NUMERIC(10,2) NOT NULL DEFAULT 0,
    discount_applied     NUMERIC(10,2) NOT NULL DEFAULT 0,
    payment_method       VARCHAR(100) NOT NULL DEFAULT 'n.a.',

    product_surr_id      BIGINT NOT NULL DEFAULT -1,
    promotion_surr_id    BIGINT NOT NULL DEFAULT -1,
    delivery_surr_id     BIGINT NOT NULL DEFAULT -1,
    engagement_surr_id   BIGINT NOT NULL DEFAULT -1,
    store_surr_id        BIGINT NOT NULL DEFAULT -1,
    customer_surr_id     BIGINT NOT NULL DEFAULT -1,
    employee_surr_id     BIGINT NOT NULL DEFAULT -1,
    transaction_date_sk  BIGINT NOT NULL,
    transaction_date     DATE NOT NULL,
    source_system        VARCHAR(100) NOT NULL,
    source_table         VARCHAR(100) NOT NULL,
    insert_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fct_transactions_dd_dd
        PRIMARY KEY (
            transaction_date,
            transaction_src_id,
            product_surr_id,
            promotion_surr_id,
            delivery_surr_id,
            engagement_surr_id,
            store_surr_id,
            customer_surr_id,
            employee_surr_id
        ),

    CONSTRAINT fk_fct_trx_prod
        FOREIGN KEY (product_surr_id)
        REFERENCES dim.dim_products(product_surr_id),

    CONSTRAINT fk_fct_trx_prom
        FOREIGN KEY (promotion_surr_id)
        REFERENCES dim.dim_promotions(promotion_surr_id),

    CONSTRAINT fk_fct_trx_deliv
        FOREIGN KEY (delivery_surr_id)
        REFERENCES dim.dim_deliveries(delivery_surr_id),

    CONSTRAINT fk_fct_trx_eng
        FOREIGN KEY (engagement_surr_id)
        REFERENCES dim.dim_engagements(engagement_surr_id),

    CONSTRAINT fk_fct_trx_store
        FOREIGN KEY (store_surr_id)
        REFERENCES dim.dim_stores(store_surr_id),

    CONSTRAINT fk_fct_trx_cust
        FOREIGN KEY (customer_surr_id)
        REFERENCES dim.dim_customers(customer_surr_id),

    CONSTRAINT fk_fct_trx_emp
        FOREIGN KEY (employee_surr_id)
        REFERENCES dim.dim_employees_scd(employee_surr_id),

    CONSTRAINT fk_fct_trx_date
        FOREIGN KEY (transaction_date_sk)
        REFERENCES dim.dim_dates(date_surr_id)
) PARTITION BY RANGE (transaction_date);


CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customers_src_id
    ON dim.dim_customers (customer_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_customers_city_state
    ON dim.dim_customers (customer_city, customer_state);

CREATE INDEX IF NOT EXISTS ix_dim_customers_membership_dt
    ON dim.dim_customers (membership_dt);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_stores_src_id
    ON dim.dim_stores (store_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_stores_city_state
    ON dim.dim_stores (store_city, store_state);

CREATE INDEX IF NOT EXISTS ix_dim_stores_location
    ON dim.dim_stores (store_location);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_products_src_id
    ON dim.dim_products (product_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_products_category
    ON dim.dim_products (product_category);

CREATE INDEX IF NOT EXISTS ix_dim_products_name_brand
    ON dim.dim_products (product_name, product_brand);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_promotions_src_id
    ON dim.dim_promotions (promotion_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_promotions_type
    ON dim.dim_promotions (promotion_type);

CREATE INDEX IF NOT EXISTS ix_dim_promotions_channel
    ON dim.dim_promotions (promotion_channel);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_deliveries_src_id
    ON dim.dim_deliveries (delivery_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_type
    ON dim.dim_deliveries (delivery_type);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_status
    ON dim.dim_deliveries (delivery_status);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_partner
    ON dim.dim_deliveries (shipping_partner);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_engagements_src_id
    ON dim.dim_engagements (engagement_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_engagements_order_channel
    ON dim.dim_engagements (order_channel);

CREATE INDEX IF NOT EXISTS ix_dim_engagements_issue_status
    ON dim.dim_engagements (issue_status);

CREATE INDEX IF NOT EXISTS ix_dim_engagements_support_method
    ON dim.dim_engagements (customer_support_method);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dm_emp_active
    ON dim.dim_employees_scd (employee_src_id)
    WHERE is_active = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dm_emp_version
    ON dim.dim_employees_scd (employee_src_id, start_dt);

CREATE INDEX IF NOT EXISTS ix_dm_emp_scd_lookup
    ON dim.dim_employees_scd (employee_src_id, start_dt, end_dt);

CREATE INDEX IF NOT EXISTS ix_dim_dates_full_date
    ON dim.dim_dates (full_date);

CREATE INDEX IF NOT EXISTS ix_dim_dates_year_month
    ON dim.dim_dates (year_of_date, month_of_year);

CREATE INDEX IF NOT EXISTS ix_dim_dates_year_week
    ON dim.dim_dates (year_of_date, week_of_year);

CREATE INDEX IF NOT EXISTS ix_dim_dates_month_name
    ON dim.dim_dates (month_name);

CREATE INDEX IF NOT EXISTS ix_dim_dates_day_name
    ON dim.dim_dates (day_name);

CREATE INDEX IF NOT EXISTS ix_fct_trx_date_sk
    ON dim.fct_transactions_dd_dd (transaction_date_sk);

CREATE INDEX IF NOT EXISTS ix_fct_trx_product
    ON dim.fct_transactions_dd_dd (product_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_promotion
    ON dim.fct_transactions_dd_dd (promotion_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_delivery
    ON dim.fct_transactions_dd_dd (delivery_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_engagement
    ON dim.fct_transactions_dd_dd (engagement_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_store
    ON dim.fct_transactions_dd_dd (store_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_customer
    ON dim.fct_transactions_dd_dd (customer_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_employee
    ON dim.fct_transactions_dd_dd (employee_surr_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_source
    ON dim.fct_transactions_dd_dd (source_system, source_table);

CREATE INDEX IF NOT EXISTS ix_fct_trx_src_id
    ON dim.fct_transactions_dd_dd (transaction_src_id);

CREATE INDEX IF NOT EXISTS ix_fct_trx_date_brin
    ON dim.fct_transactions_dd_dd USING BRIN (transaction_date);

SQL
