CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_customer_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_store_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_product_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_promotion_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_delivery_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_engagement_id START 1;
CREATE SEQUENCE IF NOT EXISTS dim.seq_dim_employee_id START 1;

--customers
CREATE TABLE IF NOT EXISTS dim.dim_customers (
    customer_surr_id     BIGINT PRIMARY KEY,                 -- DM surrogate key
    customer_src_id      BIGINT NOT NULL,                    -- 3NF surrogate: ce_customers.customer_id
    source_system        VARCHAR(100) NOT NULL,
    source_table         VARCHAR(100) NOT NULL,
    customer_id_nk       VARCHAR(100) NOT NULL,              -- raw trace key from 3NF
    gender               VARCHAR(20)  NOT NULL,
    marital_status       VARCHAR(20)  NOT NULL,
    birth_of_dt          DATE         NOT NULL,
    membership_dt        DATE         NOT NULL,
    last_purchase_dt     TIMESTAMP    NOT NULL,
    customer_zip_code    VARCHAR(30)  NOT NULL,
    customer_city        VARCHAR(100) NOT NULL,
    customer_state       VARCHAR(100) NOT NULL,
    insert_dt            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);


--stores
CREATE TABLE IF NOT EXISTS dim.dim_stores (
    store_surr_id      BIGINT PRIMARY KEY,          -- DM surrogate key
    store_src_id       BIGINT NOT NULL,             -- 3NF surrogate: ce_stores.store_id
    source_system      VARCHAR(100) NOT NULL,
    source_table       VARCHAR(100) NOT NULL,
    store_id_nk        VARCHAR(255) NOT NULL,       -- 3NF business key: ce_stores.store_src_id
    store_name         VARCHAR(100) NOT NULL,
    store_zip_code     VARCHAR(30)  NOT NULL,
    store_city         VARCHAR(100) NOT NULL,
    store_state        VARCHAR(100) NOT NULL,
    store_location     VARCHAR(100) NOT NULL,
    insert_dt          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- products
CREATE TABLE IF NOT EXISTS dim.dim_products (
    product_surr_id          BIGINT PRIMARY KEY,      -- DM surrogate
    product_src_id           BIGINT NOT NULL,         -- 3NF surrogate: ce_products.product_id
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    product_category_name    VARCHAR(100) NOT NULL,
    product_name             VARCHAR(100) NOT NULL,
    product_brand            VARCHAR(100) NOT NULL,
    product_stock            INTEGER,
    product_material         VARCHAR(100) NOT NULL,
    product_manufacture_dt   TIMESTAMP,
    product_expiry_dt        TIMESTAMP,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--promotions
CREATE TABLE IF NOT EXISTS dim.dim_promotions (
    promotion_surr_id     BIGINT PRIMARY KEY,
    promotion_src_id      BIGINT NOT NULL,     -- 3NF surrogate: ce_promotions.promotion_id
    source_system         VARCHAR(100) NOT NULL,
    source_table          VARCHAR(100) NOT NULL,
    promotion_id_nk       VARCHAR(255) NOT NULL,   -- 3NF business key: ce_promotions.promotion_src_id
    promotion_channel     VARCHAR(100) NOT NULL,
    promotion_type_name   VARCHAR(100) NOT NULL,
    promotion_start_dt    TIMESTAMP,
    promotion_end_dt      TIMESTAMP,
    insert_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- deliveries
CREATE TABLE IF NOT EXISTS dim.dim_deliveries (
    delivery_surr_id      BIGINT PRIMARY KEY,
    delivery_src_id       BIGINT NOT NULL,   -- 3NF surrogate: ce_deliveries.delivery_id
    source_system         VARCHAR(100) NOT NULL,
    source_table          VARCHAR(100) NOT NULL,
    delivery_id_nk        VARCHAR(100) NOT NULL,   -- 3NF business key: ce_deliveries.delivery_src_id
    delivery_type         VARCHAR(100) NOT NULL,
    delivery_status       VARCHAR(100) NOT NULL,
    shipping_partner_name VARCHAR(100) NOT NULL,
    insert_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--engagements
CREATE TABLE IF NOT EXISTS dim.dim_engagements (
    engagement_surr_id         BIGINT PRIMARY KEY,
    engagement_src_id          BIGINT NOT NULL,   -- 3NF surrogate: ce_engagements.engagement_id
    source_system              VARCHAR(100) NOT NULL,
    source_table               VARCHAR(100) NOT NULL,
    engagement_id_nk           VARCHAR(100) NOT NULL,
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

-- employees
CREATE TABLE IF NOT EXISTS dim.dim_employees_scd (
    employee_surr_id   BIGINT PRIMARY KEY,
    employee_src_id    BIGINT NOT NULL,
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


CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customers_src_id
    ON dim.dim_customers (customer_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_customers_nk
    ON dim.dim_customers (customer_id_nk);

CREATE INDEX IF NOT EXISTS ix_dim_customers_city_state
    ON dim.dim_customers (customer_city, customer_state);

CREATE INDEX IF NOT EXISTS ix_dim_customers_membership_dt
    ON dim.dim_customers (membership_dt);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_stores_src_id
    ON dim.dim_stores (store_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_stores_nk
    ON dim.dim_stores (store_id_nk);

CREATE INDEX IF NOT EXISTS ix_dim_stores_city_state
    ON dim.dim_stores (store_city, store_state);

CREATE INDEX IF NOT EXISTS ix_dim_stores_location
    ON dim.dim_stores (store_location);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_products_src_id
    ON dim.dim_products (product_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_products_category
    ON dim.dim_products (product_category_name);

CREATE INDEX IF NOT EXISTS ix_dim_products_name_brand
    ON dim.dim_products (product_name, product_brand);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_promotions_src_id
    ON dim.dim_promotions (promotion_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_promotions_nk
    ON dim.dim_promotions (promotion_id_nk);

CREATE INDEX IF NOT EXISTS ix_dim_promotions_type
    ON dim.dim_promotions (promotion_type_name);

CREATE INDEX IF NOT EXISTS ix_dim_promotions_channel
    ON dim.dim_promotions (promotion_channel);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_deliveries_src_id
    ON dim.dim_deliveries (delivery_src_id);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_nk
    ON dim.dim_deliveries (delivery_id_nk);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_type
    ON dim.dim_deliveries (delivery_type);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_status
    ON dim.dim_deliveries (delivery_status);

CREATE INDEX IF NOT EXISTS ix_dim_deliveries_partner
    ON dim.dim_deliveries (shipping_partner_name);

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
