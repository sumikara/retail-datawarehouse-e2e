
-- # 20_NORMALIZED LAYER  - seq + ddls + indexes

CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_state_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_city_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_address_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_product_category_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_promotion_type_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_shipping_partner_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_customer_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_store_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_product_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_promotion_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_delivery_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_engagement_id START 1;
CREATE SEQUENCE IF NOT EXISTS nf.seq_nf_employee_id START 1;

CREATE TABLE IF NOT EXISTS nf.nf_states (
    state_id        BIGINT PRIMARY KEY,
    state_src_id    VARCHAR(100) NOT NULL,
    state_name      VARCHAR(100) NOT NULL,
    source_system   VARCHAR(100) NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    insert_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_states_src UNIQUE (state_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_cities (
    city_id         BIGINT PRIMARY KEY,
    city_src_id     VARCHAR(150) NOT NULL,
    city_name       VARCHAR(100) NOT NULL,
    state_id        BIGINT NOT NULL,
    source_system   VARCHAR(100),
    source_table    VARCHAR(100),
    insert_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_cities_state
        FOREIGN KEY (state_id) REFERENCES nf.nf_states(state_id),
    CONSTRAINT uq_nf_cities_src UNIQUE (city_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_addresses (
    address_id      BIGINT PRIMARY KEY,
    address_src_id  VARCHAR(200) NOT NULL,
    zip_code        VARCHAR(30),
    city_id         BIGINT NOT NULL,
    source_system   VARCHAR(100),
    source_table    VARCHAR(100),
    insert_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_addresses_city
        FOREIGN KEY (city_id) REFERENCES nf.nf_cities(city_id),
    CONSTRAINT uq_nf_addresses_src UNIQUE (address_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_product_categories (
    product_category_id      BIGINT PRIMARY KEY,
    product_category_src_id  VARCHAR(100) NOT NULL,
    product_category_name    VARCHAR(100) NOT NULL,
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_product_categories_src UNIQUE (product_category_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_promotion_types (
    promotion_type_id      BIGINT PRIMARY KEY,
    promotion_type_src_id  VARCHAR(255) NOT NULL,
    promotion_type_name    VARCHAR(100) NOT NULL,
    source_system          VARCHAR(100) NOT NULL,
    source_table           VARCHAR(100) NOT NULL,
    insert_dt              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_promotion_types_src UNIQUE (promotion_type_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_shipping_partners (
    shipping_partner_id      BIGINT PRIMARY KEY,
    shipping_partner_src_id  VARCHAR(100) NOT NULL,
    shipping_partner_name    VARCHAR(100) NOT NULL,
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_shipping_partners_src UNIQUE (shipping_partner_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_customers (
    customer_id        BIGINT PRIMARY KEY,
    customer_src_id    VARCHAR(255) NOT NULL,
    customer_id_nk     VARCHAR(100) NOT NULL,
    gender             VARCHAR(20) NOT NULL,
    marital_status     VARCHAR(20) NOT NULL,
    birth_of_dt        DATE,
    membership_dt      DATE,
    last_purchase_dt   TIMESTAMP,
    address_id         BIGINT NOT NULL,
    source_system      VARCHAR(100) NOT NULL,
    source_table       VARCHAR(100) NOT NULL,
    insert_dt          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_customers_address
        FOREIGN KEY (address_id) REFERENCES nf.nf_addresses(address_id),
    CONSTRAINT uq_nf_customers_src UNIQUE (customer_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_stores (
    store_id            BIGINT PRIMARY KEY,
    store_src_id        VARCHAR(255) NOT NULL,
    store_name          VARCHAR(100),
    store_location_nk   VARCHAR(100),
    address_id          BIGINT NOT NULL,
    source_system       VARCHAR(100),
    source_table        VARCHAR(100),
    insert_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_stores_address
        FOREIGN KEY (address_id) REFERENCES nf.nf_addresses(address_id),
    CONSTRAINT uq_nf_stores_src UNIQUE (store_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_products (
    product_id               BIGINT PRIMARY KEY,
    product_src_id           VARCHAR(255) NOT NULL,
    product_id_nk            VARCHAR(100),
    product_category_id      BIGINT NOT NULL,
    product_name             VARCHAR(100) NOT NULL,
    product_brand            VARCHAR(100) NOT NULL,
    product_stock            INTEGER,
    product_material         VARCHAR(100) NOT NULL,
    product_manufacture_dt   TIMESTAMP,
    product_expiry_dt        TIMESTAMP,
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    insert_dt                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_products_category
        FOREIGN KEY (product_category_id) REFERENCES nf.nf_product_categories(product_category_id),
    CONSTRAINT uq_nf_products_src UNIQUE (product_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_promotions (
    promotion_id        BIGINT PRIMARY KEY,
    promotion_src_id    VARCHAR(255) NOT NULL,
    promotion_id_nk     VARCHAR(100),
    promotion_type_id   BIGINT NOT NULL,
    promotion_channel   VARCHAR(100) NOT NULL,
    promotion_start_dt  TIMESTAMP,
    promotion_end_dt    TIMESTAMP,
    source_system       VARCHAR(100) NOT NULL,
    source_table        VARCHAR(100) NOT NULL,
    insert_dt           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_promotions_type
        FOREIGN KEY (promotion_type_id) REFERENCES nf.nf_promotion_types(promotion_type_id),
    CONSTRAINT uq_nf_promotions_src UNIQUE (promotion_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_deliveries (
    delivery_id          BIGINT PRIMARY KEY,
    delivery_src_id      VARCHAR(255) NOT NULL,
    delivery_id_nk       VARCHAR(100),
    shipping_partner_id  BIGINT NOT NULL,
    delivery_type        VARCHAR(100) NOT NULL,
    delivery_status      VARCHAR(100) NOT NULL,
    source_system        VARCHAR(100) NOT NULL,
    source_table         VARCHAR(100) NOT NULL,
    insert_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_deliveries_shipping_partner
        FOREIGN KEY (shipping_partner_id) REFERENCES nf.nf_shipping_partners(shipping_partner_id),
    CONSTRAINT uq_nf_deliveries_src UNIQUE (delivery_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_engagements (
    engagement_id              BIGINT PRIMARY KEY,
    engagement_src_id          VARCHAR(100) NOT NULL,
    engagement_id_nk           VARCHAR(100),
    customer_support_calls     INTEGER,
    website_address            VARCHAR(250) NOT NULL,
    order_channel              VARCHAR(100) NOT NULL,
    customer_support_method    VARCHAR(100) NOT NULL,
    issue_status               VARCHAR(100) NOT NULL,
    app_usage                  VARCHAR(100) NOT NULL,
    website_visits             INTEGER,
    social_media_engagement    VARCHAR(100) NOT NULL,
    source_system              VARCHAR(100) NOT NULL,
    source_table               VARCHAR(100) NOT NULL,
    insert_dt                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_engagements_src UNIQUE (engagement_src_id)
);

CREATE TABLE IF NOT EXISTS nf.nf_employees_scd (
    employee_id         BIGINT,
    employee_src_id     VARCHAR(255) NOT NULL,
    employee_name_nk    VARCHAR(100),
    employee_position   VARCHAR(100),
    employee_salary     NUMERIC(10,2),
    employee_hire_date  DATE,
    start_dt            TIMESTAMP NOT NULL,
    end_dt              TIMESTAMP NOT NULL,
    is_active           BOOLEAN NOT NULL,
    source_system       VARCHAR(100),
    source_table        VARCHAR(100),
    insert_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_nf_employees_scd PRIMARY KEY (employee_id, start_dt)
);

CREATE TABLE IF NOT EXISTS nf.nf_transactions (
    transaction_id      VARCHAR(100) NOT NULL,
    transaction_dt      TIMESTAMP NOT NULL,
    total_sales         NUMERIC(10,2) NOT NULL DEFAULT 0,
    payment_method      VARCHAR(100) NOT NULL DEFAULT 'n.a.',
    quantity            INTEGER NOT NULL DEFAULT 0,
    unit_price          NUMERIC(10,2) NOT NULL DEFAULT 0,
    discount_applied    NUMERIC(10,2) NOT NULL DEFAULT 0,
    day_of_week         VARCHAR(20) NOT NULL DEFAULT 'n.a.',
    week_of_year        INTEGER NOT NULL DEFAULT -1,
    month_of_year       INTEGER NOT NULL DEFAULT -1,
    store_id            BIGINT NOT NULL DEFAULT -1,
    customer_id         BIGINT NOT NULL DEFAULT -1,
    promotion_id        BIGINT NOT NULL DEFAULT -1,
    delivery_id         BIGINT NOT NULL DEFAULT -1,
    product_id          BIGINT NOT NULL DEFAULT -1,
    engagement_id       BIGINT NOT NULL DEFAULT -1,
    city_id             BIGINT NOT NULL DEFAULT -1,
    employee_id         BIGINT NOT NULL DEFAULT -1,
    row_sig             TEXT NOT NULL,
    source_system       VARCHAR(100) NOT NULL,
    source_table        VARCHAR(100) NOT NULL,
    insert_dt           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_nf_transactions_store
        FOREIGN KEY (store_id) REFERENCES nf.nf_stores(store_id),
    CONSTRAINT fk_nf_transactions_customer
        FOREIGN KEY (customer_id) REFERENCES nf.nf_customers(customer_id),
    CONSTRAINT fk_nf_transactions_promotion
        FOREIGN KEY (promotion_id) REFERENCES nf.nf_promotions(promotion_id),
    CONSTRAINT fk_nf_transactions_delivery
        FOREIGN KEY (delivery_id) REFERENCES nf.nf_deliveries(delivery_id),
    CONSTRAINT fk_nf_transactions_product
        FOREIGN KEY (product_id) REFERENCES nf.nf_products(product_id),
    CONSTRAINT fk_nf_transactions_engagement
        FOREIGN KEY (engagement_id) REFERENCES nf.nf_engagements(engagement_id),
    CONSTRAINT fk_nf_transactions_city
        FOREIGN KEY (city_id) REFERENCES nf.nf_cities(city_id)
);

CREATE INDEX IF NOT EXISTS idx_nf_states_src
    ON nf.nf_states (state_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_cities_src
    ON nf.nf_cities (city_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_addresses_src
    ON nf.nf_addresses (address_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_product_categories_src
    ON nf.nf_product_categories (product_category_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_promotion_types_src
    ON nf.nf_promotion_types (promotion_type_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_shipping_partners_src
    ON nf.nf_shipping_partners (shipping_partner_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_customers_src
    ON nf.nf_customers (customer_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_customers_nk
    ON nf.nf_customers (customer_id_nk);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_customer
    ON nf.nf_transactions (customer_id);

CREATE INDEX IF NOT EXISTS idx_nf_customers_address
    ON nf.nf_customers (address_id);

CREATE INDEX IF NOT EXISTS idx_nf_stores_src
    ON nf.nf_stores (store_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_product
    ON nf.nf_transactions (product_id);

CREATE INDEX IF NOT EXISTS idx_nf_products_src
    ON nf.nf_products (product_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_products_nk
    ON nf.nf_products (product_id_nk);

CREATE INDEX IF NOT EXISTS idx_nf_products_category
    ON nf.nf_products (product_category_id);

CREATE INDEX IF NOT EXISTS idx_nf_promotions_src
    ON nf.nf_promotions (promotion_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_promotions_nk
    ON nf.nf_promotions (promotion_id_nk);

CREATE INDEX IF NOT EXISTS idx_nf_deliveries_src
    ON nf.nf_deliveries (delivery_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_deliveries_nk
    ON nf.nf_deliveries (delivery_id_nk);

CREATE INDEX IF NOT EXISTS idx_nf_deliveries_partner
    ON nf.nf_deliveries (shipping_partner_id);

CREATE INDEX IF NOT EXISTS idx_nf_deliveries_type
    ON nf.nf_deliveries (delivery_type);

CREATE INDEX IF NOT EXISTS idx_nf_engagements_src
    ON nf.nf_engagements (engagement_src_id);

CREATE INDEX IF NOT EXISTS idx_nf_engagements_nk
    ON nf.nf_engagements (engagement_id_nk);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_employee
    ON nf.nf_transactions (employee_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_nf_emp_active
    ON nf.nf_employees_scd (employee_src_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_nf_emp_src_start
    ON nf.nf_employees_scd (employee_src_id, start_dt);

CREATE INDEX IF NOT EXISTS idx_nf_emp_lookup
    ON nf.nf_employees_scd (employee_src_id, is_active, start_dt, end_dt);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_trx_id
    ON nf.nf_transactions (transaction_id);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_trx_dt_brin
    ON nf.nf_transactions USING brin (transaction_dt);

CREATE UNIQUE INDEX IF NOT EXISTS ux_nf_transactions_row_sig
    ON nf.nf_transactions (row_sig);

CREATE INDEX IF NOT EXISTS idx_nf_transactions_source
    ON nf.nf_transactions (source_system, source_table);

