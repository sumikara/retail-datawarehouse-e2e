CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_customer_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_store_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_product_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_promotion_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_delivery_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_engagement_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_employee_id START 1;

CREATE TABLE IF NOT EXISTS 3nf.nf_customers(
    customer_id        BIGINT PRIMARY KEY,          -- surrogate key
    customer_src_id    VARCHAR(255) NOT NULL,       -- engineered stable key
    customer_id_nk     VARCHAR(100) NOT NULL,       -- raw source key (traceability)
    gender            VARCHAR(20)  NOT NULL,
    marital_status    VARCHAR(20)  NOT NULL,
    birth_of_dt       DATE         NOT NULL,
    membership_dt     DATE         NOT NULL,
    last_purchase_dt  TIMESTAMP    NOT NULL,
    address_id        BIGINT NOT NULL,
    source_system     VARCHAR(100) NOT NULL,              
    source_table      VARCHAR(100) NOT NULL,             
    insert_dt         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT fk_nf_customers_address
    FOREIGN KEY (address_id) REFERENCES 3nf.nf_addresses(address_id),
CONSTRAINT uq_nf_customers_src UNIQUE (customer_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_stores (
    store_id        BIGINT PRIMARY KEY,
    store_src_id    VARCHAR(255) NOT NULL,
    store_name      VARCHAR(100),
    address_id      BIGINT NOT NULL,
    source_system   VARCHAR(100),
    source_table    VARCHAR(100),
    insert_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT fk_store_address
  FOREIGN KEY (address_id) REFERENCES 3nf.nf_addresses(address_id),
CONSTRAINT uq_nf_stores_src UNIQUE (store_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_products(
    product_id               BIGINT PRIMARY KEY,
    product_src_id           VARCHAR(255) NOT NULL,
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
        FOREIGN KEY (product_category_id)
        REFERENCES 3nf.nf_product_categories(product_category_id),
    CONSTRAINT uq_nf_products_src UNIQUE (product_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_promotions (
    promotion_id        BIGINT PRIMARY KEY,
    promotion_src_id    VARCHAR(255) NOT NULL,   -- business key
    promotion_type_id   BIGINT NOT NULL,
    promotion_channel   VARCHAR(100) NOT NULL,
    promotion_start_dt  TIMESTAMP,
    promotion_end_dt    TIMESTAMP,
    source_system       VARCHAR(100) NOT NULL,
    source_table        VARCHAR(100) NOT NULL,
    insert_dt           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nf_promotions_type
        FOREIGN KEY (promotion_type_id) REFERENCES 3nf.nf_promotion_types (promotion_type_id),
    CONSTRAINT ux_nf_promotions_src_id UNIQUE (promotion_src_id)
  );

CREATE TABLE IF NOT EXISTS 3nf.nf_deliveries (
    delivery_id           BIGINT PRIMARY KEY,
    delivery_src_id        VARCHAR(100) NOT NULL,
    shipping_partner_id   BIGINT NOT NULL,
    delivery_type         VARCHAR(100) NOT NULL,
    delivery_status       VARCHAR(100) NOT NULL,
    source_system         VARCHAR(100) NOT NULL,
    source_table          VARCHAR(100) NOT NULL,
    insert_dt             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_nf_deliveries_shipping_partner
        FOREIGN KEY (shipping_partner_id) REFERENCES 3nf.nf_shipping_partners (shipping_partner_id),
   CONSTRAINT ux_nf_deliveries_src UNIQUE (delivery_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_engagements (
    engagement_id              BIGINT PRIMARY KEY,
    engagement_src_id          VARCHAR(100) NOT NULL,
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
    CONSTRAINT ux_nf_engagements_src
    UNIQUE (engagement_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_employees_scd (
    employee_id        BIGINT,
    employee_src_id    VARCHAR(255) NOT NULL,
    employee_name      VARCHAR(100),
    employee_position  VARCHAR(100),
    employee_salary    NUMERIC(10,2),
    employee_hire_date DATE,
    start_dt           TIMESTAMP NOT NULL,
    end_dt             TIMESTAMP NOT NULL,
    is_active          BOOLEAN NOT NULL,
    source_system      VARCHAR(100),
    source_table       VARCHAR(100),
    insert_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT pk_nf_emp PRIMARY KEY (employee_id, start_dt)
);

CREATE INDEX IF NOT EXISTS ix_nf_customers_src_id ON 3nf.nf_customers (customer_src_id);
CREATE INDEX IF NOT EXISTS ix_nf_customers_nk ON 3nf.nf_customers (customer_id_nk);
CREATE INDEX IF NOT EXISTS ix_nf_customers_address_id ON 3nf.nf_customers (address_id);
CREATE INDEX IF NOT EXISTS idx_nf_stores_src ON 3nf.nf_stores(store_src_id);
CREATE INDEX IF NOT EXISTS ix_nf_products_src_id ON 3nf.nf_products (product_src_id);
CREATE INDEX IF NOT EXISTS ix_nf_products_category_id ON 3nf.nf_products (product_category_id);
CREATE INDEX IF NOT EXISTS ix_nf_products_name_brand ON 3nf.nf_products (product_name, product_brand);
CREATE INDEX IF NOT EXISTS ix_nf_promotions_src_id ON 3nf.nf_promotions (promotion_src_id);
CREATE INDEX IF NOT EXISTS ix_nf_deliveries_partner_id ON 3nf.nf_deliveries (shipping_partner_id);
CREATE INDEX IF NOT EXISTS ix_nf_deliveries_type ON 3nf.nf_deliveries (delivery_type);
CREATE INDEX IF NOT EXISTS ix_nf_engagements_src ON 3nf.nf_engagements (engagement_src_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq _nf_emp_active
ON 3nf.nf_employees_scd (employee_src_id)
WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS ix_nf_emp_src_start ON 3nf.nf_employees_scd (employee_src_id, start_dt);
CREATE INDEX IF NOT EXISTS ix_nf_emp_lookup ON 3nf.nf_employees_scd (employee_src_id, is_active, start_dt, end_dt);
