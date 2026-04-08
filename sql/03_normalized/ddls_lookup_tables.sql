CREATE SEQUENCE IF NOT EXISTS3nf.seq_nf_product_category_id START 1;
CREATE SEQUENCE IF NOT EXISTS3nf.seq_nf_promotion_type_id START 1;
CREATE SEQUENCE IF NOT EXISTS3nf.seq_nf_shipping_partner_id START 1;

CREATE TABLE IF NOT EXISTS 3nf.nf_product_categories (
    product_category_id      BIGINT PRIMARY KEY,
    product_category_src_id  VARCHAR(100) NOT NULL,
    product_category_name    VARCHAR(100) NOT NULL,
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ce_product_categories_src
        UNIQUE (product_category_src_id)
    );

CREATE TABLE IF NOT EXISTS 3nf.nf_promotion_types (
    promotion_type_id      BIGINT PRIMARY KEY,
    promotion_type_src_id  VARCHAR(255) NOT NULL,
    promotion_type_name    VARCHAR(100) NOT NULL,
    source_system          VARCHAR(100) NOT NULL,
    source_table           VARCHAR(100) NOT NULL,
    insert_dt              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_ce_promotion_types_src_id
        UNIQUE (promotion_type_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_shipping_partners (
    shipping_partner_id      BIGINT PRIMARY KEY,
    shipping_partner_src_id  VARCHAR(100) NOT NULL,
    shipping_partner_name    VARCHAR(100) NOT NULL,
    source_system            VARCHAR(100) NOT NULL,
    source_table             VARCHAR(100) NOT NULL,
    insert_dt                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_ce_shipping_partners_src
        UNIQUE (shipping_partner_src_id)
);

CREATE INDEX IF NOT EXISTS ix_ce_product_categories_src_id
    ON 3nf.nf_product_categories (product_category_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_promotion_types_src
    ON 3nf.nf_promotion_types (promotion_type_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_shipping_partners_src
ON 3nf.nf_shipping_partners (shipping_partner_src_id);
