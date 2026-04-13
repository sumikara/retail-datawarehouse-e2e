%%bash
set -e
DB="retail_dw"

sudo -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'

/* =========================================================
   DEFAULT ROWS FOR DIM LAYER
   Purpose:
   Add -1 surrogate fallback rows so fact loads can safely use
   unknown dimension references.
   ========================================================= */

/* -------------------------
   DIM_CUSTOMERS
   ------------------------- */
INSERT INTO dim.dim_customers (
    customer_surr_id,
    customer_src_id,
    source_system,
    source_table,
    gender,
    marital_status,
    birth_of_dt,
    membership_dt,
    last_purchase_dt,
    customer_zip_code,
    customer_city,
    customer_state,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    DATE '1900-01-01',
    DATE '1900-01-01',
    TIMESTAMP '1900-01-01 00:00:00',
    'n.a.',
    'n.a.',
    'n.a.',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_customers
    WHERE customer_surr_id = -1
);


/* -------------------------
   DIM_STORES
   ------------------------- */
INSERT INTO dim.dim_stores (
    store_surr_id,
    store_src_id,
    source_system,
    source_table,
    store_name,
    store_zip_code,
    store_city,
    store_state,
    store_location,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_stores
    WHERE store_surr_id = -1
);


/* -------------------------
   DIM_PRODUCTS
   ------------------------- */
INSERT INTO dim.dim_products (
    product_surr_id,
    product_src_id,
    source_system,
    source_table,
    product_category,
    product_name,
    product_brand,
    product_stock,
    product_material,
    product_manufacture_dt,
    product_expiry_dt,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    -1,
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '1900-01-01 00:00:00',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_products
    WHERE product_surr_id = -1
);


/* -------------------------
   DIM_PROMOTIONS
   ------------------------- */
INSERT INTO dim.dim_promotions (
    promotion_surr_id,
    promotion_src_id,
    source_system,
    source_table,
    promotion_channel,
    promotion_type,
    promotion_start_dt,
    promotion_end_dt,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '9999-12-31 23:59:59',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_promotions
    WHERE promotion_surr_id = -1
);


/* -------------------------
   DIM_DELIVERIES
   ------------------------- */
INSERT INTO dim.dim_deliveries (
    delivery_surr_id,
    delivery_src_id,
    source_system,
    source_table,
    delivery_type,
    delivery_status,
    shipping_partner,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_deliveries
    WHERE delivery_surr_id = -1
);


/* -------------------------
   DIM_ENGAGEMENTS
   ------------------------- */
INSERT INTO dim.dim_engagements (
    engagement_surr_id,
    engagement_src_id,
    source_system,
    source_table,
    customer_support_calls,
    website_address,
    order_channel,
    customer_support_method,
    issue_status,
    app_usage,
    website_visits,
    social_media_engagement,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    -1,
    'n.a.',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_engagements
    WHERE engagement_surr_id = -1
);


/* -------------------------
   DIM_EMPLOYEES_SCD
   ------------------------- */
INSERT INTO dim.dim_employees_scd (
    employee_surr_id,
    employee_src_id,
    employee_name,
    employee_position,
    employee_salary,
    employee_hire_date,
    start_dt,
    end_dt,
    is_active,
    source_system,
    source_table,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'n.a.',
    'n.a.',
    -1,
    DATE '1900-01-01',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '9999-12-31 23:59:59',
    FALSE,
    'n.a.',
    'n.a.',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_employees_scd
    WHERE employee_surr_id = -1
);


/* -------------------------
   DIM_DATES
   ------------------------- */
INSERT INTO dim.dim_dates (
    date_surr_id,
    full_date,
    day_of_month,
    month_of_year,
    year_of_date,
    quarter_of_year,
    week_of_year,
    day_name,
    month_name,
    is_weekend,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,
    insert_dt
)
SELECT
    -1,
    DATE '1900-01-01',
    1,
    1,
    1900,
    1,
    1,
    'Unknown',
    'Unknown',
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_dates
    WHERE date_surr_id = -1
);


/* -------------------------
   Quick Sanity CHECK
   ------------------------- */
SELECT 'dim_customers'     AS table_name, COUNT(*) AS default_row_count FROM dim.dim_customers     WHERE customer_surr_id = -1
UNION ALL
SELECT 'dim_stores',              COUNT(*) FROM dim.dim_stores          WHERE store_surr_id = -1
UNION ALL
SELECT 'dim_products',            COUNT(*) FROM dim.dim_products        WHERE product_surr_id = -1
UNION ALL
SELECT 'dim_promotions',          COUNT(*) FROM dim.dim_promotions      WHERE promotion_surr_id = -1
UNION ALL
SELECT 'dim_deliveries',          COUNT(*) FROM dim.dim_deliveries      WHERE delivery_surr_id = -1
UNION ALL
SELECT 'dim_engagements',         COUNT(*) FROM dim.dim_engagements     WHERE engagement_surr_id = -1
UNION ALL
SELECT 'dim_employees_scd',       COUNT(*) FROM dim.dim_employees_scd   WHERE employee_surr_id = -1
UNION ALL
SELECT 'dim_dates',               COUNT(*) FROM dim.dim_dates           WHERE date_surr_id = -1
ORDER BY table_name;
