-- customers
INSERT INTO dim.dim_customers (
    customer_surr_id,
    customer_src_id,
    source_system,
    source_table,
    customer_id_nk,
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
    'MANUAL',
    'MANUAL',
    'n.a.',
    'n.a.',
    'n.a.',
    DATE '1900-01-01',
    DATE '1900-01-01',
    TIMESTAMP '1900-01-01 00:00:00',
    'n.a.',
    'n.a.',
    'n.a.',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_customers
    WHERE customer_surr_id = -1
);

--stores

INSERT INTO dim.dim_stores (
    store_surr_id,
    store_src_id,
    source_system,
    source_table,
    store_id_nk,
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
    'MANUAL',
    'MANUAL',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_stores
    WHERE store_surr_id = -1
);

--products
INSERT INTO dim.dim_products (
    product_surr_id,
    product_src_id,
    source_system,
    source_table,
    product_category_name,
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
    'MANUAL',
    'MANUAL',
    'n.a.',
    'n.a.',
    'n.a.',
    0,
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '1900-01-01 00:00:00',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_products
    WHERE product_surr_id = -1
);

--promotions
INSERT INTO dim.dim_promotions (
    promotion_surr_id,
    promotion_src_id,
    source_system,
    source_table,
    promotion_id_nk,
    promotion_channel,
    promotion_type_name,
    promotion_start_dt,
    promotion_end_dt,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    'n.a.',
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '1900-01-01 00:00:00',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_promotions
    WHERE promotion_surr_id = -1
);

-- deliveries
INSERT INTO dim.dim_deliveries (
    delivery_surr_id,
    delivery_src_id,
    source_system,
    source_table,
    delivery_id_nk,
    delivery_type,
    delivery_status,
    shipping_partner_name,
    insert_dt,
    update_dt
)
SELECT
    -1,
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_deliveries
    WHERE delivery_surr_id = -1
);


--engagements
INSERT INTO dim.dim_engagements (
    engagement_surr_id,
    engagement_src_id,
    source_system,
    source_table,
    engagement_id_nk,
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
    'MANUAL',
    'MANUAL',
    'n.a.',
    0,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    0,
    'n.a.',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_engagements
    WHERE engagement_surr_id = -1
);

-- employees
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
    'MANUAL',
    'MANUAL',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM dim.dim_employees_scd
    WHERE employee_surr_id = -1
);






