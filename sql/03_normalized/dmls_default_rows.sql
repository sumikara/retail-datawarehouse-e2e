-- states
INSERT INTO 3nf.nf_states (
    state_id, state_src_id, state_name,
    source_system, source_table, insert_dt
)
SELECT
    -1, 'n.a.', 'n.a.',
    'MANUAL', 'MANUAL', NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_states
    WHERE state_id = -1
);
-- cities
INSERT INTO 3nf.nf_cities (
    city_id, city_src_id, city_name, state_id,
    source_system, source_table, insert_dt
)
SELECT
    -1, 'n.a.', 'n.a.', -1,
    'MANUAL', 'MANUAL', NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_cities
    WHERE city_id = -1
);

--addressess
INSERT INTO 3nf.nf_addresses (
    address_id, address_src_id, zip_code, city_id,
    source_system, source_table, insert_dt
)
SELECT
    -1, 'n.a.', 'n.a.', -1,
    'MANUAL', 'MANUAL', NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_addresses
    WHERE address_id = -1
);

-- customers
INSERT INTO 3nf.nf_customers (
    customer_id, customer_src_id, customer_id_nk,
    gender, marital_status,
    birth_of_dt, membership_dt, last_purchase_dt,
    address_id,
    source_system, source_table, insert_dt, update_dt
)
SELECT
    -1, 'n.a.', 'n.a.',
    'n.a.', 'n.a.',
    DATE '1900-01-01',
    DATE '1900-01-01',
    TIMESTAMP '1900-01-01 00:00:00',
    -1,
    'MANUAL', 'MANUAL', NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM 3nf.nf_customers WHERE customer_id = -1
);

-- stores
INSERT INTO 3nf.nf_stores (
    store_id, store_src_id, store_name, address_id,
    source_system, source_table, insert_dt, update_dt
)
SELECT
    -1, 
    'n.a.', 
    'n.a.', 
    -1,
    'MANUAL', 
    'MANUAL', 
    NOW(), 
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_stores
    WHERE store_id = -1
);

-- product categories

INSERT INTO 3nf.nf_product_categories (
    product_category_id, product_category_src_id, product_category_name,
    source_system, source_table, insert_dt
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_product_categories
    WHERE product_category_id = -1
);

-- products


INSERT INTO 3nf.nf_products (
    product_id,
    product_src_id,
    product_category_id,
    product_name,
    product_brand,
    product_stock,
    product_material,
    product_manufacture_dt,
    product_expiry_dt,
    source_system,
    source_table,
    insert_dt,
    update_dt
)
SELECT
    -1,
    'n.a.',
    -1,
    'n.a.',
    'n.a.',
    0,
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '1900-01-01 00:00:00',
    'MANUAL',
    'MANUAL',
    NOW(),
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_products
    WHERE product_id = -1
);

--promotion types
INSERT INTO 3nf.nf_promotion_types (
    promotion_type_id,
    promotion_type_src_id,
    promotion_type_name,
    source_system,
    source_table,
    insert_dt
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    NOW()
    WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_promotion_types
    WHERE promotion_type_id = -1
);

-- promotions
INSERT INTO 3nf.nf_promotions (
    promotion_id,
    promotion_src_id,
    promotion_type_id,
    promotion_channel,
    promotion_start_dt,
    promotion_end_dt,
    source_system,
    source_table,
    insert_dt
)
SELECT
    -1,
    'n.a.',
    -1,
    'n.a.',
    TIMESTAMP '1900-01-01',
    TIMESTAMP '1900-01-01',
    'MANUAL',
    'MANUAL',
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_promotions
    WHERE promotion_id = -1
);

--shipping partners
INSERT INTO 3nf.nf_shipping_partners (
    shipping_partner_id,
    shipping_partner_src_id,
    shipping_partner_name,
    source_system,
    source_table,
    insert_dt
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_shipping_partners
    WHERE shipping_partner_id = -1
);

--deliveries
INSERT INTO 3nf.nf_deliveries (
    delivery_id,
    delivery_src_id,
    shipping_partner_id,
    delivery_type,
    delivery_status,
    source_system,
    source_table,
    insert_dt
)
SELECT
    -1,
    'n.a.',
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_deliveries
    WHERE delivery_id = -1
);

--engagements
INSERT INTO 3nf.nf_engagements (
    engagement_id,
    engagement_src_id,
    customer_support_calls,
    website_address,
    order_channel,
    customer_support_method,
    issue_status,
    app_usage,
    website_visits,
    social_media_engagement,
    source_system,
    source_table,
    insert_dt
)
SELECT
    -1,
    'n.a.',
    0,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    0,
    'n.a.',
    'MANUAL',
    'MANUAL',
    NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_engagements
    WHERE engagement_id = -1
);

-- employees
INSERT INTO 3nf.nf_employees_scd (
    employee_id,
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
    'n.a.',
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
    FROM 3nf.nf_employees_scd
    WHERE employee_id = -1
      AND start_dt = TIMESTAMP '1900-01-01 00:00:00'
);


--transactions
INSERT INTO 3nf.nf_transactions (
    transaction_id,
    transaction_dt,
    total_sales,
    payment_method,
    quantity,
    unit_price,
    discount_applied,
    day_of_week,
    week_of_year,
    month_of_year,
    store_id,
    customer_id,
    promotion_id,
    delivery_id,
    product_id,
    engagement_id,
    city_id,
    employee_id,
    row_sig,
    source_system,
    source_table
)
SELECT
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    0,
    'n.a.',
    0,
    0,
    0,
    'n.a.',
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL'
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_transactions
    WHERE row_sig = 'n.a.'
);

