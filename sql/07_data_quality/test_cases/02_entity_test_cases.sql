/*
Entity Test Cases (5 tests per core entity)
Core entities: Customer, Store, Product, Promotion, Delivery, Engagement, Employee, Transaction
*/

/* ===============================
   CUSTOMER (5 tests)
   =============================== */

-- TC_CUST_01 | Dimension: Completeness | Entity: Customer
-- Check missing natural customer identifiers in mapping layer.
SELECT COUNT(*) AS missing_customer_id_nk
FROM stg.mapping_customers
WHERE customer_id_nk IS NULL OR TRIM(customer_id_nk) = '';

-- TC_CUST_02 | Dimension: Uniqueness | Entity: Customer
-- Check duplicate customer source identities in normalized layer.
SELECT customer_src_id, COUNT(*) AS dup_count
FROM nf.nf_customers
GROUP BY customer_src_id
HAVING COUNT(*) > 1;

-- TC_CUST_03 | Dimension: Validity | Entity: Customer
-- Check impossible birth dates (future dates).
SELECT COUNT(*) AS future_birth_dates
FROM nf.nf_customers
WHERE birth_of_dt > CURRENT_DATE;

-- TC_CUST_04 | Dimension: Consistency | Entity: Customer
-- Ensure each customer has a valid address reference.
SELECT COUNT(*) AS orphan_customer_addresses
FROM nf.nf_customers c
LEFT JOIN nf.nf_addresses a ON a.address_id = c.address_id
WHERE a.address_id IS NULL;

-- TC_CUST_05 | Dimension: Consistency | Entity: Customer
-- Compare normalized customer count with dimensional customer count.
SELECT
    (SELECT COUNT(*) FROM nf.nf_customers WHERE customer_id <> -1) AS nf_customers,
    (SELECT COUNT(*) FROM dim.dim_customers) AS dim_customers;

/* ===============================
   STORE (5 tests)
   =============================== */

-- TC_STORE_01 | Dimension: Completeness | Entity: Store
SELECT COUNT(*) AS missing_store_src_id
FROM stg.mapping_stores
WHERE store_src_id IS NULL OR TRIM(store_src_id) = '';

-- TC_STORE_02 | Dimension: Uniqueness | Entity: Store
SELECT store_src_id, COUNT(*) AS dup_count
FROM nf.nf_stores
GROUP BY store_src_id
HAVING COUNT(*) > 1;

-- TC_STORE_03 | Dimension: Validity | Entity: Store
SELECT COUNT(*) AS missing_store_location
FROM nf.nf_stores
WHERE store_location_nk IS NULL OR TRIM(store_location_nk) = '';

-- TC_STORE_04 | Dimension: Consistency | Entity: Store
SELECT COUNT(*) AS orphan_store_addresses
FROM nf.nf_stores s
LEFT JOIN nf.nf_addresses a ON a.address_id = s.address_id
WHERE a.address_id IS NULL;

-- TC_STORE_05 | Dimension: Consistency | Entity: Store
SELECT
    (SELECT COUNT(*) FROM nf.nf_stores WHERE store_id <> -1) AS nf_stores,
    (SELECT COUNT(*) FROM dim.dim_stores) AS dim_stores;

/* ===============================
   PRODUCT (5 tests)
   =============================== */

-- TC_PROD_01 | Dimension: Completeness | Entity: Product
SELECT COUNT(*) AS missing_product_src_id
FROM stg.mapping_products
WHERE product_src_id IS NULL OR TRIM(product_src_id) = '';

-- TC_PROD_02 | Dimension: Uniqueness | Entity: Product
SELECT product_src_id, COUNT(*) AS dup_count
FROM nf.nf_products
GROUP BY product_src_id
HAVING COUNT(*) > 1;

-- TC_PROD_03 | Dimension: Validity | Entity: Product
SELECT COUNT(*) AS negative_stock
FROM nf.nf_products
WHERE product_stock < 0;

-- TC_PROD_04 | Dimension: Consistency | Entity: Product
SELECT COUNT(*) AS orphan_product_category
FROM nf.nf_products p
LEFT JOIN nf.nf_product_categories c ON c.product_category_id = p.product_category_id
WHERE c.product_category_id IS NULL;

-- TC_PROD_05 | Dimension: Consistency | Entity: Product
SELECT
    (SELECT COUNT(*) FROM nf.nf_products WHERE product_id <> -1) AS nf_products,
    (SELECT COUNT(*) FROM dim.dim_products) AS dim_products;

/* ===============================
   PROMOTION (5 tests)
   =============================== */

-- TC_PROMO_01 | Dimension: Completeness | Entity: Promotion
SELECT COUNT(*) AS missing_promotion_src_id
FROM stg.mapping_promotions
WHERE promotion_src_id IS NULL OR TRIM(promotion_src_id) = '';

-- TC_PROMO_02 | Dimension: Uniqueness | Entity: Promotion
SELECT promotion_src_id, COUNT(*) AS dup_count
FROM nf.nf_promotions
GROUP BY promotion_src_id
HAVING COUNT(*) > 1;

-- TC_PROMO_03 | Dimension: Validity | Entity: Promotion
SELECT COUNT(*) AS invalid_promotion_dates
FROM nf.nf_promotions
WHERE promotion_end_dt < promotion_start_dt;

-- TC_PROMO_04 | Dimension: Consistency | Entity: Promotion
SELECT COUNT(*) AS orphan_promotion_type
FROM nf.nf_promotions p
LEFT JOIN nf.nf_promotion_types t ON t.promotion_type_id = p.promotion_type_id
WHERE t.promotion_type_id IS NULL;

-- TC_PROMO_05 | Dimension: Consistency | Entity: Promotion
SELECT
    (SELECT COUNT(*) FROM nf.nf_promotions WHERE promotion_id <> -1) AS nf_promotions,
    (SELECT COUNT(*) FROM dim.dim_promotions) AS dim_promotions;

/* ===============================
   DELIVERY (5 tests)
   =============================== */

-- TC_DELIV_01 | Dimension: Completeness | Entity: Delivery
SELECT COUNT(*) AS missing_delivery_src_id
FROM stg.mapping_deliveries
WHERE delivery_src_id IS NULL OR TRIM(delivery_src_id) = '';

-- TC_DELIV_02 | Dimension: Uniqueness | Entity: Delivery
SELECT delivery_src_id, COUNT(*) AS dup_count
FROM nf.nf_deliveries
GROUP BY delivery_src_id
HAVING COUNT(*) > 1;

-- TC_DELIV_03 | Dimension: Validity | Entity: Delivery
SELECT COUNT(*) AS missing_delivery_status
FROM nf.nf_deliveries
WHERE delivery_status IS NULL OR TRIM(delivery_status) = '';

-- TC_DELIV_04 | Dimension: Consistency | Entity: Delivery
SELECT COUNT(*) AS orphan_shipping_partner
FROM nf.nf_deliveries d
LEFT JOIN nf.nf_shipping_partners p ON p.shipping_partner_id = d.shipping_partner_id
WHERE p.shipping_partner_id IS NULL;

-- TC_DELIV_05 | Dimension: Consistency | Entity: Delivery
SELECT
    (SELECT COUNT(*) FROM nf.nf_deliveries WHERE delivery_id <> -1) AS nf_deliveries,
    (SELECT COUNT(*) FROM dim.dim_deliveries) AS dim_deliveries;

/* ===============================
   ENGAGEMENT (5 tests)
   =============================== */

-- TC_ENG_01 | Dimension: Completeness | Entity: Engagement
SELECT COUNT(*) AS missing_engagement_id_nk
FROM stg.mapping_engagements
WHERE engagement_id_nk IS NULL OR TRIM(engagement_id_nk) = '';

-- TC_ENG_02 | Dimension: Uniqueness | Entity: Engagement
SELECT engagement_src_id, COUNT(*) AS dup_count
FROM nf.nf_engagements
GROUP BY engagement_src_id
HAVING COUNT(*) > 1;

-- TC_ENG_03 | Dimension: Validity | Entity: Engagement
SELECT COUNT(*) AS negative_support_calls
FROM nf.nf_engagements
WHERE customer_support_calls < 0;

-- TC_ENG_04 | Dimension: Validity | Entity: Engagement
SELECT COUNT(*) AS negative_website_visits
FROM nf.nf_engagements
WHERE website_visits < 0;

-- TC_ENG_05 | Dimension: Consistency | Entity: Engagement
SELECT
    (SELECT COUNT(*) FROM nf.nf_engagements WHERE engagement_id <> -1) AS nf_engagements,
    (SELECT COUNT(*) FROM dim.dim_engagements) AS dim_engagements;

/* ===============================
   EMPLOYEE (5 tests)
   =============================== */

-- TC_EMP_01 | Dimension: Completeness | Entity: Employee
SELECT COUNT(*) AS missing_employee_src_id
FROM stg.mapping_employees
WHERE employee_src_id IS NULL OR TRIM(employee_src_id) = '';

-- TC_EMP_02 | Dimension: Uniqueness | Entity: Employee
-- More than one active SCD row for the same employee should not happen.
SELECT employee_src_id, COUNT(*) AS active_versions
FROM nf.nf_employees_scd
WHERE is_active = TRUE
GROUP BY employee_src_id
HAVING COUNT(*) > 1;

-- TC_EMP_03 | Dimension: Validity | Entity: Employee
SELECT COUNT(*) AS invalid_scd_window
FROM nf.nf_employees_scd
WHERE end_dt < start_dt;

-- TC_EMP_04 | Dimension: Validity | Entity: Employee
SELECT COUNT(*) AS negative_salary
FROM nf.nf_employees_scd
WHERE employee_salary < 0;

-- TC_EMP_05 | Dimension: Consistency | Entity: Employee
SELECT
    (SELECT COUNT(*) FROM nf.nf_employees_scd WHERE is_active = TRUE) AS nf_active,
    (SELECT COUNT(*) FROM dim.dim_employees_scd WHERE is_active = TRUE) AS dim_active;

/* ===============================
   TRANSACTION (5 tests)
   =============================== */

-- TC_TRX_01 | Dimension: Completeness | Entity: Transaction
SELECT COUNT(*) AS missing_transaction_id
FROM stg.mapping_transactions
WHERE transaction_id IS NULL OR TRIM(transaction_id) = '';

-- TC_TRX_02 | Dimension: Uniqueness | Entity: Transaction
SELECT transaction_id, COUNT(*) AS dup_count
FROM stg.mapping_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- TC_TRX_03 | Dimension: Validity | Entity: Transaction
SELECT COUNT(*) AS invalid_week_of_year
FROM stg.mapping_transactions
WHERE week_of_year IS NOT NULL
  AND (week_of_year < 1 OR week_of_year > 53);

-- TC_TRX_04 | Dimension: Accuracy | Entity: Transaction
SELECT COUNT(*) AS negative_amount_rows
FROM nf.nf_transactions
WHERE total_sales < 0 OR unit_price < 0 OR discount_applied < 0;

-- TC_TRX_05 | Dimension: Consistency | Entity: Transaction
SELECT
    (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_transaction_rows,
    (SELECT COUNT(*) FROM dim.fct_transactions_dd_dd) AS fact_transaction_rows;
