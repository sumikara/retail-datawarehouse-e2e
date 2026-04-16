/*
04_dq_test_levels_master_suite.sql
Purpose:
- DWH test levels (Smoke, Component/Unit, System, Integration, UAT-ready query set)
- Layer-by-layer and table-by-table DQ cases
- Includes source vs external/landing matching, row-count reconciliation,
  duplication checks, referential integrity, and accuracy checks.

Execution:
- Run test cases one by one.
- For each case, compare query output with "Expected Result".
*/

/* =====================================================================
   TEST LEVEL 1: SMOKE TESTS (CRITICAL - HIGHEST)
   Smoke = existence + operability checks.
   ===================================================================== */

-- =====================================================================
-- Test Case ID: SMK_001
-- Test Case Name: information_schema.schemata.schema_name - missing schema
-- Description: Verify all required schemas exist.
-- Expected Result: 5 rows (sl_online_retail, sl_offline_retail, stg, nf, dim).
-- Actual Result:
-- Steps: 1) Run query 2) Validate all required schema names returned.
-- Related DQ Dimensions: Structure, Completeness
-- Critical Level: CRITICAL
-- =====================================================================
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('sl_online_retail','sl_offline_retail','stg','nf','dim')
ORDER BY schema_name;

-- =====================================================================
-- Test Case ID: SMK_002
-- Test Case Name: information_schema.tables.table_name - missing core tables
-- Description: Verify core tables across layers exist.
-- Expected Result: All listed tables must exist.
-- Actual Result:
-- Steps: 1) Run query 2) Compare returned list with expected table list.
-- Related DQ Dimensions: Structure
-- Critical Level: CRITICAL
-- =====================================================================
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
    ('sl_online_retail','frg_online_retail'),
    ('sl_offline_retail','frg_offline_retail'),
    ('sl_online_retail','src_online_retail_raw'),
    ('sl_offline_retail','src_offline_retail_raw'),
    ('sl_online_retail','src_online_retail'),
    ('sl_offline_retail','src_offline_retail'),
    ('stg','mapping_transactions'),
    ('nf','nf_transactions'),
    ('dim','fct_transactions_dd_dd')
)
ORDER BY table_schema, table_name;

-- =====================================================================
-- Test Case ID: SMK_003
-- Test Case Name: information_schema.columns.column_name - mandatory column missing
-- Description: Verify mandatory transaction_id exists in source/landing/mapping/nf/fact path.
-- Expected Result: 5 rows returned for each table in path.
-- Actual Result:
-- Steps: 1) Run query 2) Confirm all layer tables in path are listed.
-- Related DQ Dimensions: Structure, Business Relevance
-- Critical Level: CRITICAL
-- =====================================================================
SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE column_name IN ('transaction_id','transaction_src_id')
  AND (table_schema, table_name) IN (
    ('sl_online_retail','src_online_retail'),
    ('sl_offline_retail','src_offline_retail'),
    ('stg','mapping_transactions'),
    ('nf','nf_transactions'),
    ('dim','fct_transactions_dd_dd')
)
ORDER BY table_schema, table_name;

/* =====================================================================
   TEST LEVEL 2: COMPONENT / UNIT TESTS (TABLE-BY-TABLE)
   ===================================================================== */

/* ---------------- SOURCE / EXTERNAL ---------------- */

-- =====================================================================
-- Test Case ID: UNT_SRC_001
-- Test Case Name: sl_online_retail.frg_online_retail.transaction_id - null value
-- Description: Check mandatory transaction_id completeness in online external source.
-- Expected Result: null_transaction_id_count = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate returned count is zero.
-- Related DQ Dimensions: Completeness, Accuracy
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS null_transaction_id_count
FROM sl_online_retail.frg_online_retail
WHERE transaction_id IS NULL OR TRIM(transaction_id) = '';

-- =====================================================================
-- Test Case ID: UNT_SRC_002
-- Test Case Name: sl_offline_retail.frg_offline_retail.transaction_id - null value
-- Description: Check mandatory transaction_id completeness in offline external source.
-- Expected Result: null_transaction_id_count = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate returned count is zero.
-- Related DQ Dimensions: Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS null_transaction_id_count
FROM sl_offline_retail.frg_offline_retail
WHERE transaction_id IS NULL OR TRIM(transaction_id) = '';

/* ---------------- LANDING RAW + CLEAN ---------------- */

-- =====================================================================
-- Test Case ID: UNT_LND_001
-- Test Case Name: sl_online_retail.frg_online_retail - sl_online_retail.src_online_retail_raw row-count mismatch
-- Description: External vs raw online row-count reconciliation.
-- Expected Result: row_count_delta = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Verify delta equals zero.
-- Related DQ Dimensions: Completeness, Timeliness
-- Critical Level: HIGH
-- =====================================================================
SELECT
  (SELECT COUNT(*) FROM sl_online_retail.frg_online_retail) AS external_rows,
  (SELECT COUNT(*) FROM sl_online_retail.src_online_retail_raw) AS raw_rows,
  (SELECT COUNT(*) FROM sl_online_retail.frg_online_retail)
    - (SELECT COUNT(*) FROM sl_online_retail.src_online_retail_raw) AS row_count_delta;

-- =====================================================================
-- Test Case ID: UNT_LND_002
-- Test Case Name: sl_offline_retail.frg_offline_retail - sl_offline_retail.src_offline_retail_raw row-count mismatch
-- Description: External vs raw offline row-count reconciliation.
-- Expected Result: row_count_delta = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Verify delta equals zero.
-- Related DQ Dimensions: Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT
  (SELECT COUNT(*) FROM sl_offline_retail.frg_offline_retail) AS external_rows,
  (SELECT COUNT(*) FROM sl_offline_retail.src_offline_retail_raw) AS raw_rows,
  (SELECT COUNT(*) FROM sl_offline_retail.frg_offline_retail)
    - (SELECT COUNT(*) FROM sl_offline_retail.src_offline_retail_raw) AS row_count_delta;

-- =====================================================================
-- Test Case ID: UNT_LND_003
-- Test Case Name: sl_online_retail.src_online_retail.transaction_id - duplicate transaction
-- Description: Detect duplicates in cleaned online staging by transaction_id.
-- Expected Result: 0 rows returned.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicate groups are returned.
-- Related DQ Dimensions: Structure, Business Relevance
-- Critical Level: HIGH
-- =====================================================================
SELECT transaction_id, COUNT(*) AS dup_count
FROM sl_online_retail.src_online_retail
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_LND_004
-- Test Case Name: sl_offline_retail.src_offline_retail.transaction_id - duplicate transaction
-- Description: Detect duplicates in cleaned offline staging by transaction_id.
-- Expected Result: 0 rows returned.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicate groups are returned.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT transaction_id, COUNT(*) AS dup_count
FROM sl_offline_retail.src_offline_retail
GROUP BY transaction_id
HAVING COUNT(*) > 1;

/* ---------------- MAPPING (TABLE-BY-TABLE) ---------------- */

-- =====================================================================
-- Test Case ID: UNT_MAP_001
-- Test Case Name: stg.mapping_customers.customer_src_id - duplicate key
-- Description: Verify mapping customer source key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure, Business Relevance
-- Critical Level: HIGH
-- =====================================================================
SELECT customer_src_id, COUNT(*) AS dup_count
FROM stg.mapping_customers
GROUP BY customer_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_MAP_002
-- Test Case Name: stg.mapping_stores.store_src_id - null key
-- Description: Verify store source ID is populated.
-- Expected Result: missing_store_src_id = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS missing_store_src_id
FROM stg.mapping_stores
WHERE store_src_id IS NULL OR TRIM(store_src_id) = '';

-- =====================================================================
-- Test Case ID: UNT_MAP_003
-- Test Case Name: stg.mapping_products.product_src_id - duplicate key
-- Description: Verify product mapping key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicate product_src_id.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT product_src_id, COUNT(*) AS dup_count
FROM stg.mapping_products
GROUP BY product_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_MAP_004
-- Test Case Name: stg.mapping_promotions.promotion_start_dt/promotion_end_dt - invalid timeline
-- Description: Validate promotion end >= start.
-- Expected Result: invalid_promotion_window = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Timeliness, Accuracy
-- Critical Level: MEDIUM
-- =====================================================================
SELECT COUNT(*) AS invalid_promotion_window
FROM stg.mapping_promotions
WHERE promotion_end_dt < promotion_start_dt;

-- =====================================================================
-- Test Case ID: UNT_MAP_005
-- Test Case Name: stg.mapping_deliveries.delivery_src_id - duplicate key
-- Description: Verify delivery mapping key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT delivery_src_id, COUNT(*) AS dup_count
FROM stg.mapping_deliveries
GROUP BY delivery_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_MAP_006
-- Test Case Name: stg.mapping_engagements.engagement_id_nk - duplicate key
-- Description: Verify engagement natural key uniqueness in mapping.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure, Accuracy
-- Critical Level: MEDIUM
-- =====================================================================
SELECT engagement_id_nk, COUNT(*) AS dup_count
FROM stg.mapping_engagements
GROUP BY engagement_id_nk
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_MAP_007
-- Test Case Name: stg.mapping_employees.employee_src_id - null key
-- Description: Verify employee source key is present.
-- Expected Result: missing_employee_src_id = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS missing_employee_src_id
FROM stg.mapping_employees
WHERE employee_src_id IS NULL OR TRIM(employee_src_id) = '';

-- =====================================================================
-- Test Case ID: UNT_MAP_008
-- Test Case Name: stg.mapping_transactions.row_sig - duplicate signature
-- Description: Verify transaction row signature uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure, Accuracy
-- Critical Level: CRITICAL
-- =====================================================================
SELECT row_sig, COUNT(*) AS dup_count
FROM stg.mapping_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

/* ---------------- NF (TABLE-BY-TABLE) ---------------- */

-- =====================================================================
-- Test Case ID: UNT_NF_001
-- Test Case Name: nf.nf_states.state_src_id - duplicate source key
-- Description: Verify state source key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT state_src_id, COUNT(*) AS dup_count
FROM nf.nf_states
GROUP BY state_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_NF_002
-- Test Case Name: nf.nf_cities.state_id - orphan FK
-- Description: Verify cities reference valid state.
-- Expected Result: orphan_city_state_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_city_state_fk
FROM nf.nf_cities c
LEFT JOIN nf.nf_states s ON s.state_id = c.state_id
WHERE s.state_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_003
-- Test Case Name: nf.nf_addresses.city_id - orphan FK
-- Description: Verify addresses reference valid city.
-- Expected Result: orphan_address_city_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure, Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_address_city_fk
FROM nf.nf_addresses a
LEFT JOIN nf.nf_cities c ON c.city_id = a.city_id
WHERE c.city_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_004
-- Test Case Name: nf.nf_product_categories.product_category_src_id - duplicate key
-- Description: Verify product category source key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: MEDIUM
-- =====================================================================
SELECT product_category_src_id, COUNT(*) AS dup_count
FROM nf.nf_product_categories
GROUP BY product_category_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_NF_005
-- Test Case Name: nf.nf_promotion_types.promotion_type_src_id - duplicate key
-- Description: Verify promotion type source key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: MEDIUM
-- =====================================================================
SELECT promotion_type_src_id, COUNT(*) AS dup_count
FROM nf.nf_promotion_types
GROUP BY promotion_type_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_NF_006
-- Test Case Name: nf.nf_shipping_partners.shipping_partner_src_id - duplicate key
-- Description: Verify shipping partner source key uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Validate no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: MEDIUM
-- =====================================================================
SELECT shipping_partner_src_id, COUNT(*) AS dup_count
FROM nf.nf_shipping_partners
GROUP BY shipping_partner_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_NF_007
-- Test Case Name: nf.nf_customers.address_id - orphan FK
-- Description: Verify customer-address integrity.
-- Expected Result: orphan_customer_address_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure, Accuracy
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_customer_address_fk
FROM nf.nf_customers c
LEFT JOIN nf.nf_addresses a ON a.address_id = c.address_id
WHERE a.address_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_008
-- Test Case Name: nf.nf_stores.address_id - orphan FK
-- Description: Verify store-address integrity.
-- Expected Result: orphan_store_address_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_store_address_fk
FROM nf.nf_stores s
LEFT JOIN nf.nf_addresses a ON a.address_id = s.address_id
WHERE a.address_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_009
-- Test Case Name: nf.nf_products.product_category_id - orphan FK
-- Description: Verify product-category integrity.
-- Expected Result: orphan_product_category_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_product_category_fk
FROM nf.nf_products p
LEFT JOIN nf.nf_product_categories c ON c.product_category_id = p.product_category_id
WHERE c.product_category_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_010
-- Test Case Name: nf.nf_promotions.promotion_type_id - orphan FK
-- Description: Verify promotion-type integrity.
-- Expected Result: orphan_promotion_type_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure, Business Relevance
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_promotion_type_fk
FROM nf.nf_promotions p
LEFT JOIN nf.nf_promotion_types t ON t.promotion_type_id = p.promotion_type_id
WHERE t.promotion_type_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_011
-- Test Case Name: nf.nf_deliveries.shipping_partner_id - orphan FK
-- Description: Verify delivery-shipping partner integrity.
-- Expected Result: orphan_delivery_partner_fk = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_delivery_partner_fk
FROM nf.nf_deliveries d
LEFT JOIN nf.nf_shipping_partners sp ON sp.shipping_partner_id = d.shipping_partner_id
WHERE sp.shipping_partner_id IS NULL;

-- =====================================================================
-- Test Case ID: UNT_NF_012
-- Test Case Name: nf.nf_engagements.website_visits - negative value
-- Description: Validate engagement visits are non-negative.
-- Expected Result: negative_website_visits = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Accuracy
-- Critical Level: MEDIUM
-- =====================================================================
SELECT COUNT(*) AS negative_website_visits
FROM nf.nf_engagements
WHERE website_visits < 0;

-- =====================================================================
-- Test Case ID: UNT_NF_013
-- Test Case Name: nf.nf_employees_scd.employee_src_id - multiple active versions
-- Description: Validate SCD active-version rule.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) No employee should have >1 active row.
-- Related DQ Dimensions: Timeliness, Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT employee_src_id, COUNT(*) AS active_versions
FROM nf.nf_employees_scd
WHERE is_active = TRUE
GROUP BY employee_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_NF_014
-- Test Case Name: nf.nf_transactions.row_sig - duplicate signature
-- Description: Validate normalized transaction deduplication by row signature.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicate signatures.
-- Related DQ Dimensions: Accuracy, Structure
-- Critical Level: CRITICAL
-- =====================================================================
SELECT row_sig, COUNT(*) AS dup_count
FROM nf.nf_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

/* ---------------- DIM + FACT (TABLE-BY-TABLE) ---------------- */

-- =====================================================================
-- Test Case ID: UNT_DIM_001
-- Test Case Name: dim.dim_customers.customer_src_id - duplicate key
-- Description: Validate unique customer source ID in dimension.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT customer_src_id, COUNT(*) AS dup_count
FROM dim.dim_customers
GROUP BY customer_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_DIM_002
-- Test Case Name: dim.dim_stores.store_src_id - duplicate key
-- Description: Validate unique store source ID in dimension.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT store_src_id, COUNT(*) AS dup_count
FROM dim.dim_stores
GROUP BY store_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_DIM_003
-- Test Case Name: dim.dim_products.product_src_id - duplicate key
-- Description: Validate unique product source ID in dimension.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicates.
-- Related DQ Dimensions: Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT product_src_id, COUNT(*) AS dup_count
FROM dim.dim_products
GROUP BY product_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_DIM_004
-- Test Case Name: dim.dim_promotions.promotion_start_dt/promotion_end_dt - invalid timeline
-- Description: Validate promotion date ordering in dimension.
-- Expected Result: invalid_promo_window = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Timeliness
-- Critical Level: MEDIUM
-- =====================================================================
SELECT COUNT(*) AS invalid_promo_window
FROM dim.dim_promotions
WHERE promotion_end_dt < promotion_start_dt;

-- =====================================================================
-- Test Case ID: UNT_DIM_005
-- Test Case Name: dim.dim_deliveries.delivery_status - null status
-- Description: Validate delivery status completeness.
-- Expected Result: missing_delivery_status = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Completeness
-- Critical Level: MEDIUM
-- =====================================================================
SELECT COUNT(*) AS missing_delivery_status
FROM dim.dim_deliveries
WHERE delivery_status IS NULL OR TRIM(delivery_status) = '';

-- =====================================================================
-- Test Case ID: UNT_DIM_006
-- Test Case Name: dim.dim_engagements.website_visits - negative value
-- Description: Validate dimensional engagement measures non-negative.
-- Expected Result: negative_website_visits = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate count is zero.
-- Related DQ Dimensions: Accuracy
-- Critical Level: MEDIUM
-- =====================================================================
SELECT COUNT(*) AS negative_website_visits
FROM dim.dim_engagements
WHERE website_visits < 0;

-- =====================================================================
-- Test Case ID: UNT_DIM_007
-- Test Case Name: dim.dim_employees_scd.employee_src_id - multiple active versions
-- Description: Validate SCD active-version uniqueness in dimension.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no employee has multiple active rows.
-- Related DQ Dimensions: Timeliness, Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT employee_src_id, COUNT(*) AS active_versions
FROM dim.dim_employees_scd
WHERE is_active = TRUE
GROUP BY employee_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_DIM_008
-- Test Case Name: dim.dim_dates.full_date - duplicate date
-- Description: Validate date dimension uniqueness.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicate full_date.
-- Related DQ Dimensions: Structure, Timeliness
-- Critical Level: HIGH
-- =====================================================================
SELECT full_date, COUNT(*) AS dup_count
FROM dim.dim_dates
GROUP BY full_date
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_FCT_001
-- Test Case Name: dim.fct_transactions_dd_dd.transaction_src_id - duplicate business key per day
-- Description: Validate no unexpected duplicates in fact grain.
-- Expected Result: 0 rows.
-- Actual Result:
-- Steps: 1) Run query 2) Ensure no duplicated grain combinations.
-- Related DQ Dimensions: Structure, Business Relevance
-- Critical Level: CRITICAL
-- =====================================================================
SELECT transaction_date, transaction_src_id, COUNT(*) AS dup_count
FROM dim.fct_transactions_dd_dd
GROUP BY transaction_date, transaction_src_id
HAVING COUNT(*) > 1;

-- =====================================================================
-- Test Case ID: UNT_FCT_002
-- Test Case Name: dim.fct_transactions_dd_dd.total_sales - formula mismatch
-- Description: Validate total_sales = quantity * unit_price - discount_applied (tolerance 0.01).
-- Expected Result: formula_mismatch_count = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate mismatch count is zero.
-- Related DQ Dimensions: Accuracy
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS formula_mismatch_count
FROM dim.fct_transactions_dd_dd
WHERE ABS(total_sales - ((quantity * unit_price) - discount_applied)) > 0.01;

/* =====================================================================
   TEST LEVEL 3: SYSTEM TESTS (END-TO-END IN APP BOUNDARY)
   ===================================================================== */

-- =====================================================================
-- Test Case ID: SYS_001
-- Test Case Name: Source->Landing->Mapping->NF->Fact row count reconciliation - transaction path
-- Description: Reconcile row counts across the full transaction pipeline.
-- Expected Result: Counts aligned based on design rules (or explained delta only).
-- Actual Result:
-- Steps: 1) Run query 2) Investigate unexplained deltas.
-- Related DQ Dimensions: Completeness, Business Relevance, Timeliness
-- Critical Level: CRITICAL
-- =====================================================================
SELECT
  (SELECT COUNT(*) FROM sl_online_retail.src_online_retail)
  + (SELECT COUNT(*) FROM sl_offline_retail.src_offline_retail) AS landing_clean_rows,
  (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows,
  (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows,
  (SELECT COUNT(*) FROM dim.fct_transactions_dd_dd) AS fact_rows;

-- =====================================================================
-- Test Case ID: SYS_002
-- Test Case Name: stg.mapping_transactions.transaction_id - truncation risk
-- Description: Validate transaction_id length does not exceed target NF length limit.
-- Expected Result: oversize_transaction_id_count = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate zero.
-- Related DQ Dimensions: Accuracy, Structure
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS oversize_transaction_id_count
FROM stg.mapping_transactions
WHERE LENGTH(transaction_id) > 100;

-- =====================================================================
-- Test Case ID: SYS_003
-- Test Case Name: nf.nf_transactions.<all FK columns> - orphan records
-- Description: Validate no orphan transaction references across parent tables.
-- Expected Result: all orphan counters = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate every orphan counter is zero.
-- Related DQ Dimensions: Structure, Completeness
-- Critical Level: CRITICAL
-- =====================================================================
SELECT
  SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_customer,
  SUM(CASE WHEN s.store_id IS NULL THEN 1 ELSE 0 END) AS orphan_store,
  SUM(CASE WHEN p.product_id IS NULL THEN 1 ELSE 0 END) AS orphan_product,
  SUM(CASE WHEN pr.promotion_id IS NULL THEN 1 ELSE 0 END) AS orphan_promotion,
  SUM(CASE WHEN d.delivery_id IS NULL THEN 1 ELSE 0 END) AS orphan_delivery,
  SUM(CASE WHEN e.engagement_id IS NULL THEN 1 ELSE 0 END) AS orphan_engagement,
  SUM(CASE WHEN ci.city_id IS NULL THEN 1 ELSE 0 END) AS orphan_city
FROM nf.nf_transactions t
LEFT JOIN nf.nf_customers c ON c.customer_id = t.customer_id
LEFT JOIN nf.nf_stores s ON s.store_id = t.store_id
LEFT JOIN nf.nf_products p ON p.product_id = t.product_id
LEFT JOIN nf.nf_promotions pr ON pr.promotion_id = t.promotion_id
LEFT JOIN nf.nf_deliveries d ON d.delivery_id = t.delivery_id
LEFT JOIN nf.nf_engagements e ON e.engagement_id = t.engagement_id
LEFT JOIN nf.nf_cities ci ON ci.city_id = t.city_id;

/* =====================================================================
   TEST LEVEL 4: INTEGRATION TESTS (UPSTREAM + DOWNSTREAM TOUCHPOINTS)
   ===================================================================== */

-- =====================================================================
-- Test Case ID: INT_001
-- Test Case Name: sl_online_retail.src_online_retail.transaction_id -> stg.mapping_transactions.transaction_id - missing pass-through
-- Description: Verify online source transaction IDs are represented in mapping.
-- Expected Result: missing_online_in_mapping = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Investigate any missing IDs.
-- Related DQ Dimensions: Completeness, Business Relevance
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS missing_online_in_mapping
FROM sl_online_retail.src_online_retail o
LEFT JOIN stg.mapping_transactions m
  ON m.transaction_id = o.transaction_id
WHERE m.transaction_id IS NULL;

-- =====================================================================
-- Test Case ID: INT_002
-- Test Case Name: sl_offline_retail.src_offline_retail.transaction_id -> stg.mapping_transactions.transaction_id - missing pass-through
-- Description: Verify offline source transaction IDs are represented in mapping.
-- Expected Result: missing_offline_in_mapping = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Investigate any missing IDs.
-- Related DQ Dimensions: Completeness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS missing_offline_in_mapping
FROM sl_offline_retail.src_offline_retail o
LEFT JOIN stg.mapping_transactions m
  ON m.transaction_id = o.transaction_id
WHERE m.transaction_id IS NULL;

-- =====================================================================
-- Test Case ID: INT_003
-- Test Case Name: nf.nf_transactions.transaction_id -> dim.fct_transactions_dd_dd.transaction_src_id - missing downstream record
-- Description: Verify normalized transactions are represented in fact.
-- Expected Result: missing_nf_in_fact = 0 (or only accepted design exceptions).
-- Actual Result:
-- Steps: 1) Run query 2) Analyze any missing.
-- Related DQ Dimensions: Completeness, Timeliness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS missing_nf_in_fact
FROM nf.nf_transactions n
LEFT JOIN dim.fct_transactions_dd_dd f
  ON f.transaction_src_id = n.transaction_id
WHERE f.transaction_src_id IS NULL;

-- =====================================================================
-- Test Case ID: INT_004
-- Test Case Name: dim.fct_transactions_dd_dd.transaction_date_sk -> dim.dim_dates.date_surr_id - orphan date key
-- Description: Verify fact-date integrity for downstream analytics.
-- Expected Result: orphan_fact_date_key = 0.
-- Actual Result:
-- Steps: 1) Run query 2) Validate zero.
-- Related DQ Dimensions: Structure, Timeliness
-- Critical Level: HIGH
-- =====================================================================
SELECT COUNT(*) AS orphan_fact_date_key
FROM dim.fct_transactions_dd_dd f
LEFT JOIN dim.dim_dates d ON d.date_surr_id = f.transaction_date_sk
WHERE d.date_surr_id IS NULL;

/* =====================================================================
   TEST LEVEL 5: UAT-READY CHECKS (BUSINESS/ANALYTIC VALIDATION)
   ===================================================================== */

-- =====================================================================
-- Test Case ID: UAT_001
-- Test Case Name: dim.fct_transactions_dd_dd.total_sales - source reconciliation delta
-- Description: Compare aggregated sales between landing clean source and fact.
-- Expected Result: sales_delta ~= 0 (within agreed tolerance).
-- Actual Result:
-- Steps: 1) Run query 2) Validate delta against business-approved tolerance.
-- Related DQ Dimensions: Accuracy, Business Relevance
-- Critical Level: HIGH
-- =====================================================================
SELECT
  COALESCE((
      SELECT SUM(total_sales) FROM (
          SELECT total_sales FROM sl_online_retail.src_online_retail
          UNION ALL
          SELECT total_sales FROM sl_offline_retail.src_offline_retail
      ) s
  ),0) AS src_total_sales,
  COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd),0) AS fact_total_sales,
  COALESCE((
      SELECT SUM(total_sales) FROM (
          SELECT total_sales FROM sl_online_retail.src_online_retail
          UNION ALL
          SELECT total_sales FROM sl_offline_retail.src_offline_retail
      ) s
  ),0) - COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd),0) AS sales_delta;

-- =====================================================================
-- Test Case ID: UAT_002
-- Test Case Name: dim.fct_transactions_dd_dd.transaction_date - freshness lag
-- Description: Validate latest fact transaction date is recent enough.
-- Expected Result: freshness_lag_days within SLA (e.g., <= 1 day for daily load).
-- Actual Result:
-- Steps: 1) Run query 2) Compare lag against SLA.
-- Related DQ Dimensions: Timeliness
-- Critical Level: HIGH
-- =====================================================================
SELECT
  CURRENT_DATE - MAX(transaction_date) AS freshness_lag_days,
  MAX(transaction_date) AS max_fact_transaction_date
FROM dim.fct_transactions_dd_dd;

-- =====================================================================
-- Test Case ID: UAT_003
-- Test Case Name: dim.<all dimensions>.<surrogate keys> - unknown/default row usage
-- Description: Track unknown surrogate usage in fact for business acceptance.
-- Expected Result: Unknown usage near zero or within approved threshold.
-- Actual Result:
-- Steps: 1) Run query 2) Compare counts with acceptance threshold.
-- Related DQ Dimensions: Accuracy, Completeness, Business Relevance
-- Critical Level: MEDIUM
-- =====================================================================
SELECT
  SUM(CASE WHEN product_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_product_rows,
  SUM(CASE WHEN promotion_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_promotion_rows,
  SUM(CASE WHEN delivery_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_delivery_rows,
  SUM(CASE WHEN engagement_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_engagement_rows,
  SUM(CASE WHEN store_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_store_rows,
  SUM(CASE WHEN customer_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_customer_rows,
  SUM(CASE WHEN employee_surr_id = -1 THEN 1 ELSE 0 END) AS unknown_employee_rows
FROM dim.fct_transactions_dd_dd;
