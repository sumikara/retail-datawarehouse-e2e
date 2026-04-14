/*
03_system_test_suite.sql

Goal:
- Single-file test suite for Smoke + Unit-like + Regression checks.
- Priority: Smoke tests to prove system is operational and warehouse objects are valid.

Execution note:
- Run section by section and review result sets.
- Expected behavior is documented before each query.
*/

/* =====================================================================
   A) SMOKE TESTS (PRIORITY)
   ===================================================================== */

-- SMK_01 | Smoke | Expect: all required schemas exist.
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('sl_online_retail','sl_offline_retail','stg','nf','dim')
ORDER BY schema_name;

-- SMK_02 | Smoke | Expect: all core orchestration metadata tables exist in stg.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'stg'
  AND table_name IN ('etl_batch_run','etl_step_run','etl_file_registry','etl_log')
ORDER BY table_name;

-- SMK_03 | Smoke | Expect: normalized core transaction table exists.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'nf'
  AND table_name = 'nf_transactions';

-- SMK_04 | Smoke | Expect: dimensional fact table exists.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'dim'
  AND table_name = 'fct_transactions_dd_dd';

-- SMK_05 | Smoke | Expect: key columns exist in nf.nf_transactions.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'nf'
  AND table_name = 'nf_transactions'
  AND column_name IN (
    'transaction_id','transaction_dt','total_sales','quantity','unit_price','discount_applied',
    'store_id','customer_id','promotion_id','delivery_id','product_id','engagement_id','city_id','employee_id','row_sig'
  )
ORDER BY column_name;

-- SMK_06 | Smoke | Expect: key columns exist in dim.fct_transactions_dd_dd.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'dim'
  AND table_name = 'fct_transactions_dd_dd'
  AND column_name IN (
    'transaction_src_id','transaction_date','transaction_date_sk','total_sales','quantity','unit_price','discount_applied',
    'product_surr_id','promotion_surr_id','delivery_surr_id','engagement_surr_id','store_surr_id','customer_surr_id','employee_surr_id'
  )
ORDER BY column_name;

-- SMK_07 | Smoke | Expect: mapping tables exist for all core entities.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'stg'
  AND table_name IN (
    'mapping_customers','mapping_stores','mapping_products','mapping_promotions',
    'mapping_deliveries','mapping_engagements','mapping_employees','mapping_transactions'
  )
ORDER BY table_name;

-- SMK_08 | Smoke | Expect: transaction supporting indexes exist in nf schema.
SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'nf'
  AND tablename = 'nf_transactions'
ORDER BY indexname;

-- SMK_09 | Smoke | Expect: mapping transaction indexes include source-scoped variants.
SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'stg'
  AND tablename = 'mapping_transactions'
  AND indexname IN (
    'idx_mapping_transactions_store_src', 'idx_mapping_transactions_store_src_sys_tbl',
    'idx_mapping_transactions_product_src', 'idx_mapping_transactions_product_src_sys_tbl',
    'idx_mapping_transactions_promotion_src', 'idx_mapping_transactions_promotion_src_sys_tbl',
    'idx_mapping_transactions_delivery_src', 'idx_mapping_transactions_delivery_src_sys_tbl',
    'idx_mapping_transactions_employee_src', 'idx_mapping_transactions_employee_src_sys_tbl',
    'idx_mapping_transactions_city_src', 'idx_mapping_transactions_city_src_sys_tbl'
  )
ORDER BY indexname;

-- SMK_10 | Smoke | Expect: ETL log function exists and callable.
SELECT routine_schema, routine_name
FROM information_schema.routines
WHERE routine_schema = 'stg'
  AND routine_name = 'log_etl_event';

/* =====================================================================
   B) UNIT-LIKE TESTS (DATA RULE CHECKS)
   ===================================================================== */

-- UNT_01 | Unit-like | Expect: no duplicate row signatures in mapping transactions.
SELECT row_sig, COUNT(*) AS dup_count
FROM stg.mapping_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

-- UNT_02 | Unit-like | Expect: no duplicate row signatures in normalized transactions.
SELECT row_sig, COUNT(*) AS dup_count
FROM nf.nf_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

-- UNT_03 | Unit-like | Expect: no negative sales or quantity in fact.
SELECT COUNT(*) AS invalid_rows
FROM dim.fct_transactions_dd_dd
WHERE total_sales < 0 OR quantity < 0 OR unit_price < 0 OR discount_applied < 0;

-- UNT_04 | Unit-like | Expect: customer FK integrity in normalized transactions.
SELECT COUNT(*) AS orphan_customer_fk
FROM nf.nf_transactions t
LEFT JOIN nf.nf_customers c ON c.customer_id = t.customer_id
WHERE c.customer_id IS NULL;

-- UNT_05 | Unit-like | Expect: product FK integrity in normalized transactions.
SELECT COUNT(*) AS orphan_product_fk
FROM nf.nf_transactions t
LEFT JOIN nf.nf_products p ON p.product_id = t.product_id
WHERE p.product_id IS NULL;

-- UNT_06 | Unit-like | Expect: promotion temporal consistency (end >= start).
SELECT COUNT(*) AS invalid_promotion_window
FROM nf.nf_promotions
WHERE promotion_end_dt < promotion_start_dt;

-- UNT_07 | Unit-like | Expect: employee SCD has max one active row per source id.
SELECT employee_src_id, COUNT(*) AS active_rows
FROM nf.nf_employees_scd
WHERE is_active = TRUE
GROUP BY employee_src_id
HAVING COUNT(*) > 1;

-- UNT_08 | Unit-like | Expect: date dimension has no duplicates.
SELECT full_date, COUNT(*) AS dup_count
FROM dim.dim_dates
GROUP BY full_date
HAVING COUNT(*) > 1;

/* =====================================================================
   C) REGRESSION TESTS (STABILITY / DRIFT)
   ===================================================================== */

-- REG_01 | Regression | Monitor row-count drift between mapping and normalized transactions.
SELECT
    (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows,
    (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows,
    (SELECT COUNT(*) FROM dim.fct_transactions_dd_dd) AS fact_rows;

-- REG_02 | Regression | Monitor total_sales drift between normalized and fact layers.
SELECT
    COALESCE((SELECT SUM(total_sales) FROM nf.nf_transactions), 0) AS nf_total_sales,
    COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd), 0) AS fact_total_sales,
    COALESCE((SELECT SUM(total_sales) FROM nf.nf_transactions), 0)
      - COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd), 0) AS sales_delta;

-- REG_03 | Regression | Verify dimensional coverage for core entities.
SELECT
    (SELECT COUNT(*) FROM nf.nf_customers WHERE customer_id <> -1) AS nf_customers,
    (SELECT COUNT(*) FROM dim.dim_customers) AS dim_customers,
    (SELECT COUNT(*) FROM nf.nf_products WHERE product_id <> -1) AS nf_products,
    (SELECT COUNT(*) FROM dim.dim_products) AS dim_products,
    (SELECT COUNT(*) FROM nf.nf_stores WHERE store_id <> -1) AS nf_stores,
    (SELECT COUNT(*) FROM dim.dim_stores) AS dim_stores;

-- REG_04 | Regression | Verify fact dates are represented in date dimension.
SELECT COUNT(*) AS missing_date_keys
FROM dim.fct_transactions_dd_dd f
LEFT JOIN dim.dim_dates d ON d.date_surr_id = f.transaction_date_sk
WHERE d.date_surr_id IS NULL;

-- REG_05 | Regression | Detect sudden source-system imbalance in mapping layer.
SELECT source_system, COUNT(*) AS row_count
FROM stg.mapping_transactions
GROUP BY source_system
ORDER BY source_system;

/* =====================================================================
   D) OPTIONAL SANITY SUMMARY
   ===================================================================== */

-- SAN_01 | Sanity | Quick end-to-end freshness indicators.
SELECT
    (SELECT MAX(log_ts) FROM stg.etl_log) AS last_log_ts,
    (SELECT MAX(start_ts) FROM stg.etl_batch_run) AS last_batch_start,
    (SELECT MAX(transaction_dt) FROM nf.nf_transactions) AS max_nf_transaction_dt,
    (SELECT MAX(transaction_date) FROM dim.fct_transactions_dd_dd) AS max_fact_transaction_date;
