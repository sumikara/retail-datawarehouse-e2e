/*
Layer Test Cases (Control Queries)
Each query includes:
- Test Case ID
- Dimension (DQ dimension)
- Layer focus
- English explanation
*/

/* ===============================
   STAGING LAYER (stg + sl_*)
   =============================== */

-- TC_STG_01 | Dimension: Completeness | Layer: Staging
-- Validate mandatory transaction IDs are present in online source landing.
SELECT COUNT(*) AS null_transaction_id_count
FROM sl_online_retail.src_online_retail
WHERE transaction_id IS NULL OR TRIM(transaction_id) = '';

-- TC_STG_02 | Dimension: Completeness | Layer: Staging
-- Validate mandatory transaction IDs are present in offline source landing.
SELECT COUNT(*) AS null_transaction_id_count
FROM sl_offline_retail.src_offline_retail
WHERE transaction_id IS NULL OR TRIM(transaction_id) = '';

-- TC_STG_03 | Dimension: Validity | Layer: Staging
-- Detect negative quantity values before mapping.
SELECT COUNT(*) AS negative_quantity_count
FROM (
    SELECT quantity FROM sl_online_retail.src_online_retail
    UNION ALL
    SELECT quantity FROM sl_offline_retail.src_offline_retail
) s
WHERE quantity < 0;

-- TC_STG_04 | Dimension: Validity | Layer: Staging
-- Detect impossible transaction dates (future dates > now + 1 day tolerance).
SELECT COUNT(*) AS suspicious_future_dates
FROM (
    SELECT transaction_dt FROM sl_online_retail.src_online_retail
    UNION ALL
    SELECT transaction_dt FROM sl_offline_retail.src_offline_retail
) s
WHERE transaction_dt > (CURRENT_TIMESTAMP + INTERVAL '1 day');

-- TC_STG_05 | Dimension: Consistency | Layer: Staging
-- Check source channel coverage in ingestion metadata.
SELECT source_system, COUNT(*) AS file_count
FROM stg.etl_file_registry
GROUP BY source_system
ORDER BY source_system;

/* ===============================
   MAPPING LAYER (stg.mapping_*)
   =============================== */

-- TC_MAP_01 | Dimension: Uniqueness | Layer: Mapping
-- Ensure transaction row signature uniqueness (should be zero duplicates).
SELECT row_sig, COUNT(*) AS dup_count
FROM stg.mapping_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

-- TC_MAP_02 | Dimension: Completeness | Layer: Mapping
-- Ensure customer source identity is populated.
SELECT COUNT(*) AS missing_customer_src_id
FROM stg.mapping_transactions
WHERE customer_src_id IS NULL OR TRIM(customer_src_id) = '';

-- TC_MAP_03 | Dimension: Completeness | Layer: Mapping
-- Ensure product source identity is populated.
SELECT COUNT(*) AS missing_product_src_id
FROM stg.mapping_transactions
WHERE product_src_id IS NULL OR TRIM(product_src_id) = '';

-- TC_MAP_04 | Dimension: Validity | Layer: Mapping
-- Check that mapping dates are not null for transaction event.
SELECT COUNT(*) AS missing_transaction_dt
FROM stg.mapping_transactions
WHERE transaction_dt IS NULL;

-- TC_MAP_05 | Dimension: Consistency | Layer: Mapping
-- Compare transaction counts between source and mapping.
SELECT
    (SELECT COUNT(*) FROM (
        SELECT transaction_id FROM sl_online_retail.src_online_retail
        UNION ALL
        SELECT transaction_id FROM sl_offline_retail.src_offline_retail
    ) src) AS source_rows,
    (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows;

/* ===============================
   NORMALIZED LAYER (nf.*)
   =============================== */

-- TC_NF_01 | Dimension: Integrity | Layer: Normalized
-- Detect transactions with unresolved customer FK.
SELECT COUNT(*) AS orphan_customer_fk
FROM nf.nf_transactions t
LEFT JOIN nf.nf_customers c ON c.customer_id = t.customer_id
WHERE c.customer_id IS NULL;

-- TC_NF_02 | Dimension: Integrity | Layer: Normalized
-- Detect transactions with unresolved store FK.
SELECT COUNT(*) AS orphan_store_fk
FROM nf.nf_transactions t
LEFT JOIN nf.nf_stores s ON s.store_id = t.store_id
WHERE s.store_id IS NULL;

-- TC_NF_03 | Dimension: Uniqueness | Layer: Normalized
-- Ensure transaction row signature remains unique in normalized fact-like table.
SELECT row_sig, COUNT(*) AS dup_count
FROM nf.nf_transactions
GROUP BY row_sig
HAVING COUNT(*) > 1;

-- TC_NF_04 | Dimension: Validity | Layer: Normalized
-- Ensure total_sales is not negative.
SELECT COUNT(*) AS negative_total_sales
FROM nf.nf_transactions
WHERE total_sales < 0;

-- TC_NF_05 | Dimension: Consistency | Layer: Normalized
-- Compare mapping vs normalized transaction counts.
SELECT
    (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows,
    (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows;

/* ===============================
   DIMENSIONAL LAYER (dim dimensions)
   =============================== */

-- TC_DIM_01 | Dimension: Uniqueness | Layer: Dimensional
-- Ensure one row per customer source id in dimension.
SELECT customer_src_id, COUNT(*) AS dup_count
FROM dim.dim_customers
GROUP BY customer_src_id
HAVING COUNT(*) > 1;

-- TC_DIM_02 | Dimension: Uniqueness | Layer: Dimensional
-- Ensure one row per store source id in dimension.
SELECT store_src_id, COUNT(*) AS dup_count
FROM dim.dim_stores
GROUP BY store_src_id
HAVING COUNT(*) > 1;

-- TC_DIM_03 | Dimension: Uniqueness | Layer: Dimensional
-- Ensure one row per product source id in dimension.
SELECT product_src_id, COUNT(*) AS dup_count
FROM dim.dim_products
GROUP BY product_src_id
HAVING COUNT(*) > 1;

-- TC_DIM_04 | Dimension: Consistency | Layer: Dimensional
-- Ensure date dimension contains unique calendar dates.
SELECT full_date, COUNT(*) AS dup_count
FROM dim.dim_dates
GROUP BY full_date
HAVING COUNT(*) > 1;

-- TC_DIM_05 | Dimension: Timeliness | Layer: Dimensional
-- Verify max loaded date is recent enough for operational freshness.
SELECT MAX(full_date) AS max_dim_date
FROM dim.dim_dates;

/* ===============================
   FACT LAYER (dim.fct_transactions_dd_dd)
   =============================== */

-- TC_FCT_01 | Dimension: Completeness | Layer: Fact
-- Ensure required foreign keys are populated with valid surrogate values.
SELECT COUNT(*) AS invalid_fk_rows
FROM dim.fct_transactions_dd_dd
WHERE transaction_date_sk IS NULL
   OR customer_surr_id IS NULL
   OR store_surr_id IS NULL
   OR product_surr_id IS NULL;

-- TC_FCT_02 | Dimension: Validity | Layer: Fact
-- Detect non-sensical numeric values.
SELECT COUNT(*) AS invalid_numeric_rows
FROM dim.fct_transactions_dd_dd
WHERE quantity < 0
   OR unit_price < 0
   OR total_sales < 0
   OR discount_applied < 0;

-- TC_FCT_03 | Dimension: Consistency | Layer: Fact
-- Compare normalized transaction count to dimensional fact count.
SELECT
    (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows,
    (SELECT COUNT(*) FROM dim.fct_transactions_dd_dd) AS fact_rows;

-- TC_FCT_04 | Dimension: Accuracy | Layer: Fact
-- Check total_sales approximately matches quantity * unit_price - discount (tolerance 0.01).
SELECT COUNT(*) AS amount_formula_mismatch
FROM dim.fct_transactions_dd_dd
WHERE ABS(total_sales - ((quantity * unit_price) - discount_applied)) > 0.01;

-- TC_FCT_05 | Dimension: Uniqueness | Layer: Fact
-- Check for duplicate business transaction identifiers in the same transaction date.
SELECT transaction_date, transaction_src_id, COUNT(*) AS dup_count
FROM dim.fct_transactions_dd_dd
GROUP BY transaction_date, transaction_src_id
HAVING COUNT(*) > 1;
