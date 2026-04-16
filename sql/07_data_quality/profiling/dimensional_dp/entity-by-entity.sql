/*Note: Since this dataset is synthetic, some semantic inconsistencies may appear during 
profiling. For example, the same transaction may seem to involve multiple employees, or 
a store may appear to have operational activity despite inconsistent employee-store 
relationships. These cases should be interpreted as dataset artifacts rather than 
real-world business behavior.*/

-- ============================================
-- PRODUCTS (from BOTH sources)
-- ============================================

CREATE TEMP TABLE src_products AS
    SELECT product_id, product_category, product_name, product_brand, product_material
    FROM sl_offline_retail.src_online_retail
    UNION ALL
    SELECT product_id, product_category, product_name, product_brand, product_material
    FROM sl_offline_retail.src_offline_retail;

SELECT COUNT(*) FROM src_products;

-- which pne makes sense_?
SELECT COUNT(DISTINCT (
    product_category || '-' || product_name || '-' || product_brand
)) AS distinct_keys FROM src_products;

SELECT COUNT(DISTINCT (
    product_category || '-' || product_name || '-' || product_brand || '-' || product_material
)) AS distinct_keys FROM src_products;

SELECT
    product_category || '-' || product_name || '-' || product_brand AS product_src_id,
    COUNT(*) AS cnt
FROM src_products
GROUP BY product_src_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    product_category || '-' || product_name || '-' || product_brand || '-' || product_material AS product_src_id_with_material,
    COUNT(*) AS cnt
FROM src_products
GROUP BY product_src_id_with_material
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    product_category,
    product_name,
    product_brand,
    product_material,
    COUNT(*) AS cnt,
    COUNT(DISTINCT product_id) AS distinct_keys
FROM src_products
GROUP BY
    product_category, product_name, product_brand, product_material
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;


-- ============================================
-- ENGAGEMENTS (ONLINE ONLY)
-- ============================================

SELECT COUNT(*) FROM  sl_online_retail.src_online_retail;

SELECT COUNT(DISTINCT engagement_id) FROM  sl_online_retail.src_online_retail;

SELECT engagement_id, COUNT(*) AS cnt
FROM  sl_online_retail.src_online_retail
GROUP BY engagement_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT COUNT(DISTINCT (
    order_channel || '-' || customer_support_method || '-' || app_usage || '-' || social_media_engagement
)) AS distinct_key
FROM  sl_online_retail.src_online_retail;

SELECT
    order_channel,
    customer_support_method,
    issue_status,
    app_usage,
    social_media_engagement,
    COUNT(*) AS cnt,
    COUNT(DISTINCT engagement_id) AS distinct_keys
FROM  sl_online_retail.src_online_retail
GROUP BY
    order_channel, customer_support_method, issue_status,
    app_usage, social_media_engagement
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- ============================================
-- STORES (OFFLINE ONLY) - one of the most important profiling to determine the hierachical geography dimension.
-- ============================================

SELECT COUNT(*) FROM sl_offline_retail.src_offline_retail;

SELECT COUNT(DISTINCT (
    store_location || '-' || store_city || '-' || store_state || '-' || store_zip_code
)) AS distinct_key_with_zip_code FROM sl_offline_retail.src_offline_retail;

SELECT
    store_location || '-' || store_city || '-' || store_state || '-' || store_zip_code AS store_src_id,
    COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY store_src_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    store_location,
    store_city,
    store_state,
    store_zip_code,
    COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY store_location, store_city, store_state, store_zip_code
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT COUNT(DISTINCT (
    store_location || '-' || store_city || '-' || store_state
)) AS distinct_key_no_zip_code FROM sl_offline_retail.src_offline_retail;

SELECT
    store_location || '-' || store_city || '-' || store_state AS store_src_id,
    COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY store_src_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    store_location,
    store_city,
    store_state,
    COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY store_location, store_city, store_state
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- ============================================
-- EMPLOYEES (OFFLINE ONLY)
-- ============================================

SELECT COUNT(*) FROM sl_offline_retail.src_offline_retail;

SELECT COUNT(DISTINCT employee_name) FROM sl_offline_retail.src_offline_retail;

SELECT employee_name, COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY employee_name
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    employee_name,
    employee_position,
    employee_salary,
    employee_hire_date,
    COUNT(*) AS cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY
    employee_name, employee_position, employee_salary, employee_hire_date
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

--EXTRAS ON EMPLOYEE
-- 1) Transaction count per employee
SELECT
    employee_name,
    COUNT(*) AS trx_cnt
FROM sl_offline_retail.src_offline_retail
GROUP BY employee_name
ORDER BY trx_cnt DESC, employee_name;

-- 2) min / max / avg distribution
WITH emp_dist AS (
    SELECT employee_name, COUNT(*) AS trx_cnt
    FROM sl_offline_retail.src_offline_retail
    GROUP BY employee_name
)
SELECT
    COUNT(*) AS employee_cnt,
    MIN(trx_cnt) AS min_trx_cnt,
    MAX(trx_cnt) AS max_trx_cnt,
    ROUND(AVG(trx_cnt)::numeric, 2) AS avg_trx_cnt
FROM emp_dist;


-- 3) standard deviation to see how evenly distributed they are
WITH emp_dist AS (
    SELECT employee_name, COUNT(*) AS trx_cnt
    FROM sl_offline_retail.src_offline_retail
    GROUP BY employee_name
)
SELECT
    ROUND(STDDEV_POP(trx_cnt)::numeric, 2) AS stddev_trx_cnt
FROM emp_dist;
-- If min/max are close and stddev is low, distribution is reasonably balanced.

-- ============================================
-- PROMOTIONS (from BOTH sources)
-- ============================================

CREATE TEMP TABLE src_promotions AS
    SELECT promotion_id, promotion_type, promotion_channel,
           promotion_start_dt, promotion_end_dt
    FROM  sl_online_retail.src_online_retail
    UNION ALL
    SELECT promotion_id, promotion_type, promotion_channel,
           promotion_start_dt, promotion_end_dt
    FROM sl_offline_retail.src_offline_retail;

SELECT COUNT(*) FROM src_promotions;

SELECT COUNT(DISTINCT promotion_id) FROM src_promotions;

SELECT promotion_id, COUNT(*) AS cnt
FROM src_promotions
GROUP BY promotion_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- semantic duplication
SELECT COUNT(DISTINCT (
    promotion_type || '-' || promotion_channel )) AS distinct_keys FROM src_promotions;

SELECT
    promotion_type,
    promotion_channel,
    COUNT(*) AS cnt,
    COUNT(DISTINCT promotion_id) AS distinct_keys
FROM src_promotions
GROUP BY promotion_type, promotion_channel
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- with time
SELECT COUNT(DISTINCT (
    promotion_type || '-' || promotion_channel || '-' || promotion_start_dt|| '-' || promotion_end_dt )) AS distinct_keys_with_time FROM src_promotions;

SELECT
    promotion_type,
    promotion_channel,
    promotion_start_dt,
    promotion_end_dt,
    COUNT(*) AS cnt,
    COUNT(DISTINCT promotion_id) AS distinct_keys
FROM src_promotions
GROUP BY
    promotion_type,
    promotion_channel,
    promotion_start_dt,
    promotion_end_dt
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- ============================================
-- DELIVERIES (BOTH)
-- ============================================

CREATE TEMP TABLE src_deliveries AS
    SELECT  delivery_id, delivery_type, delivery_status, shipping_partner
    FROM  sl_online_retail.src_online_retail
    UNION ALL
    SELECT delivery_id, delivery_type, delivery_status, shipping_partner
    FROM sl_offline_retail.src_offline_retail;

SELECT COUNT(*) FROM src_deliveries;

SELECT COUNT(DISTINCT delivery_id) FROM src_deliveries;

SELECT delivery_id, COUNT(*) AS cnt
FROM src_deliveries
GROUP BY delivery_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT COUNT(DISTINCT (
    delivery_type || '-' || delivery_status || '-' || shipping_partner
)) AS distinct_key_ FROM  src_deliveries ;

SELECT
    delivery_type,
    delivery_status,
    shipping_partner,
    COUNT(*) AS cnt,
    COUNT(DISTINCT delivery_id) AS distinct_keys
FROM src_deliveries
GROUP BY delivery_type, delivery_status, shipping_partner
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

SELECT
    delivery_type,
    delivery_status,
    shipping_partner,
    COUNT(*) AS row_cnt,
    COUNT(DISTINCT delivery_id) AS distinct_delivery_ids
FROM (
    SELECT delivery_id, delivery_type, delivery_status, shipping_partner
    FROM  sl_online_retail.src_online_retail
    UNION ALL
    SELECT delivery_id, delivery_type, delivery_status, shipping_partner
    FROM sl_offline_retail.src_offline_retail
) s
GROUP BY delivery_type, delivery_status, shipping_partner
ORDER BY distinct_delivery_ids DESC
LIMIT 20;


-- check whether a single delivery_id maps to more than one descriptive profile
WITH src AS (
    SELECT delivery_id, delivery_type, delivery_status, shipping_partner
    FROM  sl_online_retail.src_online_retail
    UNION ALL
    SELECT delivery_id, delivery_type, delivery_status, shipping_partner
    FROM sl_offline_retail.src_offline_retail
)
SELECT
    delivery_id,
    COUNT(DISTINCT (
        delivery_type || '|' || delivery_status || '|' || shipping_partner
    )) AS profile_versions
FROM src
GROUP BY delivery_id
HAVING COUNT(DISTINCT (
    delivery_type || '|' || delivery_status || '|' || shipping_partner
)) > 1
ORDER BY profile_versions DESC, delivery_id
LIMIT 20;

/* =========================================================
   GENERIC CHECKER - stg.mapping_customers
   ========================================================= */

WITH base AS (
    SELECT *
    FROM stg.mapping_customers -- trials
),

row_stats AS (
    SELECT
        COUNT(*) AS total_rows,

        COUNT(customer_id_nk) AS customer_id_nk_not_null,
        COUNT(DISTINCT customer_id_nk) AS customer_id_nk_distinct,

        COUNT(customer_src_id) AS customer_src_id_not_null,
        COUNT(DISTINCT customer_src_id) AS customer_src_id_distinct,

        COUNT(row_sig) AS row_sig_not_null,
        COUNT(DISTINCT row_sig) AS row_sig_distinct
    FROM base
),


dup_row_sig AS (
    SELECT COUNT(*) AS duplicate_row_sig_groups
    FROM (
        SELECT row_sig
        FROM base
        WHERE row_sig IS NOT NULL
        GROUP BY row_sig
        HAVING COUNT(*) > 1
    ) x
),


dup_customer_nk AS (
    SELECT COUNT(*) AS duplicate_customer_nk_groups
    FROM (
        SELECT customer_id_nk
        FROM base
        WHERE customer_id_nk IS NOT NULL
        GROUP BY customer_id_nk
        HAVING COUNT(*) > 1
    ) x
),


dup_customer_src AS (
    SELECT COUNT(*) AS duplicate_customer_src_id_groups
    FROM (
        SELECT customer_src_id
        FROM base
        WHERE customer_src_id IS NOT NULL
        GROUP BY customer_src_id
        HAVING COUNT(*) > 1
    ) x
),


multi_std_per_raw AS (
    SELECT COUNT(*) AS raw_keys_with_multiple_standardized_versions
    FROM (
        SELECT customer_id_nk
        FROM base
        WHERE customer_id_nk IS NOT NULL
        GROUP BY customer_id_nk
        HAVING COUNT(DISTINCT customer_src_id) > 1
    ) x
),


multi_raw_per_src AS (
    SELECT COUNT(*) AS src_ids_with_multiple_raw_keys
    FROM (
        SELECT customer_src_id
        FROM base
        WHERE customer_src_id IS NOT NULL
        GROUP BY customer_src_id
        HAVING COUNT(DISTINCT customer_id_nk) > 1
    ) x
)


SELECT 'total_rows' AS check_name, total_rows::TEXT AS result
FROM row_stats


UNION ALL
SELECT 'customer_id_nk_not_null', customer_id_nk_not_null::TEXT
FROM row_stats


UNION ALL
SELECT 'customer_id_nk_distinct', customer_id_nk_distinct::TEXT
FROM row_stats


UNION ALL
SELECT 'customer_src_id_not_null', customer_src_id_not_null::TEXT
FROM row_stats


UNION ALL
SELECT 'customer_src_id_distinct', customer_src_id_distinct::TEXT
FROM row_stats


UNION ALL
SELECT 'row_sig_not_null', row_sig_not_null::TEXT
FROM row_stats


UNION ALL
SELECT 'row_sig_distinct', row_sig_distinct::TEXT
FROM row_stats


UNION ALL
SELECT 'duplicate_row_sig_groups', duplicate_row_sig_groups::TEXT
FROM dup_row_sig


UNION ALL
SELECT 'duplicate_customer_nk_groups', duplicate_customer_nk_groups::TEXT
FROM dup_customer_nk


UNION ALL
SELECT 'duplicate_customer_src_id_groups', duplicate_customer_src_id_groups::TEXT
FROM dup_customer_src


UNION ALL
SELECT 'raw_keys_with_multiple_standardized_versions', raw_keys_with_multiple_standardized_versions::TEXT
FROM multi_std_per_raw


UNION ALL
SELECT 'src_ids_with_multiple_raw_keys', src_ids_with_multiple_raw_keys::TEXT
FROM multi_raw_per_src
;


-- ---------------------------------------------------------
-- SAMPLE DUPLICATES: row_sig
-- ---------------------------------------------------------
SELECT
    'sample_duplicate_row_sig' AS section,
    row_sig,
    COUNT(*) AS cnt
FROM stg.mapping_customers
WHERE row_sig IS NOT NULL
GROUP BY row_sig
HAVING COUNT(*) > 1
ORDER BY cnt DESC, row_sig
LIMIT 10;


-- ---------------------------------------------------------
-- SAMPLE DUPLICATES: raw key
-- ---------------------------------------------------------
SELECT
    'sample_duplicate_customer_id_nk' AS section,
    customer_id_nk,
    COUNT(*) AS cnt
FROM stg.mapping_customers
WHERE customer_id_nk IS NOT NULL
GROUP BY customer_id_nk
HAVING COUNT(*) > 1
ORDER BY cnt DESC, customer_id_nk
LIMIT 10;


-- ---------------------------------------------------------
-- SAMPLE DUPLICATES: src id
-- ---------------------------------------------------------
SELECT
    'sample_duplicate_customer_src_id' AS section,
    customer_src_id,
    COUNT(*) AS cnt
FROM stg.mapping_customers
WHERE customer_src_id IS NOT NULL
GROUP BY customer_src_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, customer_src_id
LIMIT 10;
