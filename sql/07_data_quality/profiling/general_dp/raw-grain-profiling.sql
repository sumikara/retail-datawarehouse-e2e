/*
Purpose:
- Compact EDA/profiling pack to infer source grain and quality before mapping.
- Uses repository naming conventions (`sl_*` schemas).

How to read:
- Each section starts with: Purpose / Expected output / Interpretation.
*/

/* =====================================================================
1) HIGH-LEVEL CARDINALITY SNAPSHOT
Purpose:
- Compare row count vs key/domain cardinalities per source in one pass.
Expected output:
- 1 row per source with total rows + distinct counts.
Interpretation:
- If distinct_transaction_id << total_rows, dataset is not header-grain.
- Offline-only domains (store_location, employee_name) help identify store/agent behavior.
===================================================================== */
WITH source_data AS (
    SELECT
        'offline'::text AS source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        NULL::text AS engagement_id,
        store_location,
        employee_name,
        quantity,
        unit_price,
        total_sales
    FROM sl_offline_retail.src_offline_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL

    UNION ALL

    SELECT
        'online'::text AS source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        engagement_id,
        NULL::text AS store_location,
        NULL::text AS employee_name,
        quantity,
        unit_price,
        total_sales
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
)
SELECT
    source_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS distinct_transaction_id,
    COUNT(DISTINCT transaction_dt) AS distinct_transaction_dt,
    COUNT(DISTINCT customer_id) AS distinct_customer_id,
    COUNT(DISTINCT product_id) AS distinct_product_id,
    COUNT(DISTINCT promotion_id) AS distinct_promotion_id,
    COUNT(DISTINCT delivery_id) AS distinct_delivery_id,
    COUNT(DISTINCT store_location) FILTER (WHERE source_name = 'offline') AS distinct_store_location,
    COUNT(DISTINCT employee_name) FILTER (WHERE source_name = 'offline') AS distinct_employee_name,
    COUNT(DISTINCT engagement_id) FILTER (WHERE source_name = 'online') AS distinct_engagement_id
FROM source_data
GROUP BY source_name
ORDER BY source_name;

/* =====================================================================
2) TRANSACTION-LEVEL GRAIN SIGNALS
Purpose:
- Quantify repeated transaction rows and multi-value behavior inside each transaction.
Expected output:
- 1 row per source with repeated rows and multi-attribute transaction counts.
Interpretation:
- High multi_product_txn strongly suggests line-grain.
- Offline: multi_store_location_txn / multi_employee_txn are strong grain conflict signals.
===================================================================== */
WITH source_data AS (
    SELECT
        'offline'::text AS source_name,
        transaction_id,
        product_id,
        delivery_id,
        promotion_id,
        store_location,
        employee_name
    FROM sl_offline_retail.src_offline_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL

    UNION ALL

    SELECT
        'online'::text AS source_name,
        transaction_id,
        product_id,
        delivery_id,
        promotion_id,
        NULL::text AS store_location,
        NULL::text AS employee_name
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
),
txn_rollup AS (
    SELECT
        source_name,
        transaction_id,
        COUNT(*) AS line_count,
        COUNT(DISTINCT product_id) FILTER (WHERE NULLIF(BTRIM(product_id), '') IS NOT NULL) AS distinct_product_cnt,
        COUNT(DISTINCT delivery_id) FILTER (WHERE NULLIF(BTRIM(delivery_id), '') IS NOT NULL) AS distinct_delivery_cnt,
        COUNT(DISTINCT promotion_id) FILTER (WHERE NULLIF(BTRIM(promotion_id), '') IS NOT NULL) AS distinct_promotion_cnt,
        COUNT(DISTINCT store_location) FILTER (WHERE NULLIF(BTRIM(store_location), '') IS NOT NULL) AS distinct_store_location_cnt,
        COUNT(DISTINCT employee_name) FILTER (WHERE NULLIF(BTRIM(employee_name), '') IS NOT NULL) AS distinct_employee_cnt
    FROM source_data
    GROUP BY source_name, transaction_id
)
SELECT
    source_name,
    SUM(line_count - 1) AS repeated_transaction_rows,
    COUNT(*) FILTER (WHERE distinct_product_cnt > 1) AS multi_product_txn,
    COUNT(*) FILTER (WHERE distinct_delivery_cnt > 1) AS multi_delivery_txn,
    COUNT(*) FILTER (WHERE distinct_promotion_cnt > 1) AS multi_promotion_txn,
    COUNT(*) FILTER (WHERE source_name = 'offline' AND distinct_store_location_cnt > 1) AS multi_store_location_txn,
    COUNT(*) FILTER (WHERE source_name = 'offline' AND distinct_employee_cnt > 1) AS multi_employee_txn
FROM txn_rollup
GROUP BY source_name
ORDER BY source_name;

/* =====================================================================
3) ONLINE-ONLY: ENGAGEMENT BEHAVIOR PER TRANSACTION
Purpose:
- Check whether engagement_id behaves as transaction-level or line-level.
Expected output:
- Single metric: number of transactions with >1 engagement_id.
Interpretation:
- If >0, engagement_id is not strictly transaction-header level.
===================================================================== */
SELECT
    COUNT(*) AS multi_engagement_txn
FROM (
    SELECT transaction_id
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
      AND NULLIF(BTRIM(engagement_id), '') IS NOT NULL
    GROUP BY transaction_id
    HAVING COUNT(DISTINCT engagement_id) > 1
) t;

/* =====================================================================
4) CANDIDATE BUSINESS KEY STRENGTH
Purpose:
- Test whether key candidates approach row-level uniqueness.
Expected output:
- 1 row per source with total_rows vs distinct key combinations.
Interpretation:
- If candidate distinct count ~= total_rows, candidate is strong for grain definition.
- Offline candidate includes store_location + employee_name for stronger line disambiguation.
===================================================================== */
WITH source_data AS (
    SELECT
        'offline'::text AS source_name,
        transaction_id,
        product_id,
        customer_id,
        transaction_dt,
        store_location,
        employee_name
    FROM sl_offline_retail.src_offline_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL

    UNION ALL

    SELECT
        'online'::text AS source_name,
        transaction_id,
        product_id,
        customer_id,
        transaction_dt,
        NULL::text AS store_location,
        NULL::text AS employee_name
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
)
SELECT
    source_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (transaction_id, product_id)) AS distinct_trx_product,
    COUNT(DISTINCT (transaction_id, product_id, customer_id, transaction_dt)) AS distinct_trx_product_customer_dt,
    COUNT(DISTINCT (transaction_id, product_id, store_location, employee_name, transaction_dt))
        FILTER (WHERE source_name = 'offline') AS distinct_trx_product_store_employee_dt
FROM source_data
GROUP BY source_name
ORDER BY source_name;

/* =====================================================================
5) PRACTICAL DUPLICATE GROUP CHECK
Purpose:
- Detect exact business-content duplicate groups (not full-row hash).
Expected output:
- 1 row per source with duplicate group count.
Interpretation:
- Non-zero indicates potential replay/loading duplicates or true repeated lines.
===================================================================== */
WITH source_data AS (
    SELECT
        'offline'::text AS source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        NULL::text AS engagement_id,
        store_location,
        employee_name,
        quantity,
        unit_price,
        total_sales
    FROM sl_offline_retail.src_offline_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL

    UNION ALL

    SELECT
        'online'::text AS source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        engagement_id,
        NULL::text AS store_location,
        NULL::text AS employee_name,
        quantity,
        unit_price,
        total_sales
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
),
dup_groups AS (
    SELECT
        source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        engagement_id,
        store_location,
        employee_name,
        quantity,
        unit_price,
        total_sales
    FROM source_data
    GROUP BY
        source_name,
        transaction_id,
        transaction_dt,
        customer_id,
        product_id,
        promotion_id,
        delivery_id,
        engagement_id,
        store_location,
        employee_name,
        quantity,
        unit_price,
        total_sales
    HAVING COUNT(*) > 1
)
SELECT
    s.source_name,
    COALESCE(COUNT(d.source_name), 0) AS duplicate_group_count
FROM (VALUES ('offline'::text), ('online'::text)) AS s(source_name)
LEFT JOIN dup_groups d
    ON s.source_name = d.source_name
GROUP BY s.source_name
ORDER BY s.source_name;

/* =====================================================================
6) TOP REPEATED TRANSACTIONS (DIAGNOSTIC SAMPLE)
Purpose:
- Return a compact sample of highly repeated transactions for manual inspection.
Expected output:
- Up to 20 rows per source with line count and diversity metrics.
Interpretation:
- Offline store/employee diversity supports more precise grain decisions.
===================================================================== */
WITH source_data AS (
    SELECT
        'offline'::text AS source_name,
        transaction_id,
        product_id,
        delivery_id,
        promotion_id,
        NULL::text AS engagement_id,
        store_location,
        employee_name
    FROM sl_offline_retail.src_offline_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL

    UNION ALL

    SELECT
        'online'::text AS source_name,
        transaction_id,
        product_id,
        delivery_id,
        promotion_id,
        engagement_id,
        NULL::text AS store_location,
        NULL::text AS employee_name
    FROM sl_online_retail.src_online_retail
    WHERE NULLIF(BTRIM(transaction_id), '') IS NOT NULL
),
txn_rollup AS (
    SELECT
        source_name,
        transaction_id,
        COUNT(*) AS line_count,
        COUNT(DISTINCT product_id) AS distinct_product_count,
        COUNT(DISTINCT delivery_id) AS distinct_delivery_count,
        COUNT(DISTINCT promotion_id) AS distinct_promotion_count,
        COUNT(DISTINCT engagement_id) FILTER (WHERE source_name = 'online') AS distinct_engagement_count,
        COUNT(DISTINCT store_location) FILTER (WHERE source_name = 'offline') AS distinct_store_location_count,
        COUNT(DISTINCT employee_name) FILTER (WHERE source_name = 'offline') AS distinct_employee_count,
        ROW_NUMBER() OVER (PARTITION BY source_name ORDER BY COUNT(*) DESC, transaction_id) AS rn
    FROM source_data
    GROUP BY source_name, transaction_id
    HAVING COUNT(*) > 1
)
SELECT
    source_name,
    transaction_id,
    line_count,
    distinct_product_count,
    distinct_delivery_count,
    distinct_promotion_count,
    distinct_engagement_count,
    distinct_store_location_count,
    distinct_employee_count
FROM txn_rollup
WHERE rn <= 20
ORDER BY source_name, line_count DESC, transaction_id;
