-- Store Cardinality Diagnostics (READ-ONLY)
-- Purpose: investigate potential row multiplication in store build path.
-- Safety: SELECT-only. No INSERT/UPDATE/DELETE/TRUNCATE/DROP/CALL.

-- ==========================================================
-- 1) Baseline row counts
-- ==========================================================
SELECT 'stg.mapping_stores' AS object_name, COUNT(*) AS row_count FROM stg.mapping_stores
UNION ALL
SELECT 'nf.nf_addresses', COUNT(*) FROM nf.nf_addresses
UNION ALL
SELECT 'nf.nf_stores', COUNT(*) FROM nf.nf_stores
UNION ALL
SELECT 'stg.mapping_transactions', COUNT(*) FROM stg.mapping_transactions;

-- ==========================================================
-- 2) Duplicate checks in mapping stores
-- Expectation: store_src_id should be stable at store grain.
-- ==========================================================
SELECT
    store_src_id,
    COUNT(*) AS rows_per_store_src_id
FROM stg.mapping_stores
GROUP BY store_src_id
HAVING COUNT(*) > 1
ORDER BY rows_per_store_src_id DESC, store_src_id
LIMIT 100;

SELECT
    source_system,
    source_table,
    store_src_id,
    COUNT(*) AS rows_per_src_triplet_store
FROM stg.mapping_stores
GROUP BY source_system, source_table, store_src_id
HAVING COUNT(*) > 1
ORDER BY rows_per_src_triplet_store DESC
LIMIT 100;

-- Insert-control in load_map_stores currently uses store_location_nk + source triplet.
-- This shows whether one location maps to multiple src keys (possible grain ambiguity).
SELECT
    source_system,
    source_table,
    store_location_nk,
    COUNT(DISTINCT store_src_id) AS distinct_store_src_ids,
    COUNT(*) AS total_rows
FROM stg.mapping_stores
GROUP BY source_system, source_table, store_location_nk
HAVING COUNT(DISTINCT store_src_id) > 1
ORDER BY distinct_store_src_ids DESC, total_rows DESC
LIMIT 100;

-- ==========================================================
-- 3) Duplicate checks in nf addresses (critical for join multiplicity)
-- Expectation: address_src_id should be unique.
-- ==========================================================
SELECT
    address_src_id,
    COUNT(*) AS rows_per_address_src_id
FROM nf.nf_addresses
GROUP BY address_src_id
HAVING COUNT(*) > 1
ORDER BY rows_per_address_src_id DESC, address_src_id
LIMIT 100;

-- Address breakdown by city/state/zip to identify normalized key collisions.
SELECT
    c.city_name,
    s.state_name,
    a.zip_code,
    COUNT(*) AS address_rows
FROM nf.nf_addresses a
JOIN nf.nf_cities c ON c.city_id = a.city_id
JOIN nf.nf_states s ON s.state_id = c.state_id
GROUP BY c.city_name, s.state_name, a.zip_code
HAVING COUNT(*) > 1
ORDER BY address_rows DESC, c.city_name
LIMIT 100;

-- ==========================================================
-- 4) Join multiplicity: mapping stores -> nf addresses
-- Mirrors load_ce_stores key logic for address_src_id.
-- ==========================================================
WITH src_stores AS (
    SELECT
        s.store_src_id,
        s.store_name,
        s.store_location_nk,
        COALESCE(s.store_city, 'n.a.') || '-' ||
        COALESCE(s.store_state, 'n.a.') || '-' ||
        COALESCE(s.store_zip_code, 'n.a.') AS address_src_id
    FROM stg.mapping_stores s
    WHERE s.store_src_id <> 'n.a.'
),
join_cardinality AS (
    SELECT
        ss.store_src_id,
        COUNT(a.address_id) AS matched_address_rows
    FROM src_stores ss
    LEFT JOIN nf.nf_addresses a
      ON a.address_src_id = ss.address_src_id
    GROUP BY ss.store_src_id
)
SELECT
    COUNT(*) AS distinct_store_src_id,
    SUM(CASE WHEN matched_address_rows = 0 THEN 1 ELSE 0 END) AS stores_with_no_address_match,
    SUM(CASE WHEN matched_address_rows = 1 THEN 1 ELSE 0 END) AS stores_with_1_address_match,
    SUM(CASE WHEN matched_address_rows > 1 THEN 1 ELSE 0 END) AS stores_with_multi_address_match,
    MAX(matched_address_rows) AS max_address_matches_per_store_src_id,
    AVG(matched_address_rows::NUMERIC) AS avg_address_matches_per_store_src_id
FROM join_cardinality;

-- Top offending store keys where one store_src_id fan-outs to multiple addresses.
WITH src_stores AS (
    SELECT
        s.store_src_id,
        COALESCE(s.store_city, 'n.a.') || '-' ||
        COALESCE(s.store_state, 'n.a.') || '-' ||
        COALESCE(s.store_zip_code, 'n.a.') AS address_src_id
    FROM stg.mapping_stores s
    WHERE s.store_src_id <> 'n.a.'
)
SELECT
    ss.store_src_id,
    ss.address_src_id,
    COUNT(a.address_id) AS matched_address_rows
FROM src_stores ss
LEFT JOIN nf.nf_addresses a
  ON a.address_src_id = ss.address_src_id
GROUP BY ss.store_src_id, ss.address_src_id
HAVING COUNT(a.address_id) > 1
ORDER BY matched_address_rows DESC, ss.store_src_id
LIMIT 100;

-- ==========================================================
-- 5) Current join cardinality vs deduped-address cardinality
-- If deduped join is materially smaller, runtime duplicates likely exist.
-- ==========================================================
WITH src_stores AS (
    SELECT
        s.store_src_id,
        COALESCE(s.store_city, 'n.a.') || '-' ||
        COALESCE(s.store_state, 'n.a.') || '-' ||
        COALESCE(s.store_zip_code, 'n.a.') AS address_src_id
    FROM stg.mapping_stores s
    WHERE s.store_src_id <> 'n.a.'
),
dedup_addresses AS (
    SELECT address_src_id, MIN(address_id) AS address_id
    FROM nf.nf_addresses
    GROUP BY address_src_id
)
SELECT
    (SELECT COUNT(*) FROM src_stores) AS src_rows,
    (SELECT COUNT(*) FROM src_stores ss LEFT JOIN nf.nf_addresses a ON a.address_src_id = ss.address_src_id) AS join_rows_current,
    (SELECT COUNT(*) FROM src_stores ss LEFT JOIN dedup_addresses a ON a.address_src_id = ss.address_src_id) AS join_rows_dedup_addresses;

-- ==========================================================
-- 6) Downstream check: mapping transactions -> nf stores via store_src_id
-- Detect whether one store_src_id maps to multiple nf store IDs.
-- ==========================================================
SELECT
    t.store_src_id,
    COUNT(DISTINCT st.store_id) AS distinct_nf_store_ids,
    COUNT(*) AS joined_rows
FROM stg.mapping_transactions t
LEFT JOIN nf.nf_stores st
  ON st.store_src_id = t.store_src_id
WHERE COALESCE(t.store_src_id, 'n.a.') <> 'n.a.'
GROUP BY t.store_src_id
HAVING COUNT(DISTINCT st.store_id) > 1
ORDER BY distinct_nf_store_ids DESC, joined_rows DESC
LIMIT 100;
