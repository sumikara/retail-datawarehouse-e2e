-- states
CREATE OR REPLACE PROCEDURE stg.load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc TEXT := 'stg.load_ce_states';
    v_ins INT := 0;
    v_err_msg TEXT;
    v_err_detail TEXT;
    v_err_hint TEXT;
BEGIN
WITH src_states AS (
    SELECT customer_state AS state_name
    FROM stg.mapping_customers
    UNION ALL
    SELECT store_state
    FROM stg.mapping_stores
),
     keep one row per state business key */
distinct_states AS (
    SELECT DISTINCT state_name
    FROM src_states
    WHERE state_name <> 'n.a.'
)
INSERT INTO 3nf.nf_states (
    state_id,
    state_src_id,
    state_name,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_nf_state_id'),
    s.state_name,
    s.state_name,
    'stg',
    'mapping_customers + t_map_stores'
FROM distinct_states s
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_states tgt
    WHERE tgt.state_src_id = s.state_name
);
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
        v_proc,
    '3nf.nf_states',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Loaded formatted ce_states rows from t_map_customers + t_map_stores '
    );
    RAISE NOTICE 'ce_states completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_states',
        0,
        'FAILED',
        'State load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- cities
CREATE OR REPLACE PROCEDURE stg.load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc TEXT := 'stg.load_ce_cities';
    v_ins INT := 0;
    v_err_msg TEXT;
    v_err_detail TEXT;
    v_err_hint TEXT;

BEGIN
    WITH src AS (
        SELECT customer_city AS city, customer_state AS state
        FROM stg.mapping_customers
        UNION ALL
        SELECT store_city, store_state
        FROM stg.mapping_stores
    ),
    distinct_cities AS (
        SELECT DISTINCT
            city,
            state,
            city || '-' || state AS city_src_id
        FROM src
        WHERE city  <> 'n.a.'
          AND state <> 'n.a.'

    ),
    resolved_state AS (
        SELECT
            d.city_src_id,
            d.city,
            COALESCE(s.state_id, -1) AS state_id
        FROM distinct_cities d
        LEFT JOIN 3nf.nf_states s
            ON s.state_src_id = d.state
    )
INSERT INTO 3nf.nf_cities (
    city_id,
    city_src_id,
    city_name,
    state_id,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_nf_city_id'),
    r.city_src_id,
    r.city,
    r.state_id,
    'stg',
    'mapping_customers + t_map_stores'
FROM resolved_state r
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_cities tgt
    WHERE tgt.city_src_id = r.city_src_id
);
GET DIAGNOSTICS v_ins = ROW_COUNT;
PERFORM 3nf.log_etl_event(
    v_proc,
    '3nf.nf_cities',
    v_ins,
    CASE WHEN v_ins>0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
    'Cities loaded'
);
RAISE NOTICE 'ce_cities completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_cities',
        0,
        'FAILED',
        'City load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;
-- addressess
CREATE OR REPLACE PROCEDURE stg.load_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc       TEXT := 'stg.load_ce_addresses';
    v_ins        INT  := 0;
    v_err_msg    TEXT;
    v_err_detail TEXT;
    v_err_hint   TEXT;
BEGIN
    WITH src AS (

    SELECT
        customer_city AS city,
        customer_state AS state,
        customer_zip_code AS zip
    FROM stg.mapping_customers
    UNION ALL
    SELECT
        store_city,
        store_state,
        store_zip_code
    FROM stg.mapping_stores
),
distinct_addresses AS (
    SELECT DISTINCT
        city,
        state,
        COALESCE(zip, 'n.a.') AS zip,
        city || '-' || state AS city_src_id,
        city || '-' || state || '-' || COALESCE(zip, 'n.a.') AS address_src_id
    FROM src
    WHERE city  <> 'n.a.'
      AND state <> 'n.a.'
),
resolved_city AS (
    SELECT
        d.address_src_id,
        d.zip,
        COALESCE(c.city_id, -1) AS city_id
    FROM distinct_addresses d
    LEFT JOIN 3nf.nf_cities c
        ON c.city_src_id = d.city_src_id
)
INSERT INTO 3nf.nf_addresses (
    address_id,
    address_src_id,
    zip_code,
    city_id,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_nf_address_id'),
    r.address_src_id,
    r.zip,
    r.city_id,
    'stg',
    'mapping_customers + t_map_stores'
FROM resolved_city r
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_addresses tgt
    WHERE tgt.address_src_id = r.address_src_id
);
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_addresses',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded addresses from customer/store geography.'
    );
    RAISE NOTICE 'ce_addresses completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_addresses',
        0,
        'FAILED',
        'Address load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- customers
CREATE OR REPLACE PROCEDURE stg.load_ce_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_customers';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_customers;
    CREATE TEMP TABLE tmp_final_customers
    ON COMMIT DROP
    AS
    WITH src_customers AS (
        SELECT
            c.customer_src_id,           -- engineered key
            c.customer_id AS customer_id_nk,  -- raw key
            c.gender,
            c.marital_status,
            c.birth_of_dt,
            c.membership_dt,
            c.last_purchase_dt,
            c.customer_city,
            c.customer_state,
            c.customer_zip_code,
            /* Address business key */
            COALESCE(c.customer_city, 'n.a.') || '-' ||
            COALESCE(c.customer_state, 'n.a.') || '-' ||
            COALESCE(c.customer_zip_code, 'n.a.') AS address_src_id
        FROM stg.mapping_customers c
        WHERE c.customer_src_id <> 'n.a.'
    ),
    ranked_customers AS (
    SELECT
        s.customer_src_id,
        s.customer_id_nk,
        COALESCE(s.gender, 'n.a.') AS gender,
        COALESCE(s.marital_status, 'n.a.') AS marital_status,
        COALESCE(s.birth_of_dt, DATE '1900-01-01') AS birth_of_dt,
        COALESCE(s.membership_dt, DATE '1900-01-01') AS membership_dt,
        COALESCE(s.last_purchase_dt, TIMESTAMP '1900-01-01 00:00:00') AS last_purchase_dt,
        COALESCE(ad.address_id, -1) AS address_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_src_id
            ORDER BY
                s.membership_dt DESC NULLS LAST,
                s.last_purchase_dt DESC NULLS LAST,
                s.customer_id_nk DESC,
                s.address_src_id DESC
        ) AS rn
    FROM src_customers s
    LEFT JOIN 3nf.nf_addresses ad
        ON ad.address_src_id = s.address_src_id
)
    SELECT
        customer_src_id,
        customer_id_nk,
        gender,
        marital_status,
        birth_of_dt,
        membership_dt,
        last_purchase_dt,
        address_id,
        'stg' AS source_system,
        'mapping_customers' AS source_table
    FROM ranked_customers
    WHERE rn = 1;
    CREATE INDEX idx_tmp_final_customers_src
        ON tmp_final_customers (customer_src_id);

    INSERT INTO 3nf.nf_customers (
        customer_id,
        customer_src_id,
        customer_id_nk,
        gender,
        marital_status,
        birth_of_dt,
        membership_dt,
        last_purchase_dt,
        address_id,
        source_system,
        source_table,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('bl_3nf.seq_nf_customer_id'),
        f.customer_src_id,
        f.customer_id_nk,
        f.gender,
        f.marital_status,
        f.birth_of_dt,
        f.membership_dt,
        f.last_purchase_dt,
        f.address_id,
        f.source_system,
        f.source_table,
        NOW(),
        NOW()
    FROM tmp_final_customers f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_customers ce
        WHERE nf.customer_src_id = f.customer_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    UPDATE 3nf.nf_customers ce
    SET
        customer_id_nk   = f.customer_id_nk,
        gender           = f.gender,
        marital_status   = f.marital_status,
        birth_of_dt      = f.birth_of_dt,
        membership_dt    = f.membership_dt,
        last_purchase_dt = f.last_purchase_dt,
        address_id       = f.address_id,
        source_system    = f.source_system,
        source_table     = f.source_table,
        update_dt        = NOW()
    FROM tmp_final_customers f
    WHERE nf.customer_src_id = f.customer_src_id
      AND (
            nf.customer_id_nk   IS DISTINCT FROM f.customer_id_nk
         OR nf.gender           IS DISTINCT FROM f.gender
         OR nf.marital_status   IS DISTINCT FROM f.marital_status
         OR nf.birth_of_dt      IS DISTINCT FROM f.birth_of_dt
         OR nf.membership_dt    IS DISTINCT FROM f.membership_dt
         OR nf.last_purchase_dt IS DISTINCT FROM f.last_purchase_dt
         OR nf.address_id       IS DISTINCT FROM f.address_id
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM 3nf.log_etl_event(
    v_proc,
    '3nf.nf_customers',
    (v_ins + v_upd),
    CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
    'Inserted=' || v_ins || ', Updated=' || v_upd ||
    '. Type 1 customer load using engineered customer_src_id.',
    NULL,
    'INFO',
    v_ins,
    v_upd,
    0,
    0,
    NULL
);
    RAISE NOTICE 'ce_customers completed. inserted=%, updated=%', v_ins, v_upd;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_customers',
        0,
        'FAILED',
        'ce_customers load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

--stores
CREATE OR REPLACE PROCEDURE stg.load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_stores';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_stores;
    CREATE TEMP TABLE tmp_final_stores
    ON COMMIT DROP
    AS
    WITH src_stores AS (
        SELECT
            s.store_src_id,                 -- already engineered in t_map_stores
            s.store_name,
            s.store_city || '-' ||
            s.store_state || '-' ||
            s.store_zip_code AS address_src_id --just for matching
        FROM stg.mapping_stores s
        WHERE s.store_src_id <> 'n.a.'
    ),
    resolved_address AS (
        SELECT
            ss.store_src_id,
            ss.store_name,
            COALESCE(a.address_id, -1) AS address_id

        FROM src_stores ss
        LEFT JOIN 3nf.nf_addresses a
            ON a.address_src_id = ss.address_src_id
    ),
    ranked_stores AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.store_src_id
                ORDER BY r.address_id DESC,
                         r.store_name
            ) AS rn
        FROM resolved_address r
    )
    SELECT
        store_src_id,
        store_name,
        address_id,
        'stg' AS source_system,
        'mapping_stores' AS source_table
    FROM ranked_stores
    WHERE rn = 1;
    CREATE INDEX idx_tmp_final_stores_src
        ON tmp_final_stores (store_src_id);

    -- INSERT
    INSERT INTO 3nf.nf_stores (
        store_id,
        store_src_id,
        store_name,
        address_id,
        source_system,
        source_table,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('bl_3nf.seq_nf_store_id'),
        f.store_src_id,
        f.store_name,
        f.address_id,
        f.source_system,
        f.source_table,
        NOW(),
        NOW()
    FROM tmp_final_stores f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_stores ce
        WHERE nf.store_src_id = f.store_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    UPDATE 3nf.nf_stores ce
    SET
        store_name    = f.store_name,
        address_id    = f.address_id,
        source_system = f.source_system,
        source_table  = f.source_table,
        update_dt     = NOW()
    FROM tmp_final_stores f
    WHERE nf.store_src_id = f.store_src_id
      AND (
            nf.store_name    IS DISTINCT FROM f.store_name
         OR nf.address_id    IS DISTINCT FROM f.address_id
         OR nf.source_system IS DISTINCT FROM f.source_system
         OR nf.source_table  IS DISTINCT FROM f.source_table
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_stores',
        (v_ins + v_upd),
        CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd ||
        '. Source=stg.mapping_stores; Type 1 load; address FK resolved from ce_addresses.'
    );
    RAISE NOTICE 'ce_stores completed. inserted=%, updated=%', v_ins, v_upd;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_stores',
        0,
        'FAILED',
        'ce_stores load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- product categories
CREATE OR REPLACE PROCEDURE stg.load_ce_product_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_product_categories';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_product_categories;
    CREATE TEMP TABLE tmp_final_product_categories
    ON COMMIT DROP
    AS
    SELECT DISTINCT
        p.product_category        AS product_category_src_id,
        p.product_category        AS product_category_name,
        'stg'::VARCHAR(100)     AS source_system,
        'mapping_products'::VARCHAR(100) AS source_table
    FROM stg.mapping_products p
    WHERE p.product_category IS NOT NULL
      AND p.product_category <> 'n.a.';
    CREATE INDEX idx_tmp_final_product_categories_src
        ON tmp_final_product_categories (product_category_src_id);

    INSERT INTO 3nf.nf_product_categories (
        product_category_id,
        product_category_src_id,
        product_category_name,
        source_system,
        source_table,
        insert_dt
            )
    SELECT
        nextval('bl_3nf.seq_nf_product_category_id'),
        f.product_category_src_id,
        f.product_category_name,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_product_categories f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_product_categories ce
        WHERE nf.product_category_src_id = f.product_category_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;
   PERFORM 3nf.log_etl_event(
    v_proc,
    '3nf.nf_product_categories',
    v_ins,
    CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
    'Inserted=' || v_ins ||
    '. Loaded distinct product_categories from stg.mapping_products.',
    NULL,
    'INFO',
    v_ins,
    0,
    0,
    NULL
);
    RAISE NOTICE 'ce_product_categories completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    -- Capture detailed PostgreSQL diagnostics
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_product_categories',
        0,
        'FAILED',
        'Product categories 3nf load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- products
CREATE OR REPLACE PROCEDURE stg.load_ce_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_products';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_products;
    CREATE TEMP TABLE tmp_final_products
    ON COMMIT DROP
    AS
    WITH src_products AS (
        SELECT
            p.product_id AS product_src_id,
            p.product_category,
            p.product_name,
            p.product_brand,
            p.product_stock,
            p.product_material,
            p.product_manufacture_dt,
            p.product_expiry_dt
        FROM stg.mapping_products p
        WHERE p.product_id <> 'n.a.'
    ),
    category_resolved AS (
        SELECT
            s.product_src_id,
            COALESCE(c.product_category_id, -1) AS product_category_id,
            s.product_name,
            s.product_brand,
            s.product_stock,
            s.product_material,
            s.product_manufacture_dt,
            s.product_expiry_dt
        FROM src_products s
        LEFT JOIN 3nf.nf_product_categories c
            ON c.product_category_src_id = s.product_category
    ),
    ranked_products AS (
        SELECT
            cr.*,
            ROW_NUMBER() OVER (
                PARTITION BY cr.product_src_id
                ORDER BY
                    cr.product_manufacture_dt DESC NULLS LAST,
                    cr.product_expiry_dt DESC NULLS LAST,
                    cr.product_stock DESC NULLS LAST,
                    cr.product_category_id DESC,
                    cr.product_name DESC,
                    cr.product_brand DESC,
                    cr.product_material DESC
            ) AS rn
        FROM category_resolved cr
    )
    SELECT
        product_src_id,
        product_category_id,
        product_name,
        product_brand,
        product_stock,
        product_material,
        product_manufacture_dt,
        product_expiry_dt,
        'stg' AS source_system,
        'mapping_products' AS source_table
    FROM ranked_products
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_products_src
        ON tmp_final_products (product_src_id);

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
        nextval('bl_3nf.seq_nf_product_id'),
        f.product_src_id,
        f.product_category_id,
        f.product_name,
        f.product_brand,
        f.product_stock,
        f.product_material,
        f.product_manufacture_dt,
        f.product_expiry_dt,
        f.source_system,
        f.source_table,
        NOW(),
        NOW()
    FROM tmp_final_products f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_products ce
        WHERE nf.product_src_id = f.product_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    UPDATE 3nf.nf_products ce
    SET
        product_category_id    = f.product_category_id,
        product_name           = f.product_name,
        product_brand          = f.product_brand,
        product_stock          = f.product_stock,
        product_material       = f.product_material,
        product_manufacture_dt = f.product_manufacture_dt,
        product_expiry_dt      = f.product_expiry_dt,
        update_dt              = NOW()
    FROM tmp_final_products f
    WHERE nf.product_src_id = f.product_src_id
      AND (
            nf.product_category_id    IS DISTINCT FROM f.product_category_id
         OR nf.product_name           IS DISTINCT FROM f.product_name
         OR nf.product_brand          IS DISTINCT FROM f.product_brand
         OR nf.product_stock          IS DISTINCT FROM f.product_stock
         OR nf.product_material       IS DISTINCT FROM f.product_material
         OR nf.product_manufacture_dt IS DISTINCT FROM f.product_manufacture_dt
         OR nf.product_expiry_dt      IS DISTINCT FROM f.product_expiry_dt
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_products',
        v_upd + v_ins,
        CASE WHEN (v_upd + v_ins) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'SCD Type 1 upsert on products completed.',
        NULL,
        'INFO',
        v_ins,
        v_upd,
        0,
        0,
        NULL
    );
    RAISE NOTICE 'ce_products completed. inserted=%, updated=%', v_ins, v_upd;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_products',
        0,
        'FAILED',
        'Products 3nf load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- promotion types
CREATE OR REPLACE PROCEDURE stg.load_ce_promotion_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_promotion_types';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    INSERT INTO 3nf.nf_promotion_types (
        promotion_type_id,
        promotion_type_src_id,
        promotion_type_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_nf_promotion_type_id'),
        s.promotion_type_src_id,
        s.promotion_type_name,
        'stg',
        'mapping_promotions',
        NOW()
    FROM (
        SELECT DISTINCT
            p.promotion_type AS promotion_type_src_id,
            p.promotion_type AS promotion_type_name
        FROM stg.mapping_promotions p
        WHERE COALESCE(p.promotion_type, 'n.a.') <> 'n.a.'
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_promotion_types t
        WHERE t.promotion_type_src_id = s.promotion_type_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_promotion_types',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded promotion types from stg.mapping_promotions.'
    );
            RAISE NOTICE 'ce_promotion_types completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_promotion_types',
        0,
        'FAILED',
        'Promotion types load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- promotions
CREATE OR REPLACE PROCEDURE stg.load_ce_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_promotions';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_promotions;
    CREATE TEMP TABLE tmp_final_promotions
    ON COMMIT DROP
    AS
    WITH src_promotions AS (
        SELECT
            p.promotion_id AS promotion_src_id,
            p.promotion_type,
            p.promotion_channel,
            p.promotion_start_dt,
            p.promotion_end_dt

        FROM stg.mapping_promotions p
        WHERE p.promotion_id <> 'n.a.'
    ),
    ranked AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.promotion_src_id
                ORDER BY
                    s.promotion_start_dt DESC NULLS LAST,
                    s.promotion_end_dt   DESC NULLS LAST,
                    s.promotion_type DESC,
                    s.promotion_channel DESC
            ) AS rn
        FROM src_promotions s
    ),
    resolved_type AS (
        SELECT
            r.promotion_src_id,
            COALESCE(pt.promotion_type_id, -1) AS promotion_type_id,
            r.promotion_channel,
            r.promotion_start_dt,
            r.promotion_end_dt,
            'stg'::VARCHAR(100) AS source_system,
            'mapping_promotions'::VARCHAR(100) AS source_table
        FROM ranked r
        LEFT JOIN 3nf.nf_promotion_types pt
            ON pt.promotion_type_src_id = r.promotion_type
        WHERE r.rn = 1
    )
    SELECT
        promotion_src_id,
        promotion_type_id,
        promotion_channel,
        promotion_start_dt,
        promotion_end_dt,
        source_system,
        source_table
    FROM resolved_type;

    CREATE INDEX idx_tmp_final_promotions_src
        ON tmp_final_promotions (promotion_src_id);
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
        nextval('bl_3nf.seq_nf_promotion_id'),
        f.promotion_src_id,
        f.promotion_type_id,
        f.promotion_channel,
        f.promotion_start_dt,
        f.promotion_end_dt,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_promotions f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_promotions ce
        WHERE nf.promotion_src_id = f.promotion_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
    v_proc,
    '3nf.nf_promotions',
    v_ins,
    CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
    'Inserted=' || v_ins || '. Type 0 load: one immutable row per promotion_src_id.'
);

            RAISE NOTICE 'ce_promotion completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_promotions',
        0,
        'FAILED',
        'Promotions load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

--shipping partners
CREATE OR REPLACE PROCEDURE stg.load_ce_shipping_partners()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_shipping_partners';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;

BEGIN
    INSERT INTO 3nf.nf_shipping_partners (
        shipping_partner_id,
        shipping_partner_src_id,
        shipping_partner_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_nf_shipping_partner_id'),
        s.shipping_partner_src_id,
        s.shipping_partner_name,
        'stg',
        'mapping_deliveries',
        NOW()
    FROM (
        SELECT DISTINCT
            COALESCE(d.shipping_partner, 'n.a.') AS shipping_partner_src_id,
            COALESCE(d.shipping_partner, 'n.a.') AS shipping_partner_name
        FROM stg.mapping_deliveries d
        WHERE COALESCE(d.shipping_partner, 'n.a.') <> 'n.a.'
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_shipping_partners t
        WHERE t.shipping_partner_src_id = s.shipping_partner_src_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_shipping_partners',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded unique shipping partners from stg.mapping_deliveries.'
    );
    RAISE NOTICE 'ce_shipping_partners completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_shipping_partners',
        0,
        'FAILED',
        'Shipping partners load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

--deliveries
CREATE OR REPLACE PROCEDURE stg.load_ce_deliveries()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_deliveries';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
       DROP TABLE IF EXISTS tmp_final_deliveries;
    CREATE TEMP TABLE tmp_final_deliveries
    ON COMMIT DROP
    AS
    WITH src AS (
        SELECT
            COALESCE(d.delivery_id, 'n.a.') AS delivery_src_id,
            COALESCE(d.shipping_partner, 'n.a.') AS shipping_partner_src_id,
            COALESCE(d.delivery_type, 'n.a.') AS delivery_type,
            COALESCE(d.delivery_status, 'n.a.') AS delivery_status,

            'stg' AS source_system,
            'mapping_deliveries' AS source_table
        FROM stg.mapping_deliveries d
        WHERE COALESCE(d.delivery_id, 'n.a.') <> 'n.a.'
    ),
    ranked AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.delivery_src_id
                ORDER BY
                    s.delivery_status,
                    s.delivery_type,
                    s.shipping_partner_src_id
            ) AS rn
        FROM src s
    ),
    resolved_partner AS (
        SELECT
            r.delivery_src_id,
            COALESCE(sp.shipping_partner_id, -1) AS shipping_partner_id,
            r.delivery_type,
            r.delivery_status,
            r.source_system,
            r.source_table
        FROM ranked r
        LEFT JOIN 3nf.nf_shipping_partners sp
            ON sp.shipping_partner_src_id = r.shipping_partner_src_id
        WHERE r.rn = 1
    )
    SELECT
        delivery_src_id,
        shipping_partner_id,
        delivery_type,
        delivery_status,
        source_system,
        source_table
    FROM resolved_partner;
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
        nextval('bl_3nf.seq_nf_delivery_id'),
        f.delivery_src_id,
        f.shipping_partner_id,
        f.delivery_type,
        f.delivery_status,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_deliveries f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_deliveries ce
        WHERE nf.delivery_src_id = f.delivery_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_deliveries',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Source=stg.mapping_deliveries; Type 0 load; one row per delivery_src_id; shipping_partner FK resolved from ce_shipping_partners.'
    );
    RAISE NOTICE 'ce_deliveries completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_deliveries',
        0,
        'FAILED',
        'Deliveries 3nf load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

--engagements
CREATE OR REPLACE PROCEDURE stg.load_ce_engagements()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_engagements';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_engagements;

    CREATE TEMP TABLE tmp_final_engagements
    ON COMMIT DROP
    AS
    WITH ranked AS (
        SELECT
            /*  TRUE KEY */
            e.engagement_id AS engagement_src_id,
            e.customer_support_calls,
            e.website_address,
            e.order_channel,
            e.customer_support_method,
            e.issue_status,
            e.app_usage,
            e.website_visits,
            e.social_media_engagement,
            ROW_NUMBER() OVER (
                PARTITION BY e.engagement_id
                ORDER BY e.website_visits DESC NULLS LAST
            ) AS rn
        FROM stg.mapping_engagements e
        WHERE e.engagement_id <> 'n.a.'
    )
    SELECT
        engagement_src_id,
        customer_support_calls,
        website_address,
        order_channel,
        customer_support_method,
        issue_status,
        app_usage,
        website_visits,
        social_media_engagement,
        'stg'::VARCHAR(100) AS source_system,
        'mapping_engagements'::VARCHAR(100) AS source_table
    FROM ranked
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_engagements_src
        ON tmp_final_engagements (engagement_src_id);

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
        nextval('bl_3nf.seq_nf_engagement_id'),
        f.engagement_src_id,
        f.customer_support_calls,
        f.website_address,
        f.order_channel,
        f.customer_support_method,
        f.issue_status,
        f.app_usage,
        f.website_visits,
        f.social_media_engagement,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_engagements f
    WHERE NOT EXISTS (
        SELECT 1
        FROM 3nf.nf_engagements ce
        WHERE nf.engagement_src_id = f.engagement_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_engagements',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||'. Source = stg.mapping_engagements; Type 0 load. one row per engagement.'
    );
    RAISE NOTICE 'ce_engagements completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;
    PERFORM 3nf.log_etl_event(
        v_proc,
        '3nf.nf_engagements',
        0,
        'FAILED',
        'Engagements 3nf load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

--employees
CREATE OR REPLACE PROCEDURE stg.load_ce_employees_scd()
LANGUAGE plpgsql
    AS $$
    DECLARE
        v_proc        TEXT := 'stg.load_ce_employees_scd';
        v_closed      INT  := 0;
        v_ins         INT  := 0;
        v_err_msg     TEXT;
        v_err_detail  TEXT;
        v_err_hint    TEXT;
    BEGIN
        DROP TABLE IF EXISTS tmp_employee_src;
        DROP TABLE IF EXISTS tmp_employee_changes;
        CREATE TEMP TABLE tmp_employee_src
        ON COMMIT DROP
        AS
        WITH ranked_src AS (
            SELECT
                s.employee_src_id,
                s.employee_name,
                s.employee_position,
                s.employee_salary,
                s.employee_hire_date,
                COALESCE(s.observed_ts, TIMESTAMP '1900-01-01 00:00:00') AS observed_ts,
                s.source_system,
                s.source_table,
                ROW_NUMBER() OVER (
                    PARTITION BY s.employee_src_id
                    ORDER BY
                        COALESCE(s.observed_ts, TIMESTAMP '1900-01-01 00:00:00') DESC,
                        s.employee_salary DESC NULLS LAST,
                        s.source_table DESC
                ) AS rn
            FROM stg.mapping_employees s
            WHERE s.employee_src_id <> 'n.a.'
        )
        SELECT
            employee_src_id,
            employee_name,
            employee_position,
            employee_salary,
            employee_hire_date,
            observed_ts,
            source_system,
            source_table
        FROM ranked_src
        WHERE rn = 1;

        CREATE INDEX idx_tmp_employee_src_id
            ON tmp_employee_src (employee_src_id);

        CREATE TEMP TABLE tmp_employee_changes
        ON COMMIT DROP
        AS
        SELECT
            src.employee_src_id,
            src.employee_name,
            src.employee_position,
            src.employee_salary,
            src.employee_hire_date,
            src.observed_ts,
            src.source_system,
            src.source_table,
            cur.employee_id,
            cur.start_dt AS current_start_dt,
            cur.end_dt   AS current_end_dt,
            cur.is_active,
            CASE
                WHEN cur.employee_id IS NULL THEN 'NEW'
                WHEN cur.employee_name      IS DISTINCT FROM src.employee_name
                  OR cur.employee_position  IS DISTINCT FROM src.employee_position
                  OR cur.employee_salary    IS DISTINCT FROM src.employee_salary
                  OR cur.employee_hire_date IS DISTINCT FROM src.employee_hire_date
                THEN 'CHANGED'
                ELSE 'NO_CHANGE'
            END AS change_type
        FROM tmp_employee_src src
        LEFT JOIN 3nf.nf_employees_scd cur
            ON cur.employee_src_id = src.employee_src_id
          AND cur.is_active = TRUE;

        UPDATE 3nf.nf_employees_scd tgt
        SET
            end_dt    = ch.observed_ts,
            is_active = FALSE,
            update_dt = CURRENT_TIMESTAMP
        FROM tmp_employee_changes ch
        WHERE tgt.employee_src_id = ch.employee_src_id
          AND tgt.is_active = TRUE
          AND ch.change_type = 'CHANGED'
          AND ch.observed_ts > tgt.start_dt;

        GET DIAGNOSTICS v_closed = ROW_COUNT;
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
            COALESCE(ch.employee_id, nextval('3nf.seq_nf_employee_id')),
            ch.employee_src_id,
            ch.employee_name,
            ch.employee_position,
            ch.employee_salary,
            ch.employee_hire_date,
            ch.observed_ts,
            TIMESTAMP '9999-12-31 23:59:59',
            TRUE,
            ch.source_system,
            ch.source_table,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM tmp_employee_changes ch
        WHERE ch.change_type = 'NEW'
          OR (
                ch.change_type = 'CHANGED'
                AND ch.observed_ts > COALESCE(ch.current_start_dt, TIMESTAMP '1900-01-01 00:00:00')
          );
        GET DIAGNOSTICS v_ins = ROW_COUNT;

        PERFORM 3nf.log_etl_event(
            v_proc,
            '3nf.nf_employees_scd',
            v_closed + v_ins,
            CASE WHEN (v_closed + v_ins) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
            'Employee SCD2 load completed. Closed=' || v_closed || ', Inserted=' || v_ins,
            NULL,
            'INFO',
            v_ins,
            0,
            0,
            v_closed,
            NULL
        );

        RAISE NOTICE 'nf_employees_scd completed. closed=%, inserted=%', v_closed, v_ins;
    EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_err_detail = PG_EXCEPTION_DETAIL,
            v_err_hint   = PG_EXCEPTION_HINT;
        v_err_msg := SQLERRM;
        PERFORM 3nf.log_etl_event(
            v_proc,
            '3nf.nf_employees_scd',
            0,
            'FAILED',
            'Employee SCD2 upsert failed',
            v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                      || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
            'ERROR'
        );
        RAISE;
    END;
    $$;




