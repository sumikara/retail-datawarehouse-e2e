-- states
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc TEXT := 'bl_cl.load_ce_states';
    v_ins INT := 0;
    v_err_msg TEXT;
    v_err_detail TEXT;
    v_err_hint TEXT;
BEGIN
WITH src_states AS (

    SELECT customer_state AS state_name
    FROM bl_cl.t_map_customers
    UNION ALL
    SELECT store_state
    FROM bl_cl.t_map_stores

),
     keep one row per state business key */
distinct_states AS (
    SELECT DISTINCT state_name
    FROM src_states
    WHERE state_name <> 'n.a.'
)
INSERT INTO bl_3nf.ce_states (
    state_id,
    state_src_id,
    state_name,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_ce_state_id'),
    s.state_name,
    s.state_name,
    'bl_cl',
    't_map_customers + t_map_stores'
FROM distinct_states s
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_states tgt
    WHERE tgt.state_src_id = s.state_name
);
    GET DIAGNOSTICS v_ins = ROW_COUNT;
    PERFORM bl_cl.log_etl_event(
        v_proc,
    'bl_3nf.ce_states',
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
    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_states',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc TEXT := 'bl_cl.load_ce_cities';
    v_ins INT := 0;
    v_err_msg TEXT;
    v_err_detail TEXT;
    v_err_hint TEXT;

BEGIN
    WITH src AS (
        SELECT customer_city AS city, customer_state AS state
        FROM bl_cl.t_map_customers
        UNION ALL
        SELECT store_city, store_state
        FROM bl_cl.t_map_stores
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
        LEFT JOIN bl_3nf.ce_states s
            ON s.state_src_id = d.state
    )
INSERT INTO bl_3nf.ce_cities (
    city_id,
    city_src_id,
    city_name,
    state_id,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_ce_city_id'),
    r.city_src_id,
    r.city,
    r.state_id,
    'bl_cl',
    't_map_customers + t_map_stores'
FROM resolved_state r
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_cities tgt
    WHERE tgt.city_src_id = r.city_src_id
);
GET DIAGNOSTICS v_ins = ROW_COUNT;
PERFORM bl_cl.log_etl_event(
    v_proc,
    'bl_3nf.ce_cities',
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
    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_cities',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc       TEXT := 'bl_cl.load_ce_addresses';
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
    FROM bl_cl.t_map_customers

    UNION ALL

    SELECT
        store_city,
        store_state,
        store_zip_code
    FROM bl_cl.t_map_stores

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
    LEFT JOIN bl_3nf.ce_cities c
        ON c.city_src_id = d.city_src_id

)

INSERT INTO bl_3nf.ce_addresses (
    address_id,
    address_src_id,
    zip_code,
    city_id,
    source_system,
    source_table
)
SELECT
    nextval('bl_3nf.seq_ce_address_id'),
    r.address_src_id,
    r.zip,
    r.city_id,
    'bl_cl',
    't_map_customers + t_map_stores'
FROM resolved_city r
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_addresses tgt
    WHERE tgt.address_src_id = r.address_src_id
);

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_addresses',
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

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_addresses',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_customers';
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

        FROM bl_cl.t_map_customers c
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
    LEFT JOIN bl_3nf.ce_addresses ad
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
        'bl_cl' AS source_system,
        't_map_customers' AS source_table
    FROM ranked_customers
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_customers_src
        ON tmp_final_customers (customer_src_id);

    /* ================= INSERT ================= */
    INSERT INTO bl_3nf.ce_customers (
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
        nextval('bl_3nf.seq_ce_customer_id'),
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
        FROM bl_3nf.ce_customers ce
        WHERE ce.customer_src_id = f.customer_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    /* ================= UPDATE (SCD TYPE 1) ================= */
    UPDATE bl_3nf.ce_customers ce
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
    WHERE ce.customer_src_id = f.customer_src_id
      AND (
            ce.customer_id_nk   IS DISTINCT FROM f.customer_id_nk
         OR ce.gender           IS DISTINCT FROM f.gender
         OR ce.marital_status   IS DISTINCT FROM f.marital_status
         OR ce.birth_of_dt      IS DISTINCT FROM f.birth_of_dt
         OR ce.membership_dt    IS DISTINCT FROM f.membership_dt
         OR ce.last_purchase_dt IS DISTINCT FROM f.last_purchase_dt
         OR ce.address_id       IS DISTINCT FROM f.address_id
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM bl_cl.log_etl_event(
    v_proc,
    'bl_3nf.ce_customers',
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

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_customers',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_stores';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    -- Safe rerun in same session
    DROP TABLE IF EXISTS tmp_final_stores;

    -- Build one integrated store row per store business key
    CREATE TEMP TABLE tmp_final_stores
    ON COMMIT DROP
    AS
    WITH src_stores AS (

        SELECT
            s.store_src_id,                 -- already engineered in t_map_stores
            s.store_name,

            -- Address business key (already clean in t_map)
            s.store_city || '-' ||
            s.store_state || '-' ||
            s.store_zip_code AS address_src_id --just for matching

        FROM bl_cl.t_map_stores s
        WHERE s.store_src_id <> 'n.a.'
    ),

    resolved_address AS (

        SELECT
            ss.store_src_id,
            ss.store_name,
            COALESCE(a.address_id, -1) AS address_id

        FROM src_stores ss
        LEFT JOIN bl_3nf.ce_addresses a
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
        'bl_cl' AS source_system,
        't_map_stores' AS source_table
    FROM ranked_stores
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_stores_src
        ON tmp_final_stores (store_src_id);

    -- INSERT
    INSERT INTO bl_3nf.ce_stores (
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
        nextval('bl_3nf.seq_ce_store_id'),
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
        FROM bl_3nf.ce_stores ce
        WHERE ce.store_src_id = f.store_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    -- UPDATE (SCD TYPE 1)
    UPDATE bl_3nf.ce_stores ce
    SET
        store_name    = f.store_name,
        address_id    = f.address_id,
        source_system = f.source_system,
        source_table  = f.source_table,
        update_dt     = NOW()
    FROM tmp_final_stores f
    WHERE ce.store_src_id = f.store_src_id
      AND (
            ce.store_name    IS DISTINCT FROM f.store_name
         OR ce.address_id    IS DISTINCT FROM f.address_id
         OR ce.source_system IS DISTINCT FROM f.source_system
         OR ce.source_table  IS DISTINCT FROM f.source_table
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    -- Log result
    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_stores',
        (v_ins + v_upd),
        CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd ||
        '. Source=bl_cl.t_map_stores; Type 1 load; address FK resolved from ce_addresses.'
    );

    RAISE NOTICE 'ce_stores completed. inserted=%, updated=%', v_ins, v_upd;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_stores',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_product_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_product_categories';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    -- Drop temp table safely for reruns in the same session
    DROP TABLE IF EXISTS tmp_final_product_categories;

    -- Build one unique normalized category set from BL_CL products
    -- DISTINCT is enough because this entity is lookup-like and Type 0
    CREATE TEMP TABLE tmp_final_product_categories
    ON COMMIT DROP
    AS
    SELECT DISTINCT
        p.product_category        AS product_category_src_id,
        p.product_category        AS product_category_name,
        'bl_cl'::VARCHAR(100)     AS source_system,
        't_map_products'::VARCHAR(100) AS source_table
    FROM bl_cl.t_map_products p
    WHERE p.product_category IS NOT NULL
      AND p.product_category <> 'n.a.';

    -- Optional temp index for faster anti-join check
    CREATE INDEX idx_tmp_final_product_categories_src
        ON tmp_final_product_categories (product_category_src_id);

    -- Insert only brand-new category business keys
    INSERT INTO bl_3nf.ce_product_categories (
        product_category_id,
        product_category_src_id,
        product_category_name,
        source_system,
        source_table,
        insert_dt
            )
    SELECT
        nextval('bl_3nf.seq_ce_product_category_id'),
        f.product_category_src_id,
        f.product_category_name,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_product_categories f
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_product_categories ce
        WHERE ce.product_category_src_id = f.product_category_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    -- Log ETL result
   PERFORM bl_cl.log_etl_event(
    v_proc,
    'bl_3nf.ce_product_categories',
    v_ins,
    CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
    'Inserted=' || v_ins ||
    '. Loaded distinct product_categories from bl_cl.t_map_products.',
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

    -- Log failure
    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_product_categories',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_products';
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
        FROM bl_cl.t_map_products p
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
        LEFT JOIN bl_3nf.ce_product_categories c
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
        'bl_cl' AS source_system,
        't_map_products' AS source_table
    FROM ranked_products
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_products_src
        ON tmp_final_products (product_src_id);

    INSERT INTO bl_3nf.ce_products (
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
        nextval('bl_3nf.seq_ce_product_id'),
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
        FROM bl_3nf.ce_products ce
        WHERE ce.product_src_id = f.product_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE bl_3nf.ce_products ce
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
    WHERE ce.product_src_id = f.product_src_id
      AND (
            ce.product_category_id    IS DISTINCT FROM f.product_category_id
         OR ce.product_name           IS DISTINCT FROM f.product_name
         OR ce.product_brand          IS DISTINCT FROM f.product_brand
         OR ce.product_stock          IS DISTINCT FROM f.product_stock
         OR ce.product_material       IS DISTINCT FROM f.product_material
         OR ce.product_manufacture_dt IS DISTINCT FROM f.product_manufacture_dt
         OR ce.product_expiry_dt      IS DISTINCT FROM f.product_expiry_dt
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_products',
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

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_products',
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
CREATE OR REPLACE PROCEDURE bl_cl.load_ce_promotion_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_promotion_types';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    INSERT INTO bl_3nf.ce_promotion_types (
        promotion_type_id,
        promotion_type_src_id,
        promotion_type_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('bl_3nf.seq_ce_promotion_type_id'),
        s.promotion_type_src_id,
        s.promotion_type_name,
        'bl_cl',
        't_map_promotions',
        NOW()
    FROM (
        SELECT DISTINCT
            p.promotion_type AS promotion_type_src_id,
            p.promotion_type AS promotion_type_name
        FROM bl_cl.t_map_promotions p
        WHERE COALESCE(p.promotion_type, 'n.a.') <> 'n.a.'
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_promotion_types t
        WHERE t.promotion_type_src_id = s.promotion_type_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_promotion_types',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded promotion types from bl_cl.t_map_promotions.'
    );
            RAISE NOTICE 'ce_promotion_types completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM bl_cl.log_etl_event(
        v_proc,
        'bl_3nf.ce_promotion_types',
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





