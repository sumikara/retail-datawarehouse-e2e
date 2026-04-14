CREATE OR REPLACE PROCEDURE stg.load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_states';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH src_states AS (
        SELECT customer_state AS state_name
        FROM stg.mapping_customers
        UNION ALL
        SELECT store_state
        FROM stg.mapping_stores
    ),
    distinct_states AS (
        SELECT DISTINCT state_name
        FROM src_states
        WHERE COALESCE(state_name, 'n.a.') <> 'n.a.'
    )
    INSERT INTO nf.nf_states (
        state_id,
        state_src_id,
        state_name,
        source_system,
        source_table
    )
    SELECT
        nextval('nf.seq_nf_state_id'),
        s.state_name,
        s.state_name,
        'stg',
        'mapping_customers+mapping_stores'
    FROM distinct_states s
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_states tgt
        WHERE tgt.state_src_id = s.state_name
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_states',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded distinct states from mapping_customers and mapping_stores.'
    );

        RAISE NOTICE 'nf.nf_states completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_states',
        0,
        'FAILED',
        'State load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_cities';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH src AS (
        SELECT customer_city AS city_name, customer_state AS state_name
        FROM stg.mapping_customers
        UNION ALL
        SELECT store_city, store_state
        FROM stg.mapping_stores
    ),
    distinct_cities AS (
        SELECT DISTINCT
            city_name,
            state_name,
            city_name || '-' || state_name AS city_src_id
        FROM src
        WHERE COALESCE(city_name, 'n.a.') <> 'n.a.'
          AND COALESCE(state_name, 'n.a.') <> 'n.a.'
    ),
    resolved_state AS (
        SELECT
            d.city_src_id,
            d.city_name,
            COALESCE(s.state_id, -1) AS state_id
        FROM distinct_cities d
        LEFT JOIN nf.nf_states s
            ON s.state_src_id = d.state_name
    )
    INSERT INTO nf.nf_cities (
        city_id,
        city_src_id,
        city_name,
        state_id,
        source_system,
        source_table
    )
    SELECT
        nextval('nf.seq_nf_city_id'),
        r.city_src_id,
        r.city_name,
        r.state_id,
        'stg',
        'mapping_customers+mapping_stores'
    FROM resolved_state r
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_cities tgt
        WHERE tgt.city_src_id = r.city_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_cities',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded distinct cities with resolved state FK.'
    );
    
        RAISE NOTICE 'nf.nf_cities completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_cities',
        0,
        'FAILED',
        'City load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_addresses';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH src AS (
        SELECT
            customer_city AS city_name,
            customer_state AS state_name,
            customer_zip_code AS zip_code
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
            city_name,
            state_name,
            COALESCE(zip_code, 'n.a.') AS zip_code,
            city_name || '-' || state_name AS city_src_id,
            city_name || '-' || state_name || '-' || COALESCE(zip_code, 'n.a.') AS address_src_id
        FROM src
        WHERE COALESCE(city_name, 'n.a.') <> 'n.a.'
          AND COALESCE(state_name, 'n.a.') <> 'n.a.'
    ),
    resolved_city AS (
        SELECT
            d.address_src_id,
            d.zip_code,
            COALESCE(c.city_id, -1) AS city_id
        FROM distinct_addresses d
        LEFT JOIN nf.nf_cities c
            ON c.city_src_id = d.city_src_id
    )
    INSERT INTO nf.nf_addresses (
        address_id,
        address_src_id,
        zip_code,
        city_id,
        source_system,
        source_table
    )
    SELECT
        nextval('nf.seq_nf_address_id'),
        r.address_src_id,
        r.zip_code,
        r.city_id,
        'stg',
        'mapping_customers+mapping_stores'
    FROM resolved_city r
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_addresses tgt
        WHERE tgt.address_src_id = r.address_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_addresses',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded distinct addresses with resolved city FK.'
    );

        RAISE NOTICE 'nf.nf_addresses completed. inserted=%', v_ins;
    

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_addresses',
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
    INSERT INTO nf.nf_product_categories (
        product_category_id,
        product_category_src_id,
        product_category_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('nf.seq_nf_product_category_id'),
        s.product_category_src_id,
        s.product_category_name,
        'stg',
        'mapping_products',
        NOW()
    FROM (
        SELECT DISTINCT
            p.product_category AS product_category_src_id,
            p.product_category AS product_category_name
        FROM stg.mapping_products p
        WHERE COALESCE(p.product_category, 'n.a.') <> 'n.a.'
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_product_categories tgt
        WHERE tgt.product_category_src_id = s.product_category_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_product_categories',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded product categories from mapping_products.'
    );
        RAISE NOTICE 'nf.nf_product_categories completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_product_categories',
        0,
        'FAILED',
        'Product category load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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
    INSERT INTO nf.nf_promotion_types (
        promotion_type_id,
        promotion_type_src_id,
        promotion_type_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('nf.seq_nf_promotion_type_id'),
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
        FROM nf.nf_promotion_types tgt
        WHERE tgt.promotion_type_src_id = s.promotion_type_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_promotion_types',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded promotion types from mapping_promotions.'
    );

        RAISE NOTICE 'nf.nf_promotion_types completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_promotion_types',
        0,
        'FAILED',
        'Promotion type load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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
    INSERT INTO nf.nf_shipping_partners (
        shipping_partner_id,
        shipping_partner_src_id,
        shipping_partner_name,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('nf.seq_nf_shipping_partner_id'),
        s.shipping_partner_src_id,
        s.shipping_partner_name,
        'stg',
        'mapping_deliveries',
        NOW()
    FROM (
        SELECT DISTINCT
            d.shipping_partner AS shipping_partner_src_id,
            d.shipping_partner AS shipping_partner_name
        FROM stg.mapping_deliveries d
        WHERE COALESCE(d.shipping_partner, 'n.a.') <> 'n.a.'
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_shipping_partners tgt
        WHERE tgt.shipping_partner_src_id = s.shipping_partner_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_shipping_partners',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded shipping partners from mapping_deliveries.'
    );
        RAISE NOTICE 'nf.nf_shipping_partners completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_shipping_partners',
        0,
        'FAILED',
        'Shipping partner load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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

    CREATE TEMP TABLE tmp_final_customers ON COMMIT DROP AS
    WITH src_customers AS (
        SELECT
            c.customer_src_id,
            c.customer_id_nk,
            c.gender,
            c.marital_status,
            c.birth_of_dt,
            c.membership_dt,
            c.last_purchase_dt,
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
            s.birth_of_dt,
            s.membership_dt,
            s.last_purchase_dt,
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
        LEFT JOIN nf.nf_addresses ad
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
        'stg'::VARCHAR(100) AS source_system,
        'mapping_customers'::VARCHAR(100) AS source_table
    FROM ranked_customers
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_customers_src
        ON tmp_final_customers (customer_src_id);

    INSERT INTO nf.nf_customers (
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
        nextval('nf.seq_nf_customer_id'),
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
        FROM nf.nf_customers ce
        WHERE ce.customer_src_id = f.customer_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE nf.nf_customers ce
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

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_customers',
        v_ins + v_upd,
        CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd || '. Type 1 customer load by customer_src_id.',
        NULL,
        'INFO',
        v_ins,
        v_upd,
        0,
        0,
        NULL
    );

        RAISE NOTICE 'nf.nf_customers completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_customers',
        0,
        'FAILED',
        'Customer load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_stores';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_final_stores;

    CREATE TEMP TABLE tmp_final_stores ON COMMIT DROP AS
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
    resolved_address AS (
        SELECT
            ss.store_src_id,
            ss.store_name,
            ss.store_location_nk,
            COALESCE(a.address_id, -1) AS address_id
        FROM src_stores ss
        LEFT JOIN nf.nf_addresses a
            ON a.address_src_id = ss.address_src_id
    ),
    ranked_stores AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.store_src_id
                ORDER BY r.address_id DESC, r.store_name
            ) AS rn
        FROM resolved_address r
    )
    SELECT
        store_src_id,
        store_name,
        store_location_nk,
        address_id,
        'stg'::VARCHAR(100) AS source_system,
        'mapping_stores'::VARCHAR(100) AS source_table
    FROM ranked_stores
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_stores_src
        ON tmp_final_stores (store_src_id);

    INSERT INTO nf.nf_stores (
        store_id,
        store_src_id,
        store_name,
        store_location_nk,
        address_id,
        source_system,
        source_table,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('nf.seq_nf_store_id'),
        f.store_src_id,
        f.store_name,
        f.store_location_nk,
        f.address_id,
        f.source_system,
        f.source_table,
        NOW(),
        NOW()
    FROM tmp_final_stores f
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_stores ce
        WHERE ce.store_src_id = f.store_src_id
    );


    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_stores',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Type 0 store load by store_src_id.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );
        RAISE NOTICE 'nf.nf_stores completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_stores',
        0,
        'FAILED',
        'Store load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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

    CREATE TEMP TABLE tmp_final_products ON COMMIT DROP AS
    WITH src_products AS (
        SELECT
            p.product_src_id,
            p.product_id_nk,
            p.product_category,
            p.product_name,
            p.product_brand,
            p.product_stock,
            p.product_material,
            p.product_manufacture_dt,
            p.product_expiry_dt
        FROM stg.mapping_products p
        WHERE p.product_src_id <> 'n.a.-n.a.-n.a.-n.a.'
    ),
    category_resolved AS (
        SELECT
            s.product_src_id,
            s.product_id_nk,
            COALESCE(c.product_category_id, -1) AS product_category_id,
            s.product_name,
            s.product_brand,
            s.product_stock,
            s.product_material,
            s.product_manufacture_dt,
            s.product_expiry_dt
        FROM src_products s
        LEFT JOIN nf.nf_product_categories c
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
        product_id_nk,
        product_category_id,
        product_name,
        product_brand,
        product_stock,
        product_material,
        product_manufacture_dt,
        product_expiry_dt,
        'stg'::VARCHAR(100) AS source_system,
        'mapping_products'::VARCHAR(100) AS source_table
    FROM ranked_products
    WHERE rn = 1;

    CREATE INDEX idx_tmp_final_products_src
        ON tmp_final_products (product_src_id);

    INSERT INTO nf.nf_products (
        product_id,
        product_src_id,
        product_id_nk,
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
        nextval('nf.seq_nf_product_id'),
        f.product_src_id,
        f.product_id_nk,
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
        FROM nf.nf_products ce
        WHERE ce.product_src_id = f.product_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE nf.nf_products ce
    SET
        product_id_nk          = f.product_id_nk,
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
            ce.product_id_nk          IS DISTINCT FROM f.product_id_nk
         OR ce.product_category_id    IS DISTINCT FROM f.product_category_id
         OR ce.product_name           IS DISTINCT FROM f.product_name
         OR ce.product_brand          IS DISTINCT FROM f.product_brand
         OR ce.product_stock          IS DISTINCT FROM f.product_stock
         OR ce.product_material       IS DISTINCT FROM f.product_material
         OR ce.product_manufacture_dt IS DISTINCT FROM f.product_manufacture_dt
         OR ce.product_expiry_dt      IS DISTINCT FROM f.product_expiry_dt
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_products',
        v_upd + v_ins,
        CASE WHEN (v_upd + v_ins) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd || '. Type 1 product load by product_src_id.',
        NULL,
        'INFO',
        v_ins,
        v_upd,
        0,
        0,
        NULL
    );
        RAISE NOTICE 'nf.nf_products completed. inserted=%, updated=%', v_ins, v_upd;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_products',
        0,
        'FAILED',
        'Product load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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

    CREATE TEMP TABLE tmp_final_promotions ON COMMIT DROP AS
    WITH src_promotions AS (
        SELECT
            p.promotion_src_id,
            p.promotion_id_nk,
            p.promotion_type,
            p.promotion_channel,
            p.promotion_start_dt,
            p.promotion_end_dt
        FROM stg.mapping_promotions p
        WHERE p.promotion_src_id <> 'n.a.-n.a.-1900-01-01 00:00:00-1900-01-01 00:00:00'
    ),
    ranked AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.promotion_src_id
                ORDER BY
                    s.promotion_start_dt DESC NULLS LAST,
                    s.promotion_end_dt DESC NULLS LAST,
                    s.promotion_type DESC,
                    s.promotion_channel DESC
            ) AS rn
        FROM src_promotions s
    ),
    resolved_type AS (
        SELECT
            r.promotion_src_id,
            r.promotion_id_nk,
            COALESCE(pt.promotion_type_id, -1) AS promotion_type_id,
            r.promotion_channel,
            r.promotion_start_dt,
            r.promotion_end_dt,
            'stg'::VARCHAR(100) AS source_system,
            'mapping_promotions'::VARCHAR(100) AS source_table
        FROM ranked r
        LEFT JOIN nf.nf_promotion_types pt
            ON pt.promotion_type_src_id = r.promotion_type
        WHERE r.rn = 1
    )
    SELECT *
    FROM resolved_type;

    CREATE INDEX idx_tmp_final_promotions_src
        ON tmp_final_promotions (promotion_src_id);

    INSERT INTO nf.nf_promotions (
        promotion_id,
        promotion_src_id,
        promotion_id_nk,
        promotion_type_id,
        promotion_channel,
        promotion_start_dt,
        promotion_end_dt,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('nf.seq_nf_promotion_id'),
        f.promotion_src_id,
        f.promotion_id_nk,
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
        FROM nf.nf_promotions ce
        WHERE ce.promotion_src_id = f.promotion_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_promotions',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||  '. Type 0 Promotion load by promotion_src_id.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );
        RAISE NOTICE 'nf.nf_promotions completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_promotions',
        0,
        'FAILED',
        'Promotion load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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

    CREATE TEMP TABLE tmp_final_deliveries ON COMMIT DROP AS
    WITH src AS (
        SELECT
            d.delivery_src_id,
            d.delivery_id_nk,
            d.shipping_partner,
            d.delivery_type,
            d.delivery_status,
            'stg'::VARCHAR(100) AS source_system,
            'mapping_deliveries'::VARCHAR(100) AS source_table
        FROM stg.mapping_deliveries d
        WHERE d.delivery_src_id <> 'n.a.-n.a.'
    ),
    ranked AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.delivery_src_id
                ORDER BY
                    s.delivery_status,
                    s.delivery_type,
                    s.shipping_partner
            ) AS rn
        FROM src s
    ),
    resolved_partner AS (
        SELECT
            r.delivery_src_id,
            r.delivery_id_nk,
            COALESCE(sp.shipping_partner_id, -1) AS shipping_partner_id,
            r.delivery_type,
            r.delivery_status,
            r.source_system,
            r.source_table
        FROM ranked r
        LEFT JOIN nf.nf_shipping_partners sp
            ON sp.shipping_partner_src_id = r.shipping_partner
        WHERE r.rn = 1
    )
    SELECT *
    FROM resolved_partner;

    CREATE INDEX idx_tmp_final_deliveries_src
        ON tmp_final_deliveries (delivery_src_id);

    INSERT INTO nf.nf_deliveries (
        delivery_id,
        delivery_src_id,
        delivery_id_nk,
        shipping_partner_id,
        delivery_type,
        delivery_status,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        nextval('nf.seq_nf_delivery_id'),
        f.delivery_src_id,
        f.delivery_id_nk,
        f.shipping_partner_id,
        f.delivery_type,
        f.delivery_status,
        f.source_system,
        f.source_table,
        NOW()
    FROM tmp_final_deliveries f
    WHERE NOT EXISTS (
        SELECT 1
        FROM nf.nf_deliveries ce
        WHERE ce.delivery_src_id = f.delivery_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_deliveries',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||  '. Delivery load by delivery_src_id.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );
        RAISE NOTICE 'nf.nf_deliveries completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_deliveries',
        0,
        'FAILED',
        'Delivery load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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

    CREATE TEMP TABLE tmp_final_engagements ON COMMIT DROP AS
    WITH ranked AS (
        SELECT
            e.engagement_id_nk AS engagement_src_id,
            e.engagement_id_nk,
            e.customer_support_calls,
            e.website_address,
            e.order_channel,
            e.customer_support_method,
            e.issue_status,
            e.app_usage,
            e.website_visits,
            e.social_media_engagement,
            ROW_NUMBER() OVER (
                PARTITION BY e.engagement_id_nk
                ORDER BY e.website_visits DESC NULLS LAST
            ) AS rn
        FROM stg.mapping_engagements e
        WHERE e.engagement_id_nk <> 'n.a.'
    )
    SELECT
        engagement_src_id,
        engagement_id_nk,
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

    INSERT INTO nf.nf_engagements (
        engagement_id,
        engagement_src_id,
        engagement_id_nk,
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
        nextval('nf.seq_nf_engagement_id'),
        f.engagement_src_id,
        f.engagement_id_nk,
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
        FROM nf.nf_engagements ce
        WHERE ce.engagement_src_id = f.engagement_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_engagements',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Engagement load by engagement_src_id.'
    );
        RAISE NOTICE 'nf.nf_engagements completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_engagements',
        0,
        'FAILED',
        'Engagement load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


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
            s.employee_name_nk,
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
        WHERE s.employee_src_id IS NOT NULL
          AND s.employee_src_id <> 'n.a.-1900-01-01'
    )
    SELECT
        employee_src_id,
        employee_name_nk,
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
        src.employee_name_nk,
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
            WHEN cur.employee_position IS DISTINCT FROM src.employee_position
              OR cur.employee_salary   IS DISTINCT FROM src.employee_salary
            THEN 'CHANGED'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM tmp_employee_src src
    LEFT JOIN nf.nf_employees_scd cur
        ON cur.employee_src_id = src.employee_src_id
       AND cur.is_active = TRUE;

    UPDATE nf.nf_employees_scd tgt
    SET
        end_dt    = CASE
                        WHEN ch.observed_ts > tgt.start_dt
                        THEN ch.observed_ts
                        ELSE tgt.end_dt
                    END,
        is_active = CASE
                        WHEN ch.observed_ts > tgt.start_dt
                        THEN FALSE
                        ELSE tgt.is_active
                    END,
        update_dt = CURRENT_TIMESTAMP
    FROM tmp_employee_changes ch
    WHERE tgt.employee_src_id = ch.employee_src_id
      AND tgt.is_active = TRUE
      AND ch.change_type = 'CHANGED'
      AND ch.observed_ts > tgt.start_dt;

    GET DIAGNOSTICS v_closed = ROW_COUNT;

    INSERT INTO nf.nf_employees_scd (
        employee_id,
        employee_src_id,
        employee_name_nk,
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
        COALESCE(ch.employee_id, nextval('nf.seq_nf_employee_id')),
        ch.employee_src_id,
        ch.employee_name_nk,
        ch.employee_position,
        ch.employee_salary,
        ch.employee_hire_date,
        CASE
            WHEN ch.observed_ts > COALESCE(ch.current_start_dt, TIMESTAMP '1900-01-01 00:00:00')
            THEN ch.observed_ts
            ELSE CURRENT_TIMESTAMP
        END AS start_dt,
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

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_employees_scd',
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

    RAISE NOTICE 'nf.nf_employees_scd completed. closed=%, inserted=%', v_closed, v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_employees_scd',
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

CREATE OR REPLACE PROCEDURE stg.load_ce_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_ce_transactions';
    v_ins_total   INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    INSERT INTO nf.nf_transactions (
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
        source_table,
        insert_dt
    )
    SELECT
        t.transaction_id,
        t.transaction_dt,
        COALESCE(t.total_sales, 0),
        COALESCE(t.payment_method, 'n.a.'),
        COALESCE(t.quantity, 0),
        COALESCE(t.unit_price, 0),
        COALESCE(t.discount_applied, 0),
        COALESCE(t.day_of_week, 'n.a.'),
        COALESCE(t.week_of_year, -1),
        COALESCE(t.month_of_year, -1),

        COALESCE(st.store_id, -1)      AS store_id,
        COALESCE(cu.customer_id, -1)   AS customer_id,
        COALESCE(pm.promotion_id, -1)  AS promotion_id,
        COALESCE(dv.delivery_id, -1)   AS delivery_id,
        COALESCE(pr.product_id, -1)    AS product_id,
        COALESCE(eg.engagement_id, -1) AS engagement_id,
        COALESCE(ci.city_id, -1)       AS city_id,
        COALESCE(emp.employee_id, -1)  AS employee_id,

        t.row_sig,
        t.source_system,
        t.source_table,
        NOW()

    FROM stg.mapping_transactions t

    LEFT JOIN nf.nf_customers cu
        ON cu.customer_src_id = t.customer_src_id

    LEFT JOIN nf.nf_products pr
        ON pr.product_src_id = t.product_src_id

    LEFT JOIN nf.nf_promotions pm
        ON pm.promotion_src_id = t.promotion_src_id

    LEFT JOIN nf.nf_deliveries dv
        ON dv.delivery_src_id = t.delivery_src_id

    LEFT JOIN nf.nf_engagements eg
        ON eg.engagement_src_id = t.engagement_id_nk

    LEFT JOIN nf.nf_cities ci
        ON ci.city_src_id = t.city_src_id

    LEFT JOIN nf.nf_stores st
        ON st.store_src_id = t.store_src_id

    LEFT JOIN nf.nf_employees_scd emp
        ON emp.employee_src_id = t.employee_src_id
     /* AND t.transaction_dt >= emp.start_dt
       AND t.transaction_dt <  emp.end_dt
       IDEAL WORLD: eefective-date join
       theoratically this is the right way.
       HOWEVER, Because the dataset is synthetic, employee_hire_date,
       transaction timestamps, and observed change timestamps are not
       always temporally consistent. Therefore, for demonstration
       purposes, the transaction-to-employee join uses the current
       active SCD2 row instead of a strict point-in-time effective-date join. */
      AND emp.is_active = TRUE -- demo/synthetic world: active-row join

    WHERE t.transaction_id <> 'n.a.'
      AND t.transaction_dt IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM nf.nf_transactions x
          WHERE x.row_sig = t.row_sig
      );

    GET DIAGNOSTICS v_ins_total = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_transactions',
        v_ins_total,
        CASE WHEN v_ins_total > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins_total || '. Loaded final 3NF transactions from mapping_transactions.',
        NULL,
        'INFO',
        v_ins_total,
        0,
        0,
        0,
        NULL
    );
        RAISE NOTICE 'nf.nf_transactions (factlike/pre-fact) completed. inserted=%', v_ins_total;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'nf.nf_transactions',
        0,
        'FAILED',
        'Transaction load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,
        0,
        0,
        0,
        NULL
    );
    RAISE;
END;
$$;
