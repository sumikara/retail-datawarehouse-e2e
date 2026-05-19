

CREATE OR REPLACE PROCEDURE stg.load_dim_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_customers';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_dim_customers;

    CREATE TEMP TABLE tmp_dim_customers
    ON COMMIT DROP
    AS
    SELECT
        c.customer_id AS customer_src_id,
        c.gender,
        c.marital_status,
        c.birth_of_dt,
        c.membership_dt,
        c.last_purchase_dt,
        COALESCE(a.zip_code, 'n.a.')     AS customer_zip_code,
        COALESCE(ci.city_name, 'n.a.')   AS customer_city,
        COALESCE(st.state_name, 'n.a.')  AS customer_state,
        'nf'::VARCHAR(100)               AS source_system,
        'nf_customers'::VARCHAR(100)     AS source_table
    FROM nf.nf_customers c
    LEFT JOIN nf.nf_addresses a
        ON a.address_id = c.address_id
    LEFT JOIN nf.nf_cities ci
        ON ci.city_id = a.city_id
    LEFT JOIN nf.nf_states st
        ON st.state_id = ci.state_id
    WHERE c.customer_id <> -1;

    CREATE INDEX idx_tmp_dim_customers_src
        ON tmp_dim_customers (customer_src_id);

    INSERT INTO dim.dim_customers (
        customer_surr_id,
        customer_src_id,
        source_system,
        source_table,
        gender,
        marital_status,
        birth_of_dt,
        membership_dt,
        last_purchase_dt,
        customer_zip_code,
        customer_city,
        customer_state,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_customer_id'),
        t.customer_src_id,
        t.source_system,
        t.source_table,
        t.gender,
        t.marital_status,
        t.birth_of_dt,
        t.membership_dt,
        t.last_purchase_dt,
        t.customer_zip_code,
        t.customer_city,
        t.customer_state,
        NOW(),
        NOW()
    FROM tmp_dim_customers t
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_customers d
        WHERE d.customer_src_id = t.customer_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE dim.dim_customers d
    SET
        source_system     = t.source_system,
        source_table      = t.source_table,
        gender            = t.gender,
        marital_status    = t.marital_status,
        birth_of_dt       = t.birth_of_dt,
        membership_dt     = t.membership_dt,
        last_purchase_dt  = t.last_purchase_dt,
        customer_zip_code = t.customer_zip_code,
        customer_city     = t.customer_city,
        customer_state    = t.customer_state,
        update_dt         = NOW()
    FROM tmp_dim_customers t
    WHERE d.customer_src_id = t.customer_src_id
      AND (
            d.source_system     IS DISTINCT FROM t.source_system
         OR d.source_table      IS DISTINCT FROM t.source_table
         OR d.gender            IS DISTINCT FROM t.gender
         OR d.marital_status    IS DISTINCT FROM t.marital_status
         OR d.birth_of_dt       IS DISTINCT FROM t.birth_of_dt
         OR d.membership_dt     IS DISTINCT FROM t.membership_dt
         OR d.last_purchase_dt  IS DISTINCT FROM t.last_purchase_dt
         OR d.customer_zip_code IS DISTINCT FROM t.customer_zip_code
         OR d.customer_city     IS DISTINCT FROM t.customer_city
         OR d.customer_state    IS DISTINCT FROM t.customer_state
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_customers',
        v_ins + v_upd,
        CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd ||
        '. Source=nf.nf_customers + nf_addresses + nf_cities + nf_states.',
        NULL,
        'INFO',
        v_ins,
        v_upd,
        0,
        0,
        NULL
    );

RAISE NOTICE 'dim.dim_customers completed. inserted=%, updated=%', v_ins, v_upd;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_customers',
        0,
        'FAILED',
        'dim_customers load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_stores';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_dim_stores;

    CREATE TEMP TABLE tmp_dim_stores
    ON COMMIT DROP
    AS
    SELECT
        s.store_id AS store_src_id,
        COALESCE(s.store_name, 'n.a.')          AS store_name,
        COALESCE(a.zip_code, 'n.a.')            AS store_zip_code,
        COALESCE(c.city_name, 'n.a.')           AS store_city,
        COALESCE(st.state_name, 'n.a.')         AS store_state,
        COALESCE(s.store_location_nk, 'n.a.')   AS store_location,
        'nf'::VARCHAR(100)                      AS source_system,
        'nf_stores'::VARCHAR(100)               AS source_table
    FROM nf.nf_stores s
    LEFT JOIN nf.nf_addresses a
        ON a.address_id = s.address_id
    LEFT JOIN nf.nf_cities c
        ON c.city_id = a.city_id
    LEFT JOIN nf.nf_states st
        ON st.state_id = c.state_id
    WHERE s.store_id <> -1;

    CREATE INDEX idx_tmp_dim_stores_src
        ON tmp_dim_stores (store_src_id);

    INSERT INTO dim.dim_stores (
        store_surr_id,
        store_src_id,
        source_system,
        source_table,
        store_name,
        store_zip_code,
        store_city,
        store_state,
        store_location,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_store_id'),
        t.store_src_id,
        t.source_system,
        t.source_table,
        t.store_name,
        t.store_zip_code,
        t.store_city,
        t.store_state,
        t.store_location,
        NOW(),
        NOW()
    FROM tmp_dim_stores t
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_stores d
        WHERE d.store_src_id = t.store_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_stores',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Source=nf.nf_stores + nf_addresses + nf_cities + nf_states.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_stores',
        0,
        'FAILED',
        'dim_stores load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_products';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_dim_products;

    CREATE TEMP TABLE tmp_dim_products
    ON COMMIT DROP
    AS
    SELECT
        p.product_id AS product_src_id,
        'nf'::VARCHAR(100)               AS source_system,
        'nf_products'::VARCHAR(100)      AS source_table,
        COALESCE(pc.product_category_name, 'n.a.') AS product_category,
        COALESCE(p.product_name, 'n.a.') AS product_name,
        COALESCE(p.product_brand, 'n.a.') AS product_brand,
        p.product_stock,
        COALESCE(p.product_material, 'n.a.') AS product_material,
        p.product_manufacture_dt,
        p.product_expiry_dt
    FROM nf.nf_products p
    LEFT JOIN nf.nf_product_categories pc
        ON pc.product_category_id = p.product_category_id
    WHERE p.product_id <> -1;

    CREATE INDEX idx_tmp_dim_products_src
        ON tmp_dim_products (product_src_id);

    INSERT INTO dim.dim_products (
        product_surr_id,
        product_src_id,
        source_system,
        source_table,
        product_category,
        product_name,
        product_brand,
        product_stock,
        product_material,
        product_manufacture_dt,
        product_expiry_dt,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_product_id'),
        t.product_src_id,
        t.source_system,
        t.source_table,
        t.product_category,
        t.product_name,
        t.product_brand,
        t.product_stock,
        t.product_material,
        t.product_manufacture_dt,
        t.product_expiry_dt,
        NOW(),
        NOW()
    FROM tmp_dim_products t
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_products d
        WHERE d.product_src_id = t.product_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE dim.dim_products d
    SET
        source_system          = t.source_system,
        source_table           = t.source_table,
        product_category       = t.product_category,
        product_name           = t.product_name,
        product_brand          = t.product_brand,
        product_stock          = t.product_stock,
        product_material       = t.product_material,
        product_manufacture_dt = t.product_manufacture_dt,
        product_expiry_dt      = t.product_expiry_dt,
        update_dt              = NOW()
    FROM tmp_dim_products t
    WHERE d.product_src_id = t.product_src_id
      AND (
            d.source_system          IS DISTINCT FROM t.source_system
         OR d.source_table           IS DISTINCT FROM t.source_table
         OR d.product_category       IS DISTINCT FROM t.product_category
         OR d.product_name           IS DISTINCT FROM t.product_name
         OR d.product_brand          IS DISTINCT FROM t.product_brand
         OR d.product_stock          IS DISTINCT FROM t.product_stock
         OR d.product_material       IS DISTINCT FROM t.product_material
         OR d.product_manufacture_dt IS DISTINCT FROM t.product_manufacture_dt
         OR d.product_expiry_dt      IS DISTINCT FROM t.product_expiry_dt
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_products',
        v_ins + v_upd,
        CASE WHEN (v_ins + v_upd) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd ||
        '. Source=nf.nf_products + nf_product_categories.',
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
        'dim.dim_products',
        0,
        'FAILED',
        'dim_products load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_promotions';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_dim_promotions;

    CREATE TEMP TABLE tmp_dim_promotions
    ON COMMIT DROP
    AS
    SELECT
        p.promotion_id AS promotion_src_id,
        'nf'::VARCHAR(100)               AS source_system,
        'nf_promotions'::VARCHAR(100)    AS source_table,
        COALESCE(p.promotion_channel, 'n.a.')       AS promotion_channel,
        COALESCE(pt.promotion_type_name, 'n.a.')    AS promotion_type,
        p.promotion_start_dt,
        p.promotion_end_dt
    FROM nf.nf_promotions p
    LEFT JOIN nf.nf_promotion_types pt
        ON pt.promotion_type_id = p.promotion_type_id
    WHERE p.promotion_id <> -1;

    CREATE INDEX idx_tmp_dim_promotions_src
        ON tmp_dim_promotions (promotion_src_id);

    INSERT INTO dim.dim_promotions (
        promotion_surr_id,
        promotion_src_id,
        source_system,
        source_table,
        promotion_channel,
        promotion_type,
        promotion_start_dt,
        promotion_end_dt,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_promotion_id'),
        t.promotion_src_id,
        t.source_system,
        t.source_table,
        t.promotion_channel,
        t.promotion_type,
        t.promotion_start_dt,
        t.promotion_end_dt,
        NOW(),
        NOW()
    FROM tmp_dim_promotions t
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_promotions d
        WHERE d.promotion_src_id = t.promotion_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_promotions',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Source=nf.nf_promotions + nf_promotion_types.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_promotions',
        0,
        'FAILED',
        'dim_promotions load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_deliveries()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_deliveries';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_dim_deliveries;

    CREATE TEMP TABLE tmp_dim_deliveries
    ON COMMIT DROP
    AS
    SELECT
        d.delivery_id AS delivery_src_id,
        'nf'::VARCHAR(100)                AS source_system,
        'nf_deliveries'::VARCHAR(100)     AS source_table,
        COALESCE(d.delivery_type, 'n.a.') AS delivery_type,
        COALESCE(d.delivery_status, 'n.a.') AS delivery_status,
        COALESCE(sp.shipping_partner_name, 'n.a.') AS shipping_partner
    FROM nf.nf_deliveries d
    LEFT JOIN nf.nf_shipping_partners sp
        ON sp.shipping_partner_id = d.shipping_partner_id
    WHERE d.delivery_id <> -1;

    CREATE INDEX idx_tmp_dim_deliveries_src
        ON tmp_dim_deliveries (delivery_src_id);

    INSERT INTO dim.dim_deliveries (
        delivery_surr_id,
        delivery_src_id,
        source_system,
        source_table,
        delivery_type,
        delivery_status,
        shipping_partner,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_delivery_id'),
        t.delivery_src_id,
        t.source_system,
        t.source_table,
        t.delivery_type,
        t.delivery_status,
        t.shipping_partner,
        NOW(),
        NOW()
    FROM tmp_dim_deliveries t
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_deliveries d
        WHERE d.delivery_src_id = t.delivery_src_id
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_deliveries',
        v_ins ,
        CASE WHEN (v_ins ) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Source=nf.nf_deliveries + nf_shipping_partners.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_deliveries',
        0,
        'FAILED',
        'dim_deliveries load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_engagements()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_engagements';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    INSERT INTO dim.dim_engagements (
        engagement_surr_id,
        engagement_src_id,
        source_system,
        source_table,
        customer_support_calls,
        website_address,
        order_channel,
        customer_support_method,
        issue_status,
        app_usage,
        website_visits,
        social_media_engagement,
        insert_dt,
        update_dt
    )
    SELECT
        nextval('dim.seq_dim_engagement_id'),
        e.engagement_id,
        'nf',
        'nf_engagements',
        COALESCE(e.customer_support_calls, 0),
        e.website_address,
        e.order_channel,
        e.customer_support_method,
        e.issue_status,
        e.app_usage,
        COALESCE(e.website_visits, 0),
        e.social_media_engagement,
        NOW(),
        NOW()
    FROM nf.nf_engagements e
    WHERE e.engagement_id <> -1
      AND NOT EXISTS (
          SELECT 1
          FROM dim.dim_engagements d
          WHERE d.engagement_src_id = e.engagement_id
      );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_engagements',
        v_ins ,
        CASE WHEN (v_ins) > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||  '. Source=nf.nf_engagements.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        NULL
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_engagements',
        0,
        'FAILED',
        'dim_engagements load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,NULL
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_employees_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_employees_scd';
    v_ins         INT  := 0;
    v_upd         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    /* 1) First synchronize existing DIM versions with NF
          so old active rows become inactive before new active rows are inserted */
    UPDATE dim.dim_employees_scd d
    SET
        employee_name      = e.employee_name_nk,
        employee_position  = e.employee_position,
        employee_salary    = e.employee_salary,
        employee_hire_date = e.employee_hire_date,
        end_dt             = e.end_dt,
        is_active          = e.is_active,
        source_system      = 'nf',
        source_table       = 'nf_employees_scd',
        update_dt          = NOW()
    FROM nf.nf_employees_scd e
    WHERE d.employee_src_id = e.employee_src_id
      AND d.start_dt = e.start_dt
      AND e.employee_id <> -1
      AND (
            d.employee_name      IS DISTINCT FROM e.employee_name_nk
         OR d.employee_position  IS DISTINCT FROM e.employee_position
         OR d.employee_salary    IS DISTINCT FROM e.employee_salary
         OR d.employee_hire_date IS DISTINCT FROM e.employee_hire_date
         OR d.end_dt             IS DISTINCT FROM e.end_dt
         OR d.is_active          IS DISTINCT FROM e.is_active
         OR d.source_system      IS DISTINCT FROM 'nf'
         OR d.source_table       IS DISTINCT FROM 'nf_employees_scd'
      );

    GET DIAGNOSTICS v_upd = ROW_COUNT;

    /* 2) Then insert only missing DIM versions */
    INSERT INTO dim.dim_employees_scd (
        employee_surr_id,
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
        nextval('dim.seq_dim_employee_id'),
        e.employee_src_id,
        e.employee_name_nk,
        e.employee_position,
        e.employee_salary,
        e.employee_hire_date,
        e.start_dt,
        e.end_dt,
        e.is_active,
        'nf',
        'nf_employees_scd',
        NOW(),
        NOW()
    FROM nf.nf_employees_scd e
    LEFT JOIN dim.dim_employees_scd d
        ON d.employee_src_id = e.employee_src_id
       AND d.start_dt = e.start_dt
    WHERE e.employee_id <> -1
      AND d.employee_surr_id IS NULL;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_employees_scd',
        v_ins + v_upd,
        CASE WHEN v_ins + v_upd > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ', Updated=' || v_upd || '. Mirrored employee SCD history from NF to DIM.'
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_employees_scd',
        0,
        'FAILED',
        'dim_employees_scd load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE stg.load_dim_dates(
    p_start_date DATE,
    p_end_date   DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_dim_dates';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    INSERT INTO dim.dim_dates (
        date_surr_id,
        full_date,
        day_of_month,
        month_of_year,
        year_of_date,
        quarter_of_year,
        week_of_year,
        day_name,
        month_name,
        is_weekend,
        is_month_start,
        is_month_end,
        is_quarter_start,
        is_quarter_end,
        is_year_start,
        is_year_end,
        insert_dt
    )
    SELECT
        TO_CHAR(d::date, 'YYYYMMDD')::BIGINT,
        d::date,
        EXTRACT(DAY FROM d)::INT,
        EXTRACT(MONTH FROM d)::INT,
        EXTRACT(YEAR FROM d)::INT,
        EXTRACT(QUARTER FROM d)::INT,
        EXTRACT(WEEK FROM d)::INT,
        TRIM(TO_CHAR(d::date, 'Day')),
        TRIM(TO_CHAR(d::date, 'Month')),
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = date_trunc('month', d)::date THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = (date_trunc('month', d) + interval '1 month - 1 day')::date THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = date_trunc('quarter', d)::date THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = (date_trunc('quarter', d) + interval '3 month - 1 day')::date THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = make_date(EXTRACT(YEAR FROM d)::INT, 1, 1) THEN TRUE ELSE FALSE END,
        CASE WHEN d::date = make_date(EXTRACT(YEAR FROM d)::INT, 12, 31) THEN TRUE ELSE FALSE END,
        NOW()
    FROM generate_series(p_start_date, p_end_date, interval '1 day') AS gs(d)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_dates dd
        WHERE dd.full_date = gs.d::date
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_dates',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded date rows from ' || p_start_date || ' to ' || p_end_date || '.'
    );

    RAISE NOTICE 'Inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.dim_dates',
        0,
        'FAILED',
        'dim_dates load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;
