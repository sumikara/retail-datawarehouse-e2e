
/* =========================================================
   CUSTOMERS
   Keep customer_id_nk for lineage; keep customer_src_id for 3NF.
   Exact duplicate prevention inside source set is handled by
   SELECT DISTINCT. Target-side insert control uses raw customer_id_nk.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_customers';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH unioned_sources AS (
        SELECT
            COALESCE(NULLIF(src.customer_id,''),'n.a.') AS customer_id_nk,
            COALESCE(src.gender,'n.a.') AS gender,
            COALESCE(src.marital_status,'n.a.') AS marital_status,
            src.birth_of_dt,
            src.membership_dt,
            COALESCE(src.customer_zip_code,'n.a.') AS customer_zip_code,
            COALESCE(src.customer_city,'n.a.') AS customer_city,
            COALESCE(src.customer_state,'n.a.') AS customer_state,
            src.last_purchase_dt,

            COALESCE(src.gender,'n.a.') || '-' ||
            COALESCE(src.marital_status,'n.a.') || '-' ||
            COALESCE(src.birth_of_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.membership_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.customer_zip_code,'n.a.') || '-' ||
            COALESCE(src.customer_city,'n.a.') || '-' ||
            COALESCE(src.customer_state,'n.a.') AS customer_src_id,

            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table
        FROM sl_online_retail.src_online_retail src

        UNION ALL

        SELECT
            COALESCE(NULLIF(src.customer_id,''),'n.a.') AS customer_id_nk,
            COALESCE(src.gender,'n.a.') AS gender,
            COALESCE(src.marital_status,'n.a.') AS marital_status,
            src.birth_of_dt,
            src.membership_dt,
            COALESCE(src.customer_zip_code,'n.a.') AS customer_zip_code,
            COALESCE(src.customer_city,'n.a.') AS customer_city,
            COALESCE(src.customer_state,'n.a.') AS customer_state,
            src.last_purchase_dt,

            COALESCE(src.gender,'n.a.') || '-' ||
            COALESCE(src.marital_status,'n.a.') || '-' ||
            COALESCE(src.birth_of_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.membership_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.customer_zip_code,'n.a.') || '-' ||
            COALESCE(src.customer_city,'n.a.') || '-' ||
            COALESCE(src.customer_state,'n.a.') AS customer_src_id,

            'sl_offline_retail' AS source_system,
            'src_offline_retail' AS source_table
        FROM sl_offline_retail.src_offline_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
            customer_id_nk,
            gender,
            marital_status,
            birth_of_dt,
            membership_dt,
            customer_zip_code,
            customer_city,
            customer_state,
            last_purchase_dt,
            customer_src_id,
            source_system,
            source_table
        FROM unioned_sources
        WHERE customer_id_nk <> 'n.a.'
    )
    INSERT INTO stg.mapping_customers (
        customer_id_nk,
        gender,
        marital_status,
        birth_of_dt,
        membership_dt,
        customer_zip_code,
        customer_city,
        customer_state,
        last_purchase_dt,
        customer_src_id,
        source_system,
        source_table
    )
    SELECT
        s.customer_id_nk,
        s.gender,
        s.marital_status,
        s.birth_of_dt,
        s.membership_dt,
        s.customer_zip_code,
        s.customer_city,
        s.customer_state,
        s.last_purchase_dt,
        s.customer_src_id,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_customers tgt
        WHERE tgt.customer_id_nk = s.customer_id_nk
          AND tgt.source_system  = s.source_system
          AND tgt.source_table   = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_customers',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded customer mapping rows using source-set DISTINCT plus raw-customer-id target insert control.'
    );
    RAISE NOTICE 'stg.mapping_customers completed. inserted=%=', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_customers',
        0,
        'FAILED',
        'Customer map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );

    RAISE ;
END;
$$;


/* =========================================================
   STORES
   Preserve standardized store evidence at mapping layer.
   Exact duplicate prevention inside source set is handled by
   SELECT DISTINCT. Target-side insert control uses store_src_id.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc       TEXT := 'stg.load_map_stores';
    v_ins        INT  := 0;
    v_err_msg    TEXT;
    v_err_detail TEXT;
    v_err_hint   TEXT;
BEGIN
    WITH prepared_source AS (
        SELECT
            COALESCE(src.store_location, 'n.a.') || '-' ||
            COALESCE(src.store_city, 'n.a.') || '-' ||
            COALESCE(src.store_state, 'n.a.') AS store_src_id,
            src.store_location AS store_name,
            src.store_zip_code,
            src.store_city,
            src.store_state,
            src.store_location AS store_location_nk,
            'sl_offline_retail'::VARCHAR(100) AS source_system,
            'src_offline_retail'::VARCHAR(100) AS source_table
        FROM sl_offline_retail.src_offline_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
            store_src_id,
            store_name,
            store_zip_code,
            store_city,
            store_state,
            store_location_nk,
            source_system,
            source_table
        FROM prepared_source
        WHERE store_src_id <> 'n.a.-n.a.-n.a.'
    )
    INSERT INTO stg.mapping_stores (
        store_src_id,
        store_name,
        store_zip_code,
        store_city,
        store_state,
        store_location_nk,
        source_system,
        source_table
    )
    SELECT
        s.store_src_id,
        s.store_name,
        s.store_zip_code,
        s.store_city,
        s.store_state,
        s.store_location_nk,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_stores tgt
        WHERE tgt.store_location_nk   = s.store_location_nk
          AND tgt.source_system  = s.source_system
          AND tgt.source_table   = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_stores',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ' store rows using source-set DISTINCT plus raw centric key insert control.'
    );
    
    RAISE NOTICE 'stg.mapping_stores load completed. inserted=%', v_ins;

    

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_stores',
        0,
        'FAILED',
        'Store map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;


/* =========================================================
   PRODUCTS
   Keep product_id_nk + product_src_id.
   Exact duplicate prevention inside the incoming source set is handled by SELECT DISTINCT.
   Target-side insert control is *operationally* (performance-oriented approach) simplified to raw product_id_nk + source_system + source_table.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_products';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH unioned_sources AS (
        SELECT
            COALESCE(NULLIF(src.product_id, ''), 'n.a.') AS product_id_nk,
            COALESCE(src.product_name,'n.a.') || '-' ||
            COALESCE(src.product_category,'n.a.') || '-' ||
            COALESCE(src.product_brand,'n.a.') || '-' ||
            COALESCE(src.product_material,'n.a.') AS product_src_id,
            COALESCE(src.product_category,'n.a.') AS product_category,
            COALESCE(src.product_name,'n.a.') AS product_name,
            COALESCE(src.product_brand,'n.a.') AS product_brand,
            src.product_stock,
            COALESCE(src.product_material,'n.a.') AS product_material,
            src.product_manufacture_dt,
            src.product_expiry_dt,
            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table
        FROM sl_online_retail.src_online_retail src

        UNION ALL

        SELECT
            COALESCE(NULLIF(src.product_id, ''), 'n.a.') AS product_id_nk,
            COALESCE(src.product_name,'n.a.') || '-' ||
            COALESCE(src.product_category,'n.a.') || '-' ||
            COALESCE(src.product_brand,'n.a.') || '-' ||
            COALESCE(src.product_material,'n.a.') AS product_src_id,
            COALESCE(src.product_category,'n.a.') AS product_category,
            COALESCE(src.product_name,'n.a.') AS product_name,
            COALESCE(src.product_brand,'n.a.') AS product_brand,
            src.product_stock,
            COALESCE(src.product_material,'n.a.') AS product_material,
            src.product_manufacture_dt,
            src.product_expiry_dt,
            'sl_offline_retail' AS source_system,
            'src_offline_retail' AS source_table
        FROM sl_offline_retail.src_offline_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
            product_id_nk,
            product_src_id,
            product_category,
            product_name,
            product_brand,
            product_stock,
            product_material,
            product_manufacture_dt,
            product_expiry_dt,
            source_system,
            source_table
        FROM unioned_sources
        WHERE product_id_nk <> 'n.a.' 
        AND product_id_nk IS NOT NULL
    )
    INSERT INTO stg.mapping_products (
        product_id_nk,
        product_src_id,
        product_category,
        product_name,
        product_brand,
        product_stock,
        product_material,
        product_manufacture_dt,
        product_expiry_dt,
        source_system,
        source_table
    )
    SELECT
        s.product_id_nk,
        s.product_src_id,
        s.product_category,
        s.product_name,
        s.product_brand,
        s.product_stock,
        s.product_material,
        s.product_manufacture_dt,
        s.product_expiry_dt,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_products tgt
        WHERE tgt.product_id_nk = s.product_id_nk
          AND tgt.source_system = s.source_system
          AND tgt.source_table  = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_products',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded product mapping rows using standardized-source deduplication plus raw-product-id target insert control.'
    );

        RAISE NOTICE 'stg.mapping_products completed. inserted=%', v_ins;
EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_products',
        0,
        'FAILED',
        'Product map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );

    RAISE;
END;
$$;


/* =========================================================
   PROMOTIONS
   Exact duplicate prevention inside source set is handled by
   SELECT DISTINCT. Target-side insert control uses promotion_src_id.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_promotions';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH unioned_sources AS (
        SELECT
            COALESCE(NULLIF(src.promotion_id, ''), 'n.a.') AS promotion_id_nk,
            COALESCE(src.promotion_type,'n.a.') AS promotion_type,
            COALESCE(src.promotion_channel,'n.a.') AS promotion_channel,
            src.promotion_start_dt,
            src.promotion_end_dt,
            COALESCE(src.promotion_type,'n.a.') || '-' ||
            COALESCE(src.promotion_id,'n.a.')  AS promotion_src_id,
            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table
        FROM sl_online_retail.src_online_retail src

        UNION ALL

        SELECT
            COALESCE(NULLIF(src.promotion_id, ''), 'n.a.'),
            COALESCE(src.promotion_type,'n.a.'),
            COALESCE(src.promotion_channel,'n.a.'),
            src.promotion_start_dt,
            src.promotion_end_dt,
            COALESCE(src.promotion_type,'n.a.') || '-' ||
            COALESCE(src.promotion_id,'n.a.') ,
            'sl_offline_retail',
            'src_offline_retail'
        FROM sl_offline_retail.src_offline_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
            promotion_id_nk,
            promotion_type,
            promotion_channel,
            promotion_start_dt,
            promotion_end_dt,
            promotion_src_id,
            source_system,
            source_table
        FROM unioned_sources
        WHERE promotion_id_nk <> 'n.a.'
        AND promotion_id_nk IS NOT NULL
    )
    INSERT INTO stg.mapping_promotions (
        promotion_id_nk,
        promotion_type,
        promotion_channel,
        promotion_start_dt,
        promotion_end_dt,
        promotion_src_id,
        source_system,
        source_table
    )
    SELECT
        s.promotion_id_nk,
        s.promotion_type,
        s.promotion_channel,
        s.promotion_start_dt,
        s.promotion_end_dt,
        s.promotion_src_id,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_promotions tgt
        WHERE tgt.promotion_id_nk = s.promotion_id_nk
          AND tgt.source_system    = s.source_system
          AND tgt.source_table     = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_promotions',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded promotion map rows using source-set DISTINCT plus promotion_src_id target insert control.'
    );
    RAISE NOTICE 'stg.mapping_promotions completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_promotions',
        0,
        'FAILED',
        'Promotion map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );

    RAISE;
END;
$$;

/* =========================================================
   DELIVERIES
   Exact duplicate prevention inside source set is handled by
   SELECT DISTINCT. Target-side insert control uses delivery_src_id.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_deliveries()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_deliveries';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH unioned_sources AS (
        SELECT
            COALESCE(NULLIF(src.delivery_id,''),'n.a.') AS delivery_id_nk,
            COALESCE(src.delivery_type,'n.a.') AS delivery_type,
            COALESCE(src.delivery_status,'n.a.') AS delivery_status,
            COALESCE(src.shipping_partner,'n.a.') AS shipping_partner,
            COALESCE(src.delivery_type,'n.a.') || '-' ||
            COALESCE(src.shipping_partner,'n.a.') AS delivery_src_id,
            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table
        FROM sl_online_retail.src_online_retail src

        UNION ALL

        SELECT
            COALESCE(NULLIF(src.delivery_id,''),'n.a.'),
            COALESCE(src.delivery_type,'n.a.'),
            COALESCE(src.delivery_status,'n.a.'),
            COALESCE(src.shipping_partner,'n.a.'),
            COALESCE(src.delivery_type,'n.a.') || '-' ||
            COALESCE(src.shipping_partner,'n.a.'),
            'sl_offline_retail',
            'src_offline_retail'
        FROM sl_offline_retail.src_offline_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
            delivery_id_nk,
            delivery_type,
            delivery_status,
            shipping_partner,
            delivery_src_id,
            source_system,
            source_table
        FROM unioned_sources
        WHERE delivery_id_nk <> 'n.a.'
        AND delivery_id_nk IS NOT NULL
    )
    INSERT INTO stg.mapping_deliveries (
        delivery_id_nk,
        delivery_type,
        delivery_status,
        shipping_partner,
        delivery_src_id,
        source_system,
        source_table
    )
    SELECT
        s.delivery_id_nk,
        s.delivery_type,
        s.delivery_status,
        s.shipping_partner,
        s.delivery_src_id,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_deliveries t
        WHERE t.delivery_id_nk = s.delivery_id_nk
          AND t.source_system   = s.source_system
          AND t.source_table    = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_deliveries',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded delivery map rows using source-set DISTINCT plus delivery_src_id target insert control.'
    );
      RAISE NOTICE 'stg.mapping_deliveries completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_deliveries',
        0,
        'FAILED',
        'Delivery map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );

    RAISE;
END;
$$;


/* =========================================================
   ENGAGEMENTS
   Exact duplicate prevention inside source set is handled by
   SELECT DISTINCT. Target-side insert control uses engagement_id_nk.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_engagements()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_engagements';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH prepared_source AS (
        SELECT
            COALESCE(NULLIF(src.engagement_id,''),'n.a.') AS engagement_id_nk,
            src.customer_support_calls,
            src.website_visits,
            COALESCE(src.website_address,'n.a.') AS website_address,
            COALESCE(src.order_channel,'n.a.') AS order_channel,
            COALESCE(src.customer_support_method,'n.a.') AS customer_support_method,
            COALESCE(src.issue_status,'n.a.') AS issue_status,
            COALESCE(src.app_usage,'n.a.') AS app_usage,
            COALESCE(src.social_media_engagement,'n.a.') AS social_media_engagement,
            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table
        FROM sl_online_retail.src_online_retail src
    ),
    distinct_source AS (
        SELECT DISTINCT
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
            source_table
        FROM prepared_source
        WHERE engagement_id_nk <> 'n.a.'
    )
    INSERT INTO stg.mapping_engagements (
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
        source_table
    )
    SELECT
        s.engagement_id_nk,
        s.customer_support_calls,
        s.website_address,
        s.order_channel,
        s.customer_support_method,
        s.issue_status,
        s.app_usage,
        s.website_visits,
        s.social_media_engagement,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_engagements t
        WHERE t.engagement_id_nk = s.engagement_id_nk
          AND t.source_system    = s.source_system
          AND t.source_table     = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_engagements',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded engagement map rows using source-set DISTINCT plus engagement-id target insert control.'
    );
    RAISE NOTICE 'stg.mapping_engagements completed. inserted=%', v_ins;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_engagements',
        0,
        'FAILED',
        'Engagement map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail,'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint,'n.a.'),
        'ERROR'
    );

    RAISE;
END;
$$;


/* =========================================================
   EMPLOYEES
   SCD2-ready evidence preservation in mapping layer.
   Version-level duplicate prevention is intentionally used.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_employees';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_employee_map_source;

    CREATE TEMP TABLE tmp_employee_map_source (
        employee_src_id     VARCHAR(100),
        employee_name_nk    VARCHAR(100),
        employee_position   VARCHAR(100),
        employee_salary     NUMERIC(10,2),
        employee_hire_date  DATE,
        observed_ts         TIMESTAMP,
        source_system       VARCHAR(100),
        source_table        VARCHAR(100)
    ) ON COMMIT DROP;

    INSERT INTO tmp_employee_map_source
    SELECT DISTINCT
        src.employee_name || '-' || src.employee_hire_date::text AS employee_src_id,
        COALESCE(src.employee_name, 'n.a.')     AS employee_name_nk,
        COALESCE(src.employee_position, 'n.a.') AS employee_position,
        src.employee_salary,
        src.employee_hire_date,
        COALESCE(src.transaction_dt, TIMESTAMP '1900-01-01 00:00:00') AS observed_ts,
        'sl_offline_retail' AS source_system,
        'src_offline_retail' AS source_table
    FROM sl_offline_retail.src_offline_retail src
    WHERE src.employee_name IS NOT NULL
      AND src.employee_hire_date IS NOT NULL;

    IF to_regclass('sl_offline_retail.src_offline_retail_employee_inc') IS NOT NULL THEN
        INSERT INTO tmp_employee_map_source (
            employee_src_id,
            employee_name_nk,
            employee_position,
            employee_salary,
            employee_hire_date,
            observed_ts,
            source_system,
            source_table
        )
        SELECT
            inc.employee_src_id,
            COALESCE(inc.employee_name, 'n.a.')     AS employee_name_nk,
            COALESCE(inc.employee_position, 'n.a.') AS employee_position,
            inc.employee_salary,
            inc.employee_hire_date,
            COALESCE(inc.transaction_dt, TIMESTAMP '1900-01-01 00:00:00') AS observed_ts,
            'sl_offline_retail',
            'src_offline_retail_employee_inc'
        FROM sl_offline_retail.src_offline_retail_employee_inc inc
        WHERE inc.employee_src_id IS NOT NULL;
    END IF;

    INSERT INTO stg.mapping_employees (
        employee_src_id,
        employee_name_nk,
        employee_position,
        employee_salary,
        employee_hire_date,
        observed_ts,
        source_system,
        source_table
    )
    SELECT
        s.employee_src_id,
        s.employee_name_nk,
        s.employee_position,
        s.employee_salary,
        s.employee_hire_date,
        s.observed_ts,
        s.source_system,
        s.source_table
    FROM tmp_employee_map_source s
    WHERE s.employee_src_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM stg.mapping_employees t
        WHERE t.employee_src_id = s.employee_src_id
          AND COALESCE(t.employee_name_nk, 'n.a.') = COALESCE(s.employee_name_nk, 'n.a.')
          AND COALESCE(t.employee_position, 'n.a.') = COALESCE(s.employee_position, 'n.a.')
          AND COALESCE(t.employee_salary, -1) = COALESCE(s.employee_salary, -1)
          AND COALESCE(t.employee_hire_date, DATE '1900-01-01') = COALESCE(s.employee_hire_date, DATE '1900-01-01')
          AND COALESCE(t.observed_ts, TIMESTAMP '1900-01-01 00:00:00') = COALESCE(s.observed_ts, TIMESTAMP '1900-01-01 00:00:00')
          AND t.source_system = s.source_system
          AND t.source_table  = s.source_table
      );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_employees',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded employee mapping rows.'
    );

    RAISE NOTICE 'stg.mapping_employees completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_employees',
        0,
        'FAILED',
        'Employee map load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;



/* =========================================================
   TRANSACTIONS
   This is already transaction-grain, so row_sig is the main
   fast row-based duplicate control.
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_map_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'stg.load_map_transactions';
    v_ins         INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN
    WITH unioned_sources AS (
        /* =========================================================
           ONLINE
           ========================================================= */
        SELECT
            COALESCE(NULLIF(src.transaction_id,''),'n.a.')                AS transaction_id,
            src.transaction_dt,
            src.total_sales,
            COALESCE(src.payment_method,'n.a.')                           AS payment_method,
            src.quantity,
            src.unit_price,
            src.discount_applied,
            COALESCE(src.day_of_week,'n.a.')                              AS day_of_week,
            COALESCE(src.week_of_year, -1)                                AS week_of_year,
            COALESCE(src.month_of_year, -1)                               AS month_of_year,

            COALESCE(NULLIF(src.customer_id,''),'n.a.')                   AS customer_id_nk,
            COALESCE(NULLIF(src.product_id,''),'n.a.')                    AS product_id_nk,
            COALESCE(NULLIF(src.promotion_id,''),'n.a.')                  AS promotion_id_nk,
            COALESCE(NULLIF(src.delivery_id,''),'n.a.')                   AS delivery_id_nk,
            COALESCE(NULLIF(src.engagement_id,''),'n.a.')                 AS engagement_id_nk,
            'n.a.'                                                        AS employee_name_nk,
            DATE '1900-01-01'                                             AS employee_hire_date,
            COALESCE(src.customer_city,'n.a.')                            AS customer_city,
            COALESCE(src.customer_state,'n.a.')                           AS customer_state,
            'n.a.'                                                        AS store_zip_code,
            'n.a.'                                                        AS store_city,
            'n.a.'                                                        AS store_state,
            'n.a.'                                                        AS store_location_nk,

            COALESCE(src.product_name,'n.a.')                             AS product_name,
            COALESCE(src.product_category,'n.a.')                         AS product_category,
            COALESCE(src.product_brand,'n.a.')                            AS product_brand,
            COALESCE(src.product_material,'n.a.')                         AS product_material,

            COALESCE(src.promotion_type,'n.a.')                           AS promotion_type,
            COALESCE(src.promotion_channel,'n.a.')                        AS promotion_channel,
            src.promotion_start_dt,
            src.promotion_end_dt,

            COALESCE(src.delivery_type,'n.a.')                            AS delivery_type,
            COALESCE(src.shipping_partner,'n.a.')                         AS shipping_partner,

            COALESCE(src.gender,'n.a.') || '-' ||
            COALESCE(src.marital_status,'n.a.') || '-' ||
            COALESCE(src.birth_of_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.membership_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.customer_zip_code,'n.a.') || '-' ||
            COALESCE(src.customer_city,'n.a.') || '-' ||
            COALESCE(src.customer_state,'n.a.')                           AS customer_src_id,

            COALESCE(src.product_name,'n.a.') || '-' ||
            COALESCE(src.product_category,'n.a.') || '-' ||
            COALESCE(src.product_brand,'n.a.') || '-' ||
            COALESCE(src.product_material,'n.a.')                         AS product_src_id,

            COALESCE(src.promotion_type,'n.a.') || '-' ||
            COALESCE(src.promotion_id,'n.a.')                        AS promotion_src_id,

            COALESCE(src.delivery_type,'n.a.') || '-' ||
            COALESCE(src.shipping_partner,'n.a.')                         AS delivery_src_id,

            'n.a.'                                                        AS store_src_id,

            COALESCE(src.customer_city,'n.a.') || '-' ||
            COALESCE(src.customer_state,'n.a.')                           AS city_src_id,

            'n.a.'                                                        AS employee_src_id,

            md5(concat_ws('|',
                'sl_online_retail',
                'src_online_retail',
                COALESCE(NULLIF(src.transaction_id,''),'n.a.'),
                COALESCE(src.transaction_dt::TEXT,'1900-01-01 00:00:00'),
                COALESCE(NULLIF(src.customer_id,''),'n.a.'),
                COALESCE(NULLIF(src.product_id,''),'n.a.'),
                COALESCE(NULLIF(src.promotion_id,''),'n.a.'),
                COALESCE(NULLIF(src.delivery_id,''),'n.a.'),
                COALESCE(NULLIF(src.engagement_id,''),'n.a.'),
                COALESCE(src.promotion_start_dt::TEXT,'1900-01-01 00:00:00'),
                COALESCE(src.promotion_end_dt::TEXT,'1900-01-01 00:00:00')
            )) AS row_sig,

            'sl_online_retail'                                            AS source_system,
            'src_online_retail'                                           AS source_table
        FROM sl_online_retail.src_online_retail src
        WHERE COALESCE(NULLIF(src.transaction_id,''),'n.a.') <> 'n.a.'
          AND src.transaction_dt IS NOT NULL

        UNION ALL

        /* =========================================================
           OFFLINE
           ========================================================= */
        SELECT
            COALESCE(NULLIF(src.transaction_id,''),'n.a.')                AS transaction_id,
            src.transaction_dt,
            src.total_sales,
            COALESCE(src.payment_method,'n.a.')                           AS payment_method,
            src.quantity,
            src.unit_price,
            src.discount_applied,
            COALESCE(src.day_of_week,'n.a.')                              AS day_of_week,
            COALESCE(src.week_of_year, -1)                                AS week_of_year,
            COALESCE(src.month_of_year, -1)                               AS month_of_year,

            COALESCE(NULLIF(src.customer_id,''),'n.a.')                   AS customer_id_nk,
            COALESCE(NULLIF(src.product_id,''),'n.a.')                    AS product_id_nk,
            COALESCE(NULLIF(src.promotion_id,''),'n.a.')                  AS promotion_id_nk,
            COALESCE(NULLIF(src.delivery_id,''),'n.a.')                   AS delivery_id_nk,
            'n.a.'                                                        AS engagement_id_nk,
            COALESCE(src.employee_name,'n.a.')                            AS employee_name_nk,
            COALESCE(src.employee_hire_date, DATE '1900-01-01')           AS employee_hire_date,
            COALESCE(src.customer_city,'n.a.')                            AS customer_city,
            COALESCE(src.customer_state,'n.a.')                           AS customer_state,
            COALESCE(src.store_zip_code,'n.a.')                           AS store_zip_code,
            COALESCE(src.store_city,'n.a.')                               AS store_city,
            COALESCE(src.store_state,'n.a.')                              AS store_state,
            COALESCE(src.store_location,'n.a.')                           AS store_location_nk,

            COALESCE(src.product_name,'n.a.')                             AS product_name,
            COALESCE(src.product_category,'n.a.')                         AS product_category,
            COALESCE(src.product_brand,'n.a.')                            AS product_brand,
            COALESCE(src.product_material,'n.a.')                         AS product_material,

            COALESCE(src.promotion_type,'n.a.')                           AS promotion_type,
            COALESCE(src.promotion_channel,'n.a.')                        AS promotion_channel,
            src.promotion_start_dt,
            src.promotion_end_dt,

            COALESCE(src.delivery_type,'n.a.')                            AS delivery_type,
            COALESCE(src.shipping_partner,'n.a.')                         AS shipping_partner,

            COALESCE(src.gender,'n.a.') || '-' ||
            COALESCE(src.marital_status,'n.a.') || '-' ||
            COALESCE(src.birth_of_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.membership_dt::TEXT,'n.a.') || '-' ||
            COALESCE(src.customer_zip_code,'n.a.') || '-' ||
            COALESCE(src.customer_city,'n.a.') || '-' ||
            COALESCE(src.customer_state,'n.a.')                           AS customer_src_id,

            COALESCE(src.product_name,'n.a.') || '-' ||
            COALESCE(src.product_category,'n.a.') || '-' ||
            COALESCE(src.product_brand,'n.a.') || '-' ||
            COALESCE(src.product_material,'n.a.')                         AS product_src_id,

            COALESCE(src.promotion_type,'n.a.') || '-' ||
            COALESCE(src.promotion_id,'n.a.')                        AS promotion_src_id,

            COALESCE(src.delivery_type,'n.a.') || '-' ||
            COALESCE(src.shipping_partner,'n.a.')                         AS delivery_src_id,

            CASE
                WHEN COALESCE(src.store_location,'n.a.') <> 'n.a.'
                THEN COALESCE(src.store_location,'n.a.') || '-' ||
                     COALESCE(src.store_city,'n.a.') || '-' ||
                     COALESCE(src.store_state,'n.a.')
                ELSE 'n.a.'
            END                                                           AS store_src_id,

            CASE
                WHEN COALESCE(src.store_city,'n.a.') <> 'n.a.'
                 AND COALESCE(src.store_state,'n.a.') <> 'n.a.'
                THEN COALESCE(src.store_city,'n.a.') || '-' ||
                     COALESCE(src.store_state,'n.a.')
                ELSE COALESCE(src.customer_city,'n.a.') || '-' ||
                     COALESCE(src.customer_state,'n.a.')
            END                                                           AS city_src_id,

            CASE
                WHEN COALESCE(src.employee_name,'n.a.') <> 'n.a.'
                THEN COALESCE(src.employee_name,'n.a.') || '-' ||
                     COALESCE(src.employee_hire_date::TEXT, '1900-01-01')
                ELSE 'n.a.'
            END                                                           AS employee_src_id,

            md5(concat_ws('|',
                'sl_offline_retail',
                'src_offline_retail',

                COALESCE(NULLIF(src.transaction_id,''),'n.a.'),
                COALESCE(src.transaction_dt::TEXT,'1900-01-01 00:00:00'),
                COALESCE(NULLIF(src.customer_id,''),'n.a.'),
                COALESCE(NULLIF(src.product_id,''),'n.a.'),
                COALESCE(NULLIF(src.promotion_id,''),'n.a.'),
                COALESCE(NULLIF(src.delivery_id,''),'n.a.'),
                COALESCE(src.employee_name,'n.a.'),
                COALESCE(src.promotion_start_dt::TEXT,'1900-01-01 00:00:00'),
                COALESCE(src.promotion_end_dt::TEXT,'1900-01-01 00:00:00')
            )) AS row_sig,

            'sl_offline_retail'                                           AS source_system,
            'src_offline_retail'                                          AS source_table
        FROM sl_offline_retail.src_offline_retail src
        WHERE COALESCE(NULLIF(src.transaction_id,''),'n.a.') <> 'n.a.'
          AND src.transaction_dt IS NOT NULL
    ),
    distinct_source AS (
        SELECT DISTINCT ON (row_sig)
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
            customer_id_nk,
            product_id_nk,
            promotion_id_nk,
            delivery_id_nk,
            engagement_id_nk,
            employee_name_nk,
            employee_hire_date,
            customer_city,
            customer_state,
            store_zip_code,
            store_city,
            store_state,
            store_location_nk,
            product_name,
            product_category,
            product_brand,
            product_material,
            promotion_type,
            promotion_channel,
            promotion_start_dt,
            promotion_end_dt,
            delivery_type,
            shipping_partner,
            customer_src_id,
            product_src_id,
            promotion_src_id,
            delivery_src_id,
            store_src_id,
            city_src_id,
            employee_src_id,
            row_sig,
            source_system,
            source_table
        FROM unioned_sources
        ORDER BY row_sig, transaction_dt DESC NULLS LAST
    )
    INSERT INTO stg.mapping_transactions (
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
        customer_id_nk,
        product_id_nk,
        promotion_id_nk,
        delivery_id_nk,
        engagement_id_nk,
        employee_name_nk,
        employee_hire_date,
        customer_city,
        customer_state,
        store_zip_code,
        store_city,
        store_state,
        store_location_nk,
        product_name,
        product_category,
        product_brand,
        product_material,
        promotion_type,
        promotion_channel,
        promotion_start_dt,
        promotion_end_dt,
        delivery_type,
        shipping_partner,
        customer_src_id,
        product_src_id,
        promotion_src_id,
        delivery_src_id,
        store_src_id,
        city_src_id,
        employee_src_id,
        row_sig,
        source_system,
        source_table
    )
    SELECT
        s.transaction_id,
        s.transaction_dt,
        s.total_sales,
        s.payment_method,
        s.quantity,
        s.unit_price,
        s.discount_applied,
        s.day_of_week,
        s.week_of_year,
        s.month_of_year,
        s.customer_id_nk,
        s.product_id_nk,
        s.promotion_id_nk,
        s.delivery_id_nk,
        s.engagement_id_nk,
        s.employee_name_nk,
        s.employee_hire_date,
        s.customer_city,
        s.customer_state,
        s.store_zip_code,
        s.store_city,
        s.store_state,
        s.store_location_nk,
        s.product_name,
        s.product_category,
        s.product_brand,
        s.product_material,
        s.promotion_type,
        s.promotion_channel,
        s.promotion_start_dt,
        s.promotion_end_dt,
        s.delivery_type,
        s.shipping_partner,
        s.customer_src_id,
        s.product_src_id,
        s.promotion_src_id,
        s.delivery_src_id,
        s.store_src_id,
        s.city_src_id,
        s.employee_src_id,
        s.row_sig,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_transactions tgt
        WHERE tgt.row_sig = s.row_sig
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_transactions',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded transaction rows using full standardized row_sig for fast row-based duplicate prevention.',
        NULL,
        'INFO',
        v_ins,
        0,
        0,
        0,
        NULL
    );

    RAISE NOTICE 'stg.mapping_transactions (transaction-grain) completed. inserted=%', v_ins;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_transactions',
        0,
        'FAILED',
        'Transaction map load failed',
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
