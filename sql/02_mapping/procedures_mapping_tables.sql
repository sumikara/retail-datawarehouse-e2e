-- customers
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
    /* ONLINE */
    SELECT
        COALESCE(NULLIF(src.customer_id,''),'n.a.') AS customer_id,
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
        COALESCE(src.birth_of_dt::text,'n.a.') || '-' ||
        COALESCE(src.membership_dt::text,'n.a.') || '-' ||
        COALESCE(src.customer_zip_code,'n.a.') || '-' ||
        COALESCE(src.customer_city,'n.a.') || '-' ||
        COALESCE(src.customer_state,'n.a.') AS customer_src_id,
        'sl_online_retail' AS source_system,
        'src_online_retail' AS source_table
    FROM sl_online_retail.src_online_retail src

    UNION ALL

    /* OFFLINE */
    SELECT
        COALESCE(NULLIF(src.customer_id,''),'n.a.'),
        COALESCE(src.gender,'n.a.'),
        COALESCE(src.marital_status,'n.a.'),
        src.birth_of_dt,
        src.membership_dt,
        COALESCE(src.customer_zip_code,'n.a.'),
        COALESCE(src.customer_city,'n.a.'),
        COALESCE(src.customer_state,'n.a.'),
        src.last_purchase_dt,
        COALESCE(src.gender,'n.a.') || '-' ||
        COALESCE(src.marital_status,'n.a.') || '-' ||
        COALESCE(src.birth_of_dt::text,'n.a.') || '-' ||
        COALESCE(src.membership_dt::text,'n.a.') || '-' ||
        COALESCE(src.customer_zip_code,'n.a.') || '-' ||
        COALESCE(src.customer_city,'n.a.') || '-' ||
        COALESCE(src.customer_state,'n.a.'),
        'sl_offline_retail',
        'src_offline_retail'

    FROM sl_offline_retail.src_offline_retail src
),
   DISTINCT */
    distinct_source AS (
        SELECT DISTINCT
            customer_id,
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
        FROM unioned_sources WHERE customer_id <> 'n.a.'
    )
    /* Insert only rows not already present in the stg map table */
    INSERT INTO stg.mapping_customers (
        customer_id,
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
        cc.customer_id,
        cc.gender,
        cc.marital_status,
        cc.birth_of_dt,
        cc.membership_dt,
        cc.customer_zip_code,
        cc.customer_city,
        cc.customer_state,
        cc.last_purchase_dt,
        cc.customer_src_id,
        cc.source_system,
        cc.source_table
    FROM distinct_source cc
    WHERE NOT EXISTS (
      SELECT 1
      FROM stg.mapping_customers tgt
      WHERE tgt.customer_src_id = cc.customer_src_id
        AND tgt.source_system   = cc.source_system
        AND tgt.source_table    = cc.source_table
  );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    /* Log successful or no-change load result */
    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_customers',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Loaded formatted customer rows from sl_online_retail.src_online_retail and sl_offline_retail.src_offline_retail.'
    );

    RAISE NOTICE 'mapping_customers completed. inserted=%', v_ins;

EXCEPTION
    WHEN OTHERS THEN
        /* Capture detailed PostgreSQL error diagnostics */
        GET STACKED DIAGNOSTICS
            v_err_detail = PG_EXCEPTION_DETAIL,
            v_err_hint   = PG_EXCEPTION_HINT;

        v_err_msg := SQLERRM;

        /* Log failure details for troubleshooting */
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

        RAISE;
END;
$$;

-- stores
CREATE OR REPLACE PROCEDURE stg.load_map_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc TEXT := 'stg.load_map_stores';
    v_ins  INT  := 0;
    v_err_msg TEXT;
    v_err_detail TEXT;
    v_err_hint TEXT;
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
            src.store_location,
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
            store_location,
            source_system,
            source_table
        FROM prepared_source
        WHERE store_src_id <> 'n.a., -n.a.-n.a.'
    )
    INSERT INTO stg.mapping_stores (
        store_src_id,
        store_name,
        store_zip_code,
        store_city,
        store_state,
        store_location,
        source_system,
        source_table
    )
    SELECT
        s.store_src_id,
        s.store_name,
        s.store_zip_code,
        s.store_city,
        s.store_state,
        s.store_location,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
    SELECT 1
    FROM stg.mapping_stores tgt
    WHERE tgt.store_src_id   = s.store_src_id
      AND tgt.source_system  = s.source_system
      AND tgt.source_table   = s.source_table
);
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_stores',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ' store rows (full variation preserved)'
    );

    RAISE NOTICE 'mapping_stores completed. inserted=%', v_ins;
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

-- products
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

    /* ONLINE */
    SELECT
        COALESCE(NULLIF(src.product_id, ''), 'n.a.') AS product_id,
        COALESCE(src.product_category,'n.a.') AS product_category,
        COALESCE(src.product_name,'n.a.') AS product_name,
        COALESCE(src.product_brand,'n.a.') AS product_brand,
        COALESCE(src.product_material,'n.a.') AS product_material,

        src.product_stock,
        src.product_manufacture_dt,
        src.product_expiry_dt,

        'sl_online_retail' AS source_system,
        'src_online_retail' AS source_table

    FROM sl_online_retail.src_online_retail src

    UNION ALL

    /* OFFLINE */
    SELECT
        COALESCE(NULLIF(src.product_id, ''), 'n.a.'),
        COALESCE(src.product_category,'n.a.'),
        COALESCE(src.product_name,'n.a.'),
        COALESCE(src.product_brand,'n.a.'),
        COALESCE(src.product_material,'n.a.'),

        src.product_stock,
        src.product_manufacture_dt,
        src.product_expiry_dt,

        'sl_offline_retail',
        'src_offline_retail'

    FROM sl_offline_retail.src_offline_retail src
),

distinct_source AS (
    SELECT DISTINCT
        product_id,
        product_category,
        product_name,
        product_brand,
        product_material,
        product_stock,
        product_manufacture_dt,
        product_expiry_dt,
        source_system,
        source_table
    FROM unioned_sources
    WHERE product_id <> 'n.a.'
)

      INSERT INTO stg.mapping_products (
          product_id,
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
          s.product_id,
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
          WHERE tgt.product_id = s.product_id
            AND tgt.source_system  = s.source_system
            AND tgt.source_table   = s.source_table
      );
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_products',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins ||
        '. Loaded formatted product rows using raw product_id as business key candidate in stg.'
    );

    RAISE NOTICE 't_map_products completed. inserted=%', v_ins;

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

-- promotions
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
            src.promotion_id,
            src.promotion_type,
            src.promotion_channel,
            src.promotion_start_dt,
            src.promotion_end_dt,

            'sl_online_retail' AS source_system,
            'src_online_retail' AS source_table

        FROM sl_online_retail.src_online_retail src

        UNION ALL

        SELECT
            src.promotion_id,
            src.promotion_type,
            src.promotion_channel,
            src.promotion_start_dt,
            src.promotion_end_dt,

            'sl_offline_retail',
            'src_offline_retail'

        FROM sl_offline_retail.src_offline_retail src
    ),

    distinct_source AS (
        SELECT DISTINCT
            promotion_id,
            promotion_type,
            promotion_channel,
            promotion_start_dt,
            promotion_end_dt,
            source_system,
            source_table
        FROM unioned_sources
        WHERE promotion_id <> 'n.a.'
    )

    INSERT INTO stg.mapping_promotions (
        promotion_id,
        promotion_type,
        promotion_channel,
        promotion_start_dt,
        promotion_end_dt,
        source_system,
        source_table
    )
    SELECT
        s.promotion_id,
        s.promotion_type,
        s.promotion_channel,
        s.promotion_start_dt,
        s.promotion_end_dt,
        s.source_system,
        s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
    SELECT 1
    FROM stg.mapping_promotions tgt
    WHERE tgt.promotion_id = s.promotion_id
      AND tgt.source_system    = s.source_system
      AND tgt.source_table     = s.source_table
);

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_promotions',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || ' . Loaded formatted promotion rows using promotion_id as business key candidate'
    );

    RAISE NOTICE 't_map_promotions completed. inserted=%', v_ins;

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

--deliveries

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

    /* ONLINE */
    SELECT
        COALESCE(NULLIF(src.delivery_id,''),'n.a.') AS delivery_id,

        COALESCE(src.delivery_type,'n.a.') AS delivery_type,
        COALESCE(src.delivery_status,'n.a.') AS delivery_status,
        COALESCE(src.shipping_partner,'n.a.') AS shipping_partner,

        'sl_online_retail' AS source_system,
        'src_online_retail' AS source_table

    FROM sl_online_retail.src_online_retail src

    UNION ALL

    /* OFFLINE */
    SELECT
        COALESCE(NULLIF(src.delivery_id,''),'n.a.'),

        COALESCE(src.delivery_type,'n.a.'),
        COALESCE(src.delivery_status,'n.a.'),
        COALESCE(src.shipping_partner,'n.a.'),

        'sl_offline_retail',
        'src_offline_retail'

    FROM sl_offline_retail.src_offline_retail src
),
    -- distinct_source MUST include src_id
distinct_source AS (
    SELECT DISTINCT
        delivery_id,
        delivery_type,
        delivery_status,
        shipping_partner,
        source_system,
        source_table
    FROM unioned_sources
    WHERE delivery_id <> 'n.a.'
)

-- INSERT MUST include it
INSERT INTO stg.mapping_deliveries (
    delivery_id,
    delivery_type,
    delivery_status,
    shipping_partner,
    source_system,
    source_table
)
SELECT
    s.delivery_id,
    s.delivery_type,
    s.delivery_status,
    s.shipping_partner,
    s.source_system,
    s.source_table
    FROM distinct_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg.mapping_deliveries t
        WHERE t.delivery_id      = s.delivery_id
          AND t.source_system    = s.source_system
          AND t.source_table     = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_deliveries',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded cleaned delivery rows using SELECT DISTINCT only.'
    );

    RAISE NOTICE 't_map_deliveries completed. inserted=%', v_ins;

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


-- enagagements
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
            COALESCE(NULLIF(src.engagement_id,''),'n.a.') AS engagement_id,
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
            engagement_id,
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
        WHERE engagement_id <> 'n.a.'
    )
    INSERT INTO stg.mapping_engagements (
        engagement_id,
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
        s.engagement_id,
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
        WHERE t.engagement_id = s.engagement_id
          AND t.source_system = s.source_system
          AND t.source_table  = s.source_table
    );

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_engagements',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded online engagement map rows.'
    );

    RAISE NOTICE 't_map_engagements completed. inserted=%', v_ins;

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

-- employees ** 
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
        employee_name       VARCHAR(100),
        employee_position   VARCHAR(100),
        employee_salary     NUMERIC(10,2),
        employee_hire_date  DATE,
        observed_ts         TIMESTAMP,
        source_system       VARCHAR(100),
        source_table        VARCHAR(100)
    ) ON COMMIT DROP;

    INSERT INTO tmp_employee_map_source
    SELECT DISTINCT
        src.employee_name || '-' || src.employee_hire_date AS employee_src_id,   
        src.employee_name,
        src.employee_position,
        src.employee_salary,
        src.employee_hire_date,
        COALESCE(src.transaction_dt, TIMESTAMP '1900-01-01') AS observed_ts, 
        'sl_offline_retail' AS source_system,
        'src_offline_retail' AS source_table
    FROM sl_offline_retail.src_offline_retail src
    WHERE COALESCE(src.employee_name, 'n.a.') IS NOT NULL;

       -- INCREMENTAL SOURCE (optional)
       
    IF to_regclass('sl_offline_retail.src_offline_retail_employee_inc') IS NOT NULL THEN

        INSERT INTO tmp_employee_map_source
        SELECT DISTINCT
            inc.employee_name || '-' || inc.employee_hire_date AS employee_src_id,
            inc.employee_name,
            inc.employee_position,
            inc.employee_salary,
            inc.employee_hire_date,

            COALESCE(inc.transaction_dt, CURRENT_TIMESTAMP) AS observed_ts,  -- burayı değiştirdim

            'sl_offline_retail',
            'src_offline_retail_employee_inc'
        FROM sl_offline_retail.src_offline_retail_employee_inc inc
        WHERE inc.employee_name IS NOT NULL;

    END IF;

-- FINAL INSERT (SCD2 READY)

    INSERT INTO stg.mapping_employees (
        employee_src_id,
        employee_name,
        employee_position,
        employee_salary,
        employee_hire_date,
        observed_ts,
        source_system,
        source_table
    )
    SELECT
        s.employee_src_id,
        s.employee_name,
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
      AND COALESCE(t.employee_position, 'n.a.')
          = COALESCE(s.employee_position, 'n.a.')
      AND COALESCE(t.employee_salary, -1)
          = COALESCE(s.employee_salary, -1)
      AND COALESCE(t.employee_hire_date, DATE '1900-01-01')
          = COALESCE(s.employee_hire_date, DATE '1900-01-01')
      AND COALESCE(t.observed_ts, TIMESTAMP '1900-01-01 00:00:00')
          = COALESCE(s.observed_ts, TIMESTAMP '1900-01-01 00:00:00')
      AND t.source_system = s.source_system
      AND t.source_table  = s.source_table
);

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'stg.mapping_employees',
        v_ins,
        CASE WHEN v_ins > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins || '. Loaded employee version rows (SCD2-ready)'
    );

    RAISE NOTICE 't_map_employees completed. inserted=%', v_ins;

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
