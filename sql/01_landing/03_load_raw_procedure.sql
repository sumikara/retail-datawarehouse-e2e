
-- OffLINE CLEAN STAGING
  
CREATE OR REPLACE PROCEDURE stg.load_raw_offline(
    p_file_name   TEXT,
    p_batch_id    BIGINT,
    p_load_type   TEXT,
    p_truncate    BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_rows_inserted INTEGER := 0;
    v_err_msg       TEXT;
    v_err_detail    TEXT;
    v_err_hint      TEXT;
BEGIN
    EXECUTE format(
        $sql$
        ALTER FOREIGN TABLE sl_offline_retail.frg_offline_retail
           OPTIONS (SET filename %L)
           $sql$,
        p_file_name
    );

    IF p_truncate THEN
        TRUNCATE TABLE sl_offline_retail.src_offline_retail_raw;
    END IF;

    INSERT INTO sl_offline_retail.src_offline_retail_raw (
      customer_id, gender, marital_status, transaction_id, transaction_date,
      product_id, product_category, quantity, unit_price, discount_applied,
      day_of_week, week_of_year, month_of_year, product_name, product_brand,
      product_stock, product_material, promotion_id, promotion_type,
      promotion_start_date, promotion_end_date, customer_zip_code,
      customer_city, customer_state, store_zip_code, store_city, store_state,
      date_of_birth, payment_method, delivery_id, delivery_type,
      delivery_status, shipping_partner, employee_salary, membership_date,
      store_location, last_purchase_date, total_sales, product_manufacture_date,
      product_expiry_date, promotion_channel, employee_name, employee_position,
      employee_hire_date,
      batch_id, load_type, source_file_name, load_dts, source_row_num
    )
    SELECT
      customer_id, gender, marital_status, transaction_id, transaction_date,
      product_id, product_category, quantity, unit_price, discount_applied,
      day_of_week, week_of_year, month_of_year, product_name, product_brand,
      product_stock, product_material, promotion_id, promotion_type,
      promotion_start_date, promotion_end_date, customer_zip_code,
      customer_city, customer_state, store_zip_code, store_city, store_state,
      date_of_birth, payment_method, delivery_id, delivery_type,
      delivery_status, shipping_partner, employee_salary, membership_date,
      store_location, last_purchase_date, total_sales, product_manufacture_date,
      product_expiry_date, promotion_channel, employee_name, employee_position,
      employee_hire_date,
      p_batch_id,
      p_load_type,
      regexp_replace(p_file_name, '^.*/', ''),
      CURRENT_TIMESTAMP,
      ROW_NUMBER() OVER ()
    FROM sl_offline_retail.frg_offline_retail;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    PERFORM stg.log_etl_event(
        'stg.load_raw_offline',
        'sl_offline_retail.src_offline_retail_raw',
        v_rows_inserted,
        'SUCCESS',
        'Offline raw load completed from file: ' || p_file_name,
        NULL,
        'INFO',
        v_rows_inserted,
        0,0,0,
        p_batch_id
    );

        RAISE NOTICE 'OFFLINE RAW INGESTION completed. inserted=%', v_rows_inserted;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        'stg.load_raw_offline',
        'sl_offline_retail.src_offline_retail_raw',
        0,
        'FAILED',
        'Offline raw load failed for file: ' || p_file_name,
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,
        p_batch_id
    );

    RAISE;
END;
$proc$;

/* =========================================================
   ONLINE CLEAN STAGING
   ========================================================= */

CREATE OR REPLACE PROCEDURE stg.load_raw_online(
    p_file_name   TEXT,
    p_batch_id    BIGINT,
    p_load_type   TEXT,
    p_truncate    BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_rows_inserted INTEGER := 0;
    v_err_msg       TEXT;
    v_err_detail    TEXT;
    v_err_hint      TEXT;
BEGIN
    EXECUTE format(
        $sql$
        ALTER FOREIGN TABLE sl_online_retail.frg_online_retail
           OPTIONS (SET filename %L)
           $sql$,
        p_file_name
    );

    IF p_truncate THEN
        TRUNCATE TABLE sl_online_retail.src_online_retail_raw;
    END IF;

    INSERT INTO sl_online_retail.src_online_retail_raw (
      customer_id, gender, marital_status, transaction_id, transaction_date,
      product_id, product_category, quantity, unit_price, discount_applied,
      day_of_week, week_of_year, month_of_year, product_name, product_brand,
      product_stock, product_material, promotion_id, promotion_type,
      promotion_start_date, promotion_end_date, customer_zip_code,
      customer_city, customer_state, customer_support_calls, date_of_birth,
      payment_method, delivery_id, delivery_type, delivery_status,
      shipping_partner, membership_date, website_address, order_channel,
      customer_support_method, issue_status, product_manufacture_date,
      product_expiry_date, total_sales, promotion_channel, last_purchase_date,
      app_usage, website_visits, social_media_engagement, engagement_id,
      batch_id, load_type, source_file_name, load_dts, source_row_num
    )
    SELECT
      customer_id, gender, marital_status, transaction_id, transaction_date,
      product_id, product_category, quantity, unit_price, discount_applied,
      day_of_week, week_of_year, month_of_year, product_name, product_brand,
      product_stock, product_material, promotion_id, promotion_type,
      promotion_start_date, promotion_end_date, customer_zip_code,
      customer_city, customer_state, customer_support_calls, date_of_birth,
      payment_method, delivery_id, delivery_type, delivery_status,
      shipping_partner, membership_date, website_address, order_channel,
      customer_support_method, issue_status, product_manufacture_date,
      product_expiry_date, total_sales, promotion_channel, last_purchase_date,
      app_usage, website_visits, social_media_engagement, engagement_id,
      p_batch_id,
      p_load_type,
      regexp_replace(p_file_name, '^.*/', ''),
      CURRENT_TIMESTAMP,
      ROW_NUMBER() OVER ()
    FROM sl_online_retail.frg_online_retail;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    PERFORM stg.log_etl_event(
        'stg.load_raw_online',
        'sl_online_retail.src_online_retail_raw',
        v_rows_inserted,
        'SUCCESS',
        'Online raw load completed from file: ' || p_file_name,
        NULL,
        'INFO',
        v_rows_inserted,
        0,0,0,
        p_batch_id
    );

        RAISE NOTICE 'Online raw load completed' ;


EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        'stg.load_raw_online',
        'sl_online_retail.src_online_retail_raw',
        0,
        'FAILED',
        'Online raw load failed for file: ' || p_file_name,
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,
        p_batch_id
    );

    RAISE;
END;
$proc$;

