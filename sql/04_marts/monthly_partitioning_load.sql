CREATE OR REPLACE PROCEDURE dim.load_fct_transactions_dd_by_month(
    p_year  INTEGER,
    p_month INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc           TEXT := 'dim.load_fct_transactions_dd_by_month';
    v_partition_name TEXT;
    v_start_date     DATE;
    v_end_date       DATE;
    v_rows_inserted  INTEGER := 0;
    v_err_msg        TEXT;
    v_err_detail     TEXT;
    v_err_hint       TEXT;
BEGIN
    v_start_date := make_date(p_year, p_month, 1);
    v_end_date   := (v_start_date + INTERVAL '1 month')::DATE;
    v_partition_name := 'fct_transactions_dd_dd_' || to_char(v_start_date, 'YYYYMM');

    BEGIN
        EXECUTE format('ALTER TABLE dim.fct_transactions_dd_dd DETACH PARTITION dim.%I;', v_partition_name);
        EXECUTE format('DROP TABLE dim.%I;', v_partition_name);
    EXCEPTION
        WHEN undefined_table THEN
            NULL;
    END;

    EXECUTE format(
        'CREATE TABLE dim.%I PARTITION OF dim.fct_transactions_dd_dd
         FOR VALUES FROM (%L) TO (%L);',
        v_partition_name, v_start_date, v_end_date
    );

    EXECUTE format($SQL$
        INSERT INTO dim.%I (
            transaction_src_id,
            total_sales,
            quantity,
            unit_price,
            discount_applied,
            payment_method,
            product_surr_id,
            promotion_surr_id,
            delivery_surr_id,
            engagement_surr_id,
            store_surr_id,
            customer_surr_id,
            employee_surr_id,
            transaction_date_sk,
            transaction_date,
            source_system,
            source_table,
            insert_dt,
            update_dt
        )
        SELECT
            t.transaction_id,
            COALESCE(t.total_sales, 0.00),
            COALESCE(t.quantity, 0),
            COALESCE(t.unit_price, 0.00),
            COALESCE(t.discount_applied, 0.00),
            COALESCE(NULLIF(t.payment_method, ''), 'n.a.'),

            COALESCE(dp.product_surr_id, -1),
            COALESCE(dpr.promotion_surr_id, -1),
            COALESCE(ddv.delivery_surr_id, -1),
            COALESCE(den.engagement_surr_id, -1),
            COALESCE(ds.store_surr_id, -1),
            COALESCE(dc.customer_surr_id, -1),

            COALESCE(
                (
                    SELECT des.employee_surr_id
                    FROM dim.dim_employees_scd des
                    WHERE des.employee_src_id = t.employee_id
                      AND des.is_active = TRUE
                    LIMIT 1
                ),
                -1
            ) AS employee_surr_id,

            dd.date_surr_id,
            dd.full_date,
            COALESCE(t.source_system, 'n.a.'),
            COALESCE(t.source_table, 'n.a.'),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM nf.nf_transactions t
        JOIN dim.dim_dates dd
          ON dd.full_date = DATE(t.transaction_dt)
        LEFT JOIN dim.dim_products    dp  ON dp.product_src_id      = t.product_id
        LEFT JOIN dim.dim_promotions  dpr ON dpr.promotion_src_id   = t.promotion_id
        LEFT JOIN dim.dim_deliveries  ddv ON ddv.delivery_src_id    = t.delivery_id
        LEFT JOIN dim.dim_engagements den ON den.engagement_src_id  = t.engagement_id
        LEFT JOIN dim.dim_stores      ds  ON ds.store_src_id        = t.store_id
        LEFT JOIN dim.dim_customers   dc  ON dc.customer_src_id     = t.customer_id
        WHERE dd.full_date >= %L
          AND dd.full_date <  %L;
    $SQL$, v_partition_name, v_start_date, v_end_date);

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.fct_transactions_dd_dd',
        v_rows_inserted,
        CASE WHEN v_rows_inserted > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Loaded month=' || to_char(v_start_date, 'YYYY-MM') ||
        ', partition=' || v_partition_name ||
        ', inserted=' || v_rows_inserted || '.'
    );

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.fct_transactions_dd_dd',
        0,
        'FAILED',
        'Monthly fact load failed for ' || COALESCE(to_char(v_start_date, 'YYYY-MM'), 'n.a.'),
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;

-- for incremental mostly alternative use
CREATE OR REPLACE PROCEDURE stg.master_transactions_monthly_load()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc             TEXT := 'stg.master_transactions_monthly_load';
    v_last_loaded_date DATE;
    v_min_source_date  DATE;
    v_max_source_date  DATE;
    v_start_loop_date  DATE;
    v_months_loaded    INTEGER := 0;
    v_err_msg          TEXT;
    v_err_detail       TEXT;
    v_err_hint         TEXT;
BEGIN
    SELECT MIN(DATE(transaction_dt)), MAX(DATE(transaction_dt))
    INTO v_min_source_date, v_max_source_date
    FROM nf.nf_transactions
    WHERE transaction_id <> 'n.a.';

    IF v_min_source_date IS NULL OR v_max_source_date IS NULL THEN
        PERFORM stg.log_etl_event(
            v_proc,
            'dim.fct_transactions_dd_dd',
            0,
            'NO_CHANGE',
            'No source rows found. Nothing to load.'
        );
        RETURN;
    END IF;

    SELECT MAX(transaction_date)
    INTO v_last_loaded_date
    FROM dim.fct_transactions_dd_dd;

    IF v_last_loaded_date IS NULL THEN
        v_start_loop_date := date_trunc('month', v_min_source_date)::DATE;
    ELSE
        v_start_loop_date := (date_trunc('month', v_last_loaded_date)::DATE + INTERVAL '1 month')::DATE;
    END IF;

    IF v_start_loop_date > v_max_source_date THEN
        PERFORM stg.log_etl_event(
            v_proc,
            'dim.fct_transactions_dd_dd',
            0,
            'NO_CHANGE',
            'No new months to load.'
        );
        RETURN;
    END IF;

    WHILE v_start_loop_date <= v_max_source_date LOOP
        CALL dim.load_fct_transactions_dd_by_month(
            EXTRACT(YEAR  FROM v_start_loop_date)::INT,
            EXTRACT(MONTH FROM v_start_loop_date)::INT
        );

        v_months_loaded := v_months_loaded + 1;
        v_start_loop_date := (v_start_loop_date + INTERVAL '1 month')::DATE;
    END LOOP;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.fct_transactions_dd_dd',
        v_months_loaded,
        'SUCCESS',
        'Master monthly load completed. Months loaded=' || v_months_loaded || '.'
    );
    RAISE NOTICE 'Master monthly load completed. Months loaded=%', v_months_loaded;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;
    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        'dim.fct_transactions_dd_dd',
        0,
        'FAILED',
        'Master monthly load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR'
    );
    RAISE;
END;
$$;
