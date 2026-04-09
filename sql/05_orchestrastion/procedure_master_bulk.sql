CREATE OR REPLACE PROCEDURE stg.master_full_load(
    p_source_system   VARCHAR DEFAULT NULL,
    p_file_id         BIGINT DEFAULT NULL,
    p_trigger_type    VARCHAR DEFAULT 'MANUAL'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id BIGINT;
    v_step_id  BIGINT;
BEGIN
    /* =========================================================
       1) OPEN BATCH
       ========================================================= */
    INSERT INTO stg.etl_batch_run (
        pipeline_name,
        trigger_type,
        status,
        source_system
    )
    VALUES (
        'retail_dwh_master_full_load',
        p_trigger_type,
        'STARTED',
        p_source_system
    )
    RETURNING batch_id INTO v_batch_id;

    PERFORM stg.log_etl_event(
        'stg.master_full_load',
        'MASTER',
        0,
        'STARTED',
        'Master load started. source_system=' || COALESCE(p_source_system,'n.a.')
            || ', file_id=' || COALESCE(p_file_id::TEXT,'n.a.'),
        NULL,
        'INFO',
        0,0,0,0,
        v_batch_id
    );

    /* =========================================================
       2) MAP LAYER
       ========================================================= */

    -- load_map_customers
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_customers', 10, 'stg.mapping_customers', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_customers();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_stores
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_stores', 20, 'stg.mapping_stores', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_stores();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_products
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_products', 30, 'stg.mapping_products', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_products();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_promotions
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_promotions', 40, 'stg.mapping_promotions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_promotions();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_deliveries
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_deliveries', 50, 'stg.mapping_deliveries', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_deliveries();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_engagements
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_engagements', 60, 'stg.mapping_engagements', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_engagements();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_employees
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_employees', 70, 'stg.mapping_employees', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_employees();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    -- load_map_transactions
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_map_transactions', 80, 'stg.mapping_transactions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_map_transactions();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    /* =========================================================
       3) 3NF REFERENCE ENTITIES
       ========================================================= */

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_states', 90, '3nf.nf_states', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_states();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_cities', 100, '3nf.nf_cities', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_cities();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_addresses', 110, '3nf.nf_addresses', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_addresses();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_product_categories', 120, '3nf.nf_product_categories', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_product_categories();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_promotion_types', 130, '3nf.nf_promotion_types', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_promotion_types();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_shipping_partners', 140, '3nf.nf_shipping_partners', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_shipping_partners();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    /* =========================================================
       4) 3NF BUSINESS ENTITIES
       ========================================================= */

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_customers', 150, '3nf.nf_customers', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_customers();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_stores', 160, '3nf.nf_stores', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_stores();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_products', 170, '3nf.nf_products', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_products();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_promotions', 180, '3nf.nf_promotions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_promotions();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_deliveries', 190, '3nf.nf_deliveries', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_deliveries();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_engagements', 200, '3nf.nf_engagements', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_engagements();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_employees_scd', 210, '3nf.nf_employees_scd', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_employees_scd();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_ce_transactions', 220, '3nf.nf_transactions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_ce_transactions();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    /* =========================================================
       5) DIMENSIONS
       ========================================================= */

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_customers', 230, 'dim.dim_customers', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_customers();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_stores', 240, 'dim.dim_stores', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_stores();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_products', 250, 'dim.dim_products', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_products();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_promotions', 260, 'dim.dim_promotions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_promotions();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_deliveries', 270, 'dim.dim_deliveries', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_deliveries();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_engagements', 280, 'dim.dim_engagements', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_engagements();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_employees_scd', 290, 'dim.dim_employees_scd', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_employees_scd();
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_dim_dates', 300, 'dim.dim_dates', 'STARTED')
    RETURNING step_run_id INTO v_step_id;
    CALL stg.load_dim_dates('2024-01-01'::DATE, '2030-12-31'::DATE);
    UPDATE stg.etl_step_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP WHERE step_run_id=v_step_id;

    /* =========================================================
       6) FACT
       ========================================================= */
    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'master_transactions_monthly_load', 310, 'bl_dm.fct_transactions', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.master_transactions_monthly_load();

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    /* =========================================================
       7) CLOSE BATCH
       ========================================================= */
    UPDATE stg.etl_batch_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE batch_id = v_batch_id;

    PERFORM stg.log_etl_event(
        'stg.master_full_load',
        'MASTER',
        1,
        'SUCCESS',
        'Master load completed successfully.',
        NULL,
        'INFO',
        0,0,0,0,
        v_batch_id
    );

EXCEPTION
    WHEN OTHERS THEN

        UPDATE stg.etl_step_run
           SET status = 'FAILED',
               end_ts = CURRENT_TIMESTAMP,
               error_message = SQLERRM
         WHERE step_run_id = v_step_id;

        UPDATE stg.etl_batch_run
           SET status = 'FAILED',
               end_ts = CURRENT_TIMESTAMP,
               error_message = SQLERRM
         WHERE batch_id = v_batch_id;

        PERFORM stg.log_etl_event(
            'stg.master_full_load',
            'MASTER',
            0,
            'FAILED',
            'Master load failed.',
            SQLERRM,
            'ERROR',
            0,0,0,0,
            v_batch_id
        );

        RAISE;
END;
$$;

