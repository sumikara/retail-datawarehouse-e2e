CREATE OR REPLACE PROCEDURE stg.master_ingestion_load(
    p_offline_file TEXT,
    p_online_file  TEXT,
    p_load_type    TEXT,
    p_truncate     BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id         BIGINT;
    v_step_id          BIGINT;
    v_file_offline_id  BIGINT;
    v_file_online_id   BIGINT;
    v_err_msg          TEXT;
    v_err_detail       TEXT;
    v_err_hint         TEXT;
BEGIN
    INSERT INTO stg.etl_batch_run (
        pipeline_name,
        trigger_type,
        status,
        source_system,
        file_count
    )
    VALUES (
        'stg.master_ingestion_load',
        'MANUAL',
        'STARTED',
        'offline+online',
        2
    )
    RETURNING batch_id INTO v_batch_id;

    INSERT INTO stg.etl_file_registry (
        source_system, file_name, file_path, status, batch_id
    )
    VALUES (
        'offline',
        regexp_replace(p_offline_file, '^.*/', ''),
        p_offline_file,
        'IN_PROGRESS',
        v_batch_id
    )
    RETURNING file_id INTO v_file_offline_id;

    INSERT INTO stg.etl_file_registry (
        source_system, file_name, file_path, status, batch_id
    )
    VALUES (
        'online',
        regexp_replace(p_online_file, '^.*/', ''),
        p_online_file,
        'IN_PROGRESS',
        v_batch_id
    )
    RETURNING file_id INTO v_file_online_id;

    PERFORM stg.log_etl_event(
        'stg.master_ingestion_load',
        'MASTER_INGESTION',
        0,
        'STARTED',
        'Master ingestion started.',
        NULL,
        'INFO',
        0,0,0,0,
        v_batch_id
    );

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'load_raw_sources', 10, 'sl_*_raw', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.load_raw_sources(
        p_offline_file,
        p_online_file,
        v_batch_id,
        p_load_type,
        p_truncate
    );

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    INSERT INTO stg.etl_step_run(batch_id, step_name, step_order, target_object, status)
    VALUES (v_batch_id, 'build_clean_staging', 20, 'sl_*_retail', 'STARTED')
    RETURNING step_run_id INTO v_step_id;

    CALL stg.build_clean_staging(v_batch_id);

    UPDATE stg.etl_step_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE step_run_id = v_step_id;

    UPDATE stg.etl_file_registry
       SET status = 'DONE',
           processed_ts = CURRENT_TIMESTAMP
     WHERE file_id IN (v_file_offline_id, v_file_online_id);

    UPDATE stg.etl_batch_run
       SET status = 'SUCCESS',
           end_ts = CURRENT_TIMESTAMP
     WHERE batch_id = v_batch_id;

    PERFORM stg.log_etl_event(
        'stg.master_ingestion_load',
        'MASTER_INGESTION',
        0,
        'SUCCESS',
        'Master ingestion completed successfully.',
        NULL,
        'INFO',
        0,0,0,0,
        v_batch_id
    );
    RAISE NOTICE 'Ingestion is completed' ;

    

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    UPDATE stg.etl_step_run
       SET status = 'FAILED',
           end_ts = CURRENT_TIMESTAMP,
           error_message = v_err_msg
     WHERE step_run_id = v_step_id;

    UPDATE stg.etl_file_registry
       SET status = 'FAILED',
           processed_ts = CURRENT_TIMESTAMP,
           error_message = v_err_msg
     WHERE file_id IN (v_file_offline_id, v_file_online_id);

    UPDATE stg.etl_batch_run
       SET status = 'FAILED',
           end_ts = CURRENT_TIMESTAMP,
           error_message = v_err_msg
     WHERE batch_id = v_batch_id;

    PERFORM stg.log_etl_event(
        'stg.master_ingestion_load',
        'MASTER_INGESTION',
        0,
        'FAILED',
        'Master ingestion failed.',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,
        v_batch_id
    );

    RAISE;
END;
$$;
