
CREATE OR REPLACE PROCEDURE stg.load_raw_sources(
    p_offline_file TEXT,
    p_online_file  TEXT,
    p_batch_id     BIGINT,
    p_load_type    TEXT,
    p_truncate     BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_err_msg       TEXT;
    v_err_detail    TEXT;
    v_err_hint      TEXT;
BEGIN
    CALL stg.load_raw_offline(p_offline_file, p_batch_id, p_load_type, p_truncate);
    CALL stg.load_raw_online (p_online_file , p_batch_id, p_load_type, p_truncate);

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        'stg.load_raw_sources',
        'RAW_SOURCES_WRAPPER',
        0,
        'FAILED',
        'Raw sources wrapper failed.',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,
        p_batch_id
    );

    RAISE;
END;
$$;
