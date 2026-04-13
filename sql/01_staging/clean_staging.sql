
CREATE OR REPLACE PROCEDURE stg.build_clean_staging(
    p_batch_id BIGINT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_err_msg       TEXT;
    v_err_detail    TEXT;
    v_err_hint      TEXT;
BEGIN
    CALL stg.build_clean_online(p_batch_id);
    CALL stg.build_clean_offline(p_batch_id);

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        'stg.build_clean_staging',
        'CLEAN_STAGING_WRAPPER',
        0,
        'FAILED',
        'Clean staging wrapper failed.',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,0,0,0,
        p_batch_id
    );

    RAISE;
END;
$$
