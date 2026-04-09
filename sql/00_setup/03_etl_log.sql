

CREATE SEQUENCE IF NOT EXISTS stg.etl_log_seq
START WITH 1
INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS stg.etl_log (

    log_id BIGINT PRIMARY KEY
        DEFAULT nextval('stg.etl_log_seq'),

    log_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    log_schema_name TEXT DEFAULT 'STG',
    procedure_name TEXT NOT NULL,
    table_name TEXT,
    rows_affected INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'SUCCESS',
        -- SUCCESS
        -- NO_CHANGE
        -- FAILED
        -- STARTED
    log_type VARCHAR(20) DEFAULT 'INFO',
        -- INFO
        -- WARNING
        -- ERROR
    log_message TEXT,
    error_detail TEXT,
    inserted_rows INTEGER DEFAULT 0,
    updated_rows  INTEGER DEFAULT 0,
    deleted_rows  INTEGER DEFAULT 0,
    closed_rows   INTEGER DEFAULT 0,
    batch_id      BIGINT DEFAULT NULL

);


CREATE OR REPLACE FUNCTION stg.log_etl_event(
    p_procedure_name TEXT,
    p_table_name TEXT,
    p_rows_affected INTEGER,
    p_status TEXT,
    p_log_message TEXT,
    p_error_detail TEXT DEFAULT NULL,
    p_log_type TEXT DEFAULT 'INFO',
    p_inserted_rows INTEGER DEFAULT 0,
    p_updated_rows  INTEGER DEFAULT 0,
    p_deleted_rows  INTEGER DEFAULT 0,
    p_closed_rows   INTEGER DEFAULT 0,
    p_batch_id      BIGINT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO stg.etl_log (
        procedure_name,
        table_name,
        rows_affected,
        status,
        log_message,
        error_detail,
        log_type,
        inserted_rows,
        updated_rows,
        deleted_rows,
        closed_rows,
        batch_id
    )
    VALUES (
        p_procedure_name,
        p_table_name,
        p_rows_affected,
        p_status,
        p_log_message,
        p_error_detail,
        p_log_type,
        p_inserted_rows,
        p_updated_rows,
        p_deleted_rows,
        p_closed_rows,
        p_batch_id
    );
END;
$$;

CREATE OR REPLACE VIEW stg.v_last_etl_runs AS
SELECT
    procedure_name,
    table_name,
    rows_affected,
    status,
    log_ts,
    log_message
FROM stg.etl_log;

