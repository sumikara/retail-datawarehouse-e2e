
CREATE SEQUENCE IF NOT EXISTS stg.etl_batch_run_seq START 1;
CREATE SEQUENCE IF NOT EXISTS stg.etl_step_run_seq START 1;
CREATE SEQUENCE IF NOT EXISTS stg.etl_file_registry_seq START 1;

CREATE TABLE IF NOT EXISTS stg.etl_batch_run (
    batch_id            BIGINT PRIMARY KEY DEFAULT nextval('stg.etl_batch_run_seq'),
    pipeline_name       VARCHAR(200) NOT NULL,
    trigger_type        VARCHAR(50) NOT NULL DEFAULT 'MANUAL',   -- MANUAL / SCHEDULED
    status              VARCHAR(30) NOT NULL DEFAULT 'STARTED',  -- STARTED / SUCCESS / FAILED
    source_system       VARCHAR(100),
    initiated_by        VARCHAR(100) DEFAULT current_user,
    start_ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_ts              TIMESTAMP,
    file_count          INTEGER DEFAULT 0,
    total_rows_read     BIGINT DEFAULT 0,
    total_rows_loaded   BIGINT DEFAULT 0,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS stg.etl_file_registry (
    file_id             BIGINT PRIMARY KEY DEFAULT nextval('stg.etl_file_registry_seq'),
    source_system       VARCHAR(100) NOT NULL,                   -- online / offline
    file_name           VARCHAR(500) NOT NULL,
    file_path           VARCHAR(1000) NOT NULL,
    file_period         DATE,                                    -- e.g. 2026-03-01 for monthly batch
    file_hash           VARCHAR(128),
    status              VARCHAR(30) NOT NULL DEFAULT 'NEW',      -- NEW / IN_PROGRESS / DONE / FAILED / SKIPPED
    arrival_ts          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_ts        TIMESTAMP,
    batch_id            BIGINT,
    rows_read           BIGINT DEFAULT 0,
    rows_loaded         BIGINT DEFAULT 0,
    error_message       TEXT,
    CONSTRAINT uq_etl_file_registry UNIQUE (source_system, file_name)
);

CREATE TABLE IF NOT EXISTS stg.etl_step_run (
    step_run_id         BIGINT PRIMARY KEY DEFAULT nextval('stg.etl_step_run_seq'),
    batch_id            BIGINT NOT NULL,
    step_name           VARCHAR(200) NOT NULL,
    step_order          INTEGER,
    target_object       VARCHAR(200),
    status              VARCHAR(30) NOT NULL DEFAULT 'STARTED',  -- STARTED / SUCCESS / FAILED
    start_ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_ts              TIMESTAMP,
    rows_inserted       BIGINT DEFAULT 0,
    rows_updated        BIGINT DEFAULT 0,
    rows_deleted        BIGINT DEFAULT 0,
    rows_rejected       BIGINT DEFAULT 0,
    error_message       TEXT,
    CONSTRAINT fk_step_batch
        FOREIGN KEY (batch_id) REFERENCES stg.etl_batch_run(batch_id)
);

