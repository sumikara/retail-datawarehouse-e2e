
-- BULK LOAD - For Bulk Load Call first this

CALL stg.master_ingestion_load(
    '/content/data/01_empty_95_off.csv', -- depends on your file
    '/content/data/02_empty_95_on.csv',  -- depends on your file
    'BULK',
    TRUE -- it uses DROP, TRUNCATE etc.
);

-- first checks

-- check the log table first and orch's records

SELECT * FROM stg.etl_batch_run;
SELECT * FROM stg.etl_file_registry;
SELECT * FROM stg.etl_step_run;
SELECT * FROM stg.etl_log;



