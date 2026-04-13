-- INGESTION FIRST

CALL stg.master_full_load(
    p_source_system => 'bulk_95_demo', -- whatever
    p_file_id       => NULL,
    p_trigger_type  => 'MANUAL'
);

CALL stg.master_ingestion_load(
    '/content/data/01_empty_95_off.csv',
    '/content/data/02_empty_95_on.csv',
    'BULK',
    TRUE
);
