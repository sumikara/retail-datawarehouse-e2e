

%%bash
set -e
DB="retail_dw"
sudo -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
-- incremental load
-- INGESTION FIRST

CALL stg.master_ingestion_load(
    '/content/data/03_empty_5_off.csv',
    '/content/data/04_empty_5_on.csv',
    'BULK',
    TRUE
);


CALL stg.master_full_load(
    p_source_system => 'bulk_95_demo', -- whatever
    p_file_id       => NULL,
    p_trigger_type  => 'MANUAL'
);


SQL
