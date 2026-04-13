

CALL stg.master_full_load(
    p_source_system => 'inc_5_demo', -- whatever
    p_file_id       => NULL,
    p_trigger_type  => 'MANUAL'
);
SQL
