# Audit Trail Logic

## Core objects
- `stg.etl_batch_run`: batch lifecycle
- `stg.etl_step_run`: step-level execution state
- `stg.etl_file_registry`: source file observability
- `stg.etl_log`: procedure/event-level diagnostics

## Required audit dimensions
- who (`initiated_by`)
- when (`start_ts`, `end_ts`, `log_ts`)
- what (`pipeline_name`, `step_name`, `table_name`)
- result (`status`, rows affected, error details)

## Operational use
- SLA tracking
- rerun troubleshooting
- compliance evidence during reviews
