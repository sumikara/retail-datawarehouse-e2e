# Grain Profiling Contract

## Contract statement
The canonical process grain is **one transaction event per row**.

## Validation rules
- `transaction_id` + event context should be unique at ingest scope.
- No mixed-grain facts in the same target process table.
- Aggregated records must be redirected to separate marts or views.

## Anti-patterns to reject
- Blending transaction and daily-aggregate rows.
- Recomputing grain keys differently between mapping and fact load.
- Dimension values that imply multiple business events per row.
