# Evidence Pack (What to attach after each run)

This folder stores reproducible evidence for architecture, DQ, security, and performance claims.

## Suggested run order
1. `04_schema_smoke_checks.sql`
2. `02_row_count_reconciliation.sql`
3. `03_measure_reconciliation.sql`
4. `05_rls_access_evidence.sql`
5. `06_performance_explain_template.sql`

## Suggested artifacts to commit
- SQL outputs (CSV/TSV or screenshots)
- `01_run_log_template.md` filled per run
- Query plans (`EXPLAIN (ANALYZE, BUFFERS)`) for critical joins
- RLS evidence with role/session context values

## Naming convention
- `run_YYYYMMDD_HHMM_<scope>.md`
- `plan_YYYYMMDD_<query_name>.txt`
- `recon_YYYYMMDD_<layer_pair>.csv`
