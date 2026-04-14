# Security Model

## Role model
- `sumiadmin`: platform owner with full control.
- `role_etl_runner`: read/write execution role for ETL workloads.
- `role_bi_reader`: read-only mart consumption role.
- `role_dq_analyst`: read access for quality/reconciliation analysis.
- `role_auditor`: observability and control-table focused access.
- `role_employee_app`, `role_customer_app`: RLS-scoped self-service roles.

## Schema boundaries
- Restrict raw/staging write to ETL roles.
- Prefer dimensional read access for BI users.
- Keep normalized schema guarded for controlled access patterns.

## Row-level security
- Employee self-access by `app.current_employee_src_id`.
- Customer self-access by `app.current_customer_id_nk` for profile, transactions, and address lineage.
- Optional department-like scope through allowed position list.

## Security operations
- Use default privileges for future-proof grants.
- Audit via ETL metadata and log tables.
- Prefer view-based exposure for external consumers.
