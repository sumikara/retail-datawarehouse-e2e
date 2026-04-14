# SCD Strategy

## Scope
SCD treatment primarily applies to employee attributes where history matters for analytics and auditability.

## Recommended pattern
- **Type 2** for `employee_position`, `employee_salary`, and other changing descriptive attributes.
- Use `start_dt`, `end_dt`, `is_active` as temporal controls.
- Enforce one active version per employee source id.

## Loading logic (high-level)
1. Detect changed rows by comparing source snapshot vs active version.
2. Close active row (`end_dt = now`, `is_active=false`).
3. Insert new active row (`start_dt = now`, open-ended `end_dt`).

## Quality checks
- No overlapping effective periods per employee.
- Exactly one active row for employees that still exist in source.
