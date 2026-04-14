# Data Quality Requirements

## Functional
- Validate incoming retail records from both channels before mapping.
- Detect and report duplicate transactions and key collisions.
- Enforce referential integrity expectations across layers.
- Provide batch-level DQ status for go/no-go decision.

## Non-functional
- Checks should be rerunnable and idempotent.
- DQ outputs should be human-readable and machine-queryable.
- Every failed check must include context (entity, key, reason, count).
