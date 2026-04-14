# Requirement Matrix (DQ)

| Requirement ID | Category | Description | Suggested Check Artifact |
|---|---|---|---|
| DQ-001 | Completeness | Mandatory keys are not null | null-rate check query |
| DQ-002 | Uniqueness | No duplicate transaction identity | duplicate finder query |
| DQ-003 | Validity | Numeric measures in valid bounds | range/domain query |
| DQ-004 | Consistency | Source and target counts reconcile | reconciliation query |
| DQ-005 | Timeliness | Batch freshness within SLA | batch timestamp monitor |
| DQ-006 | Accuracy | Amount logic plausibility | rule-based anomaly check |
