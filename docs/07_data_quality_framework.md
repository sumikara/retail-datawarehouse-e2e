# Data Quality Framework

## Quality dimensions (minimum)
1. **Completeness**: required fields populated (IDs, dates, keys).
2. **Uniqueness**: no duplicate transaction identity / row signatures.
3. **Validity**: values in expected type/range/domain.
4. **Consistency**: cross-layer agreement (mapping → nf → dim/fact).
5. **Timeliness**: ingestion latency and batch freshness thresholds.
6. **Accuracy**: measure reasonability and business-rule plausibility.

## Layered DQ checkpoints
- Staging: null/format/type checks.
- Mapping: source-id derivation consistency.
- Normalized: FK integrity and reference resolution.
- Marts: conformed dimensions and aggregate parity.

## Scorecard idea
- Per-batch DQ summary with pass/fail counts.
- Severity tagging: blocker / warning / info.
