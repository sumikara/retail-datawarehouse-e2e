# Reconciliation Results Interpretation

## Levels
- Row count reconciliation
- Measure reconciliation (sum totals, quantities)
- Key coverage reconciliation (mapped vs unresolved)

## Outcome classes
- **PASS**: fully matched within tolerance
- **WARN**: minor drift within known tolerance
- **FAIL**: mismatch requiring batch hold/review

## Typical root causes
- Late-arriving or replayed files
- Mapping key drift
- Dimension lag before fact load
- Filtering discrepancies across layers
