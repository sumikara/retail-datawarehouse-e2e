# Incremental Load Strategy

## Baseline
Current orchestration supports repeatable full loads with step-level observability.

## Target incremental design
1. **Source watermarking**
   - Use `transaction_dt` + optional file metadata (`file_period`, `file_hash`).
2. **Idempotent merge**
   - Maintain `row_sig`/business key uniqueness checks for replay-safe ingestion.
3. **Late arriving data**
   - Allow backfill windows (e.g., last N days) to capture delayed records.
4. **Dimension synchronization**
   - Upsert dimensions before fact load to avoid unresolved FK churn.
5. **Reconciliation gates**
   - Compare source vs target counts and amount sums before closing batch.

## Failure recovery
- Restart from failed step via orchestration metadata.
- Avoid full reruns when only downstream steps failed.
