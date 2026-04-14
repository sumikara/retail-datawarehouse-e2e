# Orchestration Notes

## Design
Master procedures execute deterministic step order and update metadata tables for each step.

## Step categories
1. Ingestion and cleansing
2. Mapping
3. Normalized reference entities
4. Normalized business entities
5. Dimensions
6. Fact loading

## Reliability principles
- Every step writes status + timing.
- Fail-fast with error capture to ETL log.
- Keep step boundaries small for rerun control.
