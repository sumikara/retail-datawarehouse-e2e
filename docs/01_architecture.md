# Architecture Overview

## System Goal
Build an end-to-end retail data warehouse that integrates online + offline transactions into a unified analytical model.

## Architectural Style
- **Integration core**: normalized `nf` schema (Inmon-style discipline)
- **Presentation layer**: dimensional `dim` schema (Kimball-style star consumption)
- **Control plane**: batch/step/file metadata + ETL logging in `stg`

## Execution Flow
1. Load source files into staging raw/clean tables.
2. Build entity mapping tables with standardized fields and source identifiers.
3. Load normalized reference + business entities.
4. Load dimensions and date dimension.
5. Load partitioned fact table for analytics.

## Operational Features
- Procedure-based orchestration
- Batch-step logging and event audit trail
- Security baseline (roles/grants/RLS)
- DQ and reconciliation scaffolding

## Why this works
This pattern separates concerns: ingestion correctness, integration correctness, and analytics performance.
