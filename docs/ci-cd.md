# CI/CD Foundation

## What CI means in this repository

In this repository, CI means automatic validation on pushes and pull requests to `main` for core project structure and setup/schema safety checks.

## Workflow behavior

The GitHub Actions workflow includes **concurrency cancellation** so repeated pushes to the same branch cancel older in-progress runs:

- `concurrency.group = ${{ github.workflow }}-${{ github.ref }}`
- `cancel-in-progress = true`

This keeps feedback fast and reduces redundant CI usage.

## CI jobs

The workflow currently has two jobs:

1. **Repository smoke validation**
   - Python 3.11 setup
   - test/lint dependency install
   - repository path and critical setup file checks
   - `pytest -q`
   - SQLFluff lint (non-blocking for now)

2. **PostgreSQL schema smoke validation**
   - temporary PostgreSQL 14 service
   - schema/setup-only SQL execution:
     - `sql/00_setup/01_extensions_schemas.sql`
     - `sql/00_setup/02_orchestration_metadata.sql`
     - `sql/00_setup/03_etl_log.sql`
   - assertive validation for required schemas, tables, function, and view

## Why SQLFluff is non-blocking right now

SQLFluff remains non-blocking to avoid failing CI while lint rules are tuned for the repository's PL/pgSQL-heavy procedural style.

## What is intentionally excluded

This is **not** full CD and **not** full end-to-end data pipeline execution.

Intentionally excluded from CI at this stage:

- landing/file_fdw CSV ingestion execution
- mapping/NF/dimensional load procedure execution
- store entity/full pipeline/master orchestration procedure execution
- DQ suite execution against loaded fixtures
- production deployment automation claims

## Maturity path

- Stage 1: repository smoke checks
- Stage 2: linting hardening
- Stage 3: schema/setup-only PostgreSQL smoke checks (current)
- Stage 4+: fixture-based data execution and deeper automated quality gates
