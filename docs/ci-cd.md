# CI/CD Foundation

## What CI means in this repository

In this repository, Continuous Integration (CI) means running automated validation on every push and pull request to `main` so core warehouse assets stay intact as the project evolves.

The current focus is delivery readiness and engineering discipline for a SQL-native PostgreSQL warehouse, without over-claiming production deployment maturity.

## What the GitHub Actions workflow validates

The `CI` workflow (`.github/workflows/ci.yml`) currently validates:

- Python environment setup for test tooling (Python 3.11)
- Development dependency installation from `requirements-dev.txt`
- Repository structure checks for core SQL layer directories
- Existence checks for critical setup SQL files
- Smoke tests via `pytest -q`
- SQL lint execution via SQLFluff (currently non-blocking)
- PostgreSQL 14 service-based schema/setup smoke execution for safe setup files only

## Why full CD/deployment is intentionally not claimed yet

This project does **not** claim a full production CD pipeline at this stage. That would require:

- environment-specific infrastructure automation,
- secrets/configuration management,
- controlled release gates, and
- deployment rollback strategy.

Those elements are intentionally deferred so the current implementation stays honest and portfolio-accurate.

## Why full PostgreSQL execution is planned for a later stage

The warehouse currently depends on local/Colab-oriented file paths and source CSV assumptions that are not guaranteed inside a generic CI runner.

To keep CI reliable and reviewer-friendly, this stage validates structure and quality gates that do not require full production datasets. Full database execution is a planned maturity step, introduced only when reproducible fixtures and ephemeral database setup are in place.

## CI/CD maturity roadmap

1. **Stage 1: smoke tests and structure validation**  
   Validate repository layout, critical SQL assets, and basic test execution.

2. **Stage 2: SQL linting hardening**  
   Introduce stricter SQLFluff configuration for PL/pgSQL patterns and progressively enforce failures.

3. **Stage 3: temporary PostgreSQL service with schema-only execution (partially implemented)**  
   ✅ Implemented now: setup/schema-only smoke execution for `sql/00_setup/01_extensions_schemas.sql`, `sql/00_setup/02_orchestration_metadata.sql`, and `sql/00_setup/03_etl_log.sql` against an ephemeral PostgreSQL 14 service.

   🚫 Still intentionally excluded: landing/file_fdw CSV ingestion, master load procedure calls, and data quality suite execution.

4. **Stage 4: mini fixture CSVs and data quality test execution**  
   Add tiny deterministic fixture datasets and execute DQ SQL suites in CI.

5. **Stage 5: Docker image build**  
   Build and validate a container image for reproducible local and CI execution.

6. **Stage 6: optional deployment to Airflow/dbt/cloud environment**  
   Add optional environment-targeted delivery automation only when infrastructure and governance are ready.


## Stage 3 implementation note

The current PostgreSQL smoke job is intentionally limited to safe setup/schema checks. It does **not** run full ELT ingestion, external CSV loading, orchestration master calls, or DQ assertion suites. This keeps CI stable on GitHub-hosted runners while validating core database object creation behavior.
