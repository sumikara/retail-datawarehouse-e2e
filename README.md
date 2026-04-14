#   WORK IN PROGRESS.. #
#Retail E2E Data Warehouse (PostgreSQL)

End-to-end retail data warehouse in PostgreSQL with ETL, dimensional modeling (star/snowflake), Python data profiling, and data quality checks.
I designed a hybrid warehouse architecture: normalized integration core (Inmon-style EDW discipline) feeding dimensional marts (Kimball-style star schema) with process-level orchestration, observability, and data quality controls.

## Status
In progress — scripts and documentation are being migrated and organized.
I designed a hybrid warehouse architecture: normalized integration core (Inmon-style EDW discipline) feeding dimensional marts (Kimball-style star schema) with process-level orchestration, observability, and data quality controls.

## Repository structure (high level)
- `sql/` – ETL and modeling scripts organized by layers
- `docs/` – engineering notes and decisions (performance, modeling, data quality)
- `diagrams/` – ERD and other visuals

> Detailed dataset notes and run instructions will be added after the script migration is complete.

---

## Architecture
This repository implements a layered PostgreSQL retail warehouse with explicit orchestration and observability.
The flow is designed as a hybrid model: **normalized integration core (Inmon-style)** feeding **dimensional marts (Kimball-style)**.

### Pipeline
`external_csv` → `staging_raw` → `staging_clean` → `mapping` → `nf (3NF core)` → `dim` → `fact` → `reporting`

### Stage responsibilities
| Stage | Primary SQL area | Responsibility | Output |
|---|---|---|---|
| Setup & metadata | `sql/00_setup` | Schemas, extensions, orchestration metadata, ETL logging primitives | Control tables, log utilities |
| Staging ingestion | `sql/01_staging` | Raw file load + standardization + clean staging build | Standardized source-layer retail tables |
| Mapping | `sql/02_mapping` | Entity-by-entity source key shaping and canonical source identifiers | Mapping tables for customers/stores/products/... |
| Normalized core (3NF) | `sql/03_normalized` | Reference + business entities with surrogate keys and lineage | Integrated EDW-style normalized model |
| Dimensional marts | `sql/04_marts` | Denormalized dimensions, SCD handling, partitioned fact | Analytics-ready star schema |
| Master orchestration | `sql/05_orchestrastion` | Batch/step-driven execution flow across all layers | End-to-end repeatable pipeline runs |
| Security | `sql/06_security` | Roles, grants, RLS policy layer | Access boundary and data governance controls |
| Data quality | `sql/07_data_quality` | Profiling/reconciliation/test scaffolding | DQ evidence and defect visibility |

---

## Grain Contract
### Declared analytical grain
**One row in fact = one retail sales transaction event (`transaction_src_id`) at transaction date grain.**

### Fact scope (current design)
- Fact table target: `dim.fct_transactions_dd_dd`
- Measures:
  - `total_sales`
  - `quantity`
  - `unit_price`
  - `discount_applied`
- Degenerate/business identifiers:
  - `transaction_src_id`
  - `payment_method`

### Conformance keys
- `transaction_date_sk` (date dimension)
- `customer_surr_id`
- `store_surr_id`
- `product_surr_id`
- `promotion_surr_id`
- `delivery_surr_id`
- `engagement_surr_id`
- `employee_surr_id`

---

## Bus Matrix (Business Process × Dimensions)

Legend: ✅ direct FK use in the fact, ⚪ optional/future extension.

| Business Process | Date | Customer | Store | Product | Promotion | Delivery | Engagement | Employee |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Retail transaction (online/offline unified sales) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Campaign effectiveness analysis | ✅ | ✅ | ✅ | ✅ | ✅ | ⚪ | ✅ | ⚪ |
| Fulfillment performance analysis | ✅ | ⚪ | ✅ | ✅ | ⚪ | ✅ | ⚪ | ⚪ |
| Workforce / service interaction analysis | ✅ | ✅ | ✅ | ⚪ | ⚪ | ⚪ | ✅ | ✅ |

---

## Analytic Navigation (How to read this project)
1. **Start with orchestration** (`sql/05_orchestrastion/master_full_load.sql`) to understand execution order.
2. **Inspect mapping layer** for key derivation and source lineage.
3. **Inspect 3NF layer** for integrated entities and surrogate key design.
4. **Inspect marts** for final star-schema consumption model and partition strategy.
5. **Review logs + security + DQ** for operational maturity and governance.

---

## Design Notes (Why this structure)
- Entity mapping before NF makes key decisions explicit and testable.
- NF layer protects integration semantics before denormalized analytics views.
- Dim/fact layer optimizes analytical access patterns and BI workloads.
- Explicit metadata tables and step logs support rerunability, auditability, and debuggability.
