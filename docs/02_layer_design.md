# Layer Design

## L0 Source Landing (`sl_online_retail`, `sl_offline_retail`)
- Raw-like source mirror tables loaded from CSV inputs.
- Purpose: preserve source fidelity and replayability.

## L1 Staging (`stg`)
- Standardization and cleansing (trim/coalesce/typing) occurs here.
- Master ingestion procedures track file-level and step-level metadata.

## L2 Mapping (`stg.mapping_*`)
- Entity-by-entity mapping tables derive source identities (`*_src_id`) and keep natural keys (`*_nk`).
- Purpose: explicit lineage and controlled key resolution into normalized entities.

## L3 Normalized (`nf`)
- Integrated 3NF entities with surrogate keys and FK lineage.
- Includes reference entities (states/cities/categories/types) and business entities.

## L4 Dimensional (`dim`)
- Denormalized dimensions for BI consumption.
- Partitioned transaction fact table with conformed date key.

## Cross-cutting
- Logging (`stg.etl_log`) and orchestration metadata (`etl_batch_run`, `etl_step_run`, `etl_file_registry`).
