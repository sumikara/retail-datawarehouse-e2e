## .nf / Core / Integration Layer

### Purpose and Design Intent

The **nf** layer is the warehouse’s **core integration layer**, responsible for enforcing normalization rules, preserving **data integrity**, and resolving source anomalies **without causing data loss**. Its purpose is not only to store cleaned data, but to transform source-level ambiguity into stable, relationally controlled enterprise structures.

This layer is designed around a key principle: unreliable source identifiers must remain available for **traceability** (src_ids), while actual warehouse relationships must be governed by newly generated, system-controlled keys (**surrogate key by SEQUENCE -nextval-**). Therefore, every target table in **nf** must be loaded through its own dedicated procedure, and required object existence checks must be handled inside procedural logic to support controlled reruns and repeatable deployments.

### Core Rules

- Normalization rules must be enforced in this layer.
- Data integrity must be preserved.
- Source anomalies must be resolved **without data loss**.
- Each 3NF target table must have its **own load procedure**.
- Required object existence must be checked inside procedures.
- All required **nf_*** objects in the **nf** layer must be created and verified.
- All required privileges must be granted to the **stg** schema.

### (NO) Partitioning Rule 

**Partitioning is strictly forbidden in `nf`.**  
The **3NF layer must remain fully normalized and integration-focused**. Partitioning is allowed **only in `dim`**, where analytical access patterns and fact-table scale justify physical segmentation.

### Surrogate Key and ID Management

Source-side broken or unreliable identifiers are never trusted as warehouse keys. They are preserved only as `*_id_nk` columns for **lineage, auditability, and traceability**. Each entity in `nf` must instead receive a newly generated surrogate key via **explicit `SEQUENCE` + `nextval()`** logic.

**This means:**
- original source (`*_id_nk`) IDs and engineered keys (`*_src_id`) (combination of more than one column from source to reference PK column in nf layer) remain stored for reference only
- surrogate keys drive relational integrity
- faulty source IDs do not define uniqueness
- **system-generated IDs must be explicit and traceable**
- composite key logic is **not** the primary determinant at this layer, except where natural hierarchy explicitly requires it
- Each target table must also preserve the **SOURCE TRIBLET**: 

- `*_src_id`
- `source_system`
- `source_table`

This guarantees that lineage is never lost, even after entity integration. (Traceability)

### Source System Handling in 3NF

For **deduplicated datasets**, `source_system` must always be set to `'bl_cl'`, and `source_table` must point to the corresponding deduplicated or mapping table coming from the cleansing layer.

For **non-deduplicated entities** such as **customers, employees, engagements, and stores**, the original `source_system` and `source_table` values must be preserved, because these entities may still carry source-specific duplication patterns and require lineage to remain visible in the core layer.

Even if duplicates exist across both sources — for example the same category such as **`Toys`** appearing in both online and offline data — only **one integrated row** should be stored in **nf**, and its lineage should indicate that it originated from the deduplicated `bl_cl` layer.

### Query and Load Standards

The **nf** layer follows strict SQL standards to guarantee correctness, rerunnability, and predictable integration behavior:

- **LEFT JOIN only** — `INNER JOIN` is never used, because row preservation must be intentional and data loss must be avoided.
- **UNION ALL only** — `UNION` is never used, because deduplication must be controlled explicitly, not implicitly through set semantics.
- **`ROW_NUMBER()` must be used in every insert pipeline** as an additional deduplication guarantee.
- **`SELECT DISTINCT`** for lookup tables.
- **`WHERE NOT EXISTS`** or **`LEFT JOIN ... IS NULL`** must be used for idempotent insert logic.
- Joins must always use the **full triplet key**:
  - `*_src_id`
  - `source_system`
  - `source_table`

This join policy ensures that entity resolution remains source-aware and that rows from different systems are never incorrectly merged based on ID alone.

### Idempotency and Procedural Expectations

All **nf** procedures must be **repeatable** and **idempotent**. Re-executing the same procedure with the same source data must not generate duplicate inserts or inconsistent state.

Procedural expectations include:

- one dedicated load procedure per target table
- existence checks for required objects inside the procedure
- use of at least one **`SETOF`** or **`RETURNS TABLE`** style function
- mandatory **`EXCEPTION`** block in every procedure

These requirements make the layer operationally robust, testable, and aligned with controlled ETL behavior.

### SCD Type 2 Rule for Employees

A special rule applies to the **employees** entity. **SCD Type 2** logic must be implemented directly in **nf** to preserve historical changes such as position or salary updates.

The SCD2 implementation must include:

- **change detection** on non-key descriptive attributes
- comparison of **new hash vs existing hash** using `md5(concat_ws(...))`
- closing the current version by updating:
  - `end_dt`
  - `is_active = FALSE`
- inserting the new version with:
  - `start_dt = current_timestamp`
  - `is_active = TRUE`

If no attribute-level change is detected, **no new version should be created**.

Whenever possible, `start_dt` and `end_dt` should be stored as **`TIMESTAMP`**, not `DATE`.

A critical modeling rule also applies here: the **employees SCD2 table in nf must not be directly connected to the transactions table through a foreign key**. Historical employee resolution is intentionally deferred to the **data mart layer**, where analytical interpretation is more appropriate.

### Referential Integrity and Data Integrity Logic

The purpose of **nf** is not only normalization, but also anomaly-safe integration. Source anomalies must be resolved **without losing records**.

For example:

- broken original IDs remain preserved as `*_src_id`
- surrogate keys provide stable warehouse identity
- source lineage remains attached through triplet columns
- row uniqueness is enforced through procedural and query design rather than trusting raw business identifiers

This structure allows the warehouse to keep problematic source data visible, while preventing that same source inconsistency from corrupting relational integrity.

### Hierarchy and Natural Key Interpretation

Profiling results must directly influence **3NF** modeling rules.

For example, if the same `city_name` appears under multiple different states, then **city name alone cannot be the primary determinant** in the city entity. In such a case, the 3NF city structure must be modeled using a **composite natural key** such as:

- `city_name + state_name`

This is not treated as a warehouse surrogate key strategy, but as a necessary modeling correction to preserve hierarchical integrity.

Similarly, if a single zip code appears across multiple cities, it proves that zip code is **not a reliable key**. In that case, zip code must remain only an **attribute**, never a key.

Therefore, in the **3NF** layer:

- only `*_states` and `*_cities` style entities should hold normalized location hierarchy
- transactional tables should reference them through simple foreign keys such as `city_id` or `state_id`
- unstable source attributes like zip code must not define relational identity

### nf to dim Transition Rule

In the dimensional layer, the surrogate key generated in **nf** becomes the effective source-side identifier for dimensional modeling. In other words:

- **nf surrogate keys become the `*_src_id` reference in `dim`**
- **dim creates its own new surrogate keys** for analytical modeling

This preserves a clean separation between:

- enterprise integration identity in **3NF**
- analytical identity in **data marts**

### Summary Table

| Area | Rule |
|---|---|
| Layer purpose | Enforce normalization, preserve data integrity, resolve anomalies without data loss |
| Partitioning | Not allowed in `nf`; allowed only in `dim` |
| Key strategy | Preserve broken source IDs as `*_src_id`; generate surrogate keys with `SEQUENCE` |
| Lineage | Always keep `*_src_id + source_system + source_table` |
| Source handling | Deduplicated rows use `source_system = 'bl_cl'`; non-deduplicated entities keep original source metadata |
| Join standard | `LEFT JOIN` only |
| Union standard | `UNION ALL` only |
| Dedup guarantee | `ROW_NUMBER()` in every insert pipeline |
| Idempotency | `WHERE NOT EXISTS` or `LEFT JOIN ... IS NULL` |
| SCD2 | Implement in `employees`; detect change by hash; close old version, insert new active version |
| Employee FK rule | No direct FK from employees SCD2 table to transactions in 3NF |
| Hierarchy rule | Use composite natural key where hierarchy demands it (for example `city_name + state_name`) |
| Zip code rule | Attribute only, never a key |
| DM transition | 3NF surrogate key becomes `*_src_id` in `dim`; DM generates its own surrogate key |
