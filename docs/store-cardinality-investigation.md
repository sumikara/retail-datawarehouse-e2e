# Store Entity Cardinality Investigation (Analysis-Only)

## Scope and constraints

This note investigates the known store entity build performance/cardinality issue without changing warehouse business logic.

- No full ingestion/master orchestration execution is performed here.
- No DML fix is introduced in this document.
- Focus is on identifying probable row multiplication points and defining read-only diagnostics.

## Store loading path (files/procedures involved)

### Mapping layer
- `stg.mapping_stores` table definition: `sql/02_mapping/01_ddls_mapping.sql`.
- `stg.load_map_stores()` population logic: `sql/02_mapping/02_procedures_mapping.sql`.

`load_map_stores()` derives:
- `store_src_id = store_location || '-' || store_city || '-' || store_state`
- `store_location_nk = store_location`

and then uses target-side `NOT EXISTS` keyed by `(store_location_nk, source_system, source_table)` rather than by `store_src_id`.

### Normalized layer
- `nf.nf_stores` table definition + uniqueness: `sql/03_normalized/01_ddls_nf.sql` (`UNIQUE(store_src_id)`).
- `stg.load_ce_stores()` load logic: `sql/03_normalized/03_procedures_nf.sql`.

`load_ce_stores()` builds `address_src_id` as:
- `store_city || '-' || store_state || '-' || store_zip_code`

then joins to `nf.nf_addresses` on `address_src_id`.

### Dimensional layer
- `stg.load_dim_stores()` in `sql/04_dimensions/03_procedures_dims.sql` joins:
  - `nf_stores -> nf_addresses -> nf_cities -> nf_states`

If upstream `nf_stores` has unintended duplication pressure, this layer can inherit heavy scans.

### Transaction/factlike linkage (downstream impact)
- `stg.load_ce_transactions()` joins `stg.mapping_transactions` to `nf.nf_stores` on `store_src_id`.

If `store_src_id` grain is unstable upstream, transaction joins can become costly or ambiguous.

## Suspected risky cardinality points

## 1) Potential mismatch of insert-control key vs entity key in `load_map_stores()`

In `load_map_stores()`:
- entity key candidate is `store_src_id` (location+city+state)
- insert-control key is `store_location_nk` + source triplet

If the same `store_location_nk` appears across multiple city/state combinations (or data corrections), this can:
- suppress legitimate distinct `store_src_id` rows (data loss risk), or
- produce unstable rerun behavior, depending on sequence of source appearances.

This is not guaranteed row explosion by itself, but it can destabilize grain and downstream joins.

## 2) Address join multiplicity in `load_ce_stores()`

`load_ce_stores()` resolves stores by joining on `address_src_id` to `nf.nf_addresses`.

Expected safe condition:
- `nf.nf_addresses.address_src_id` is effectively unique in runtime data.

If not unique in practice (e.g., legacy duplicates from prior runs or incomplete constraints), one source store row can match multiple address rows, multiplying rows before the `ROW_NUMBER()` collapse.

Even when `ROW_NUMBER()` filters to `rn=1`, the intermediate explosion can still cause major performance degradation.

## 3) Composite string key fragility

Store grain is represented by concatenated text keys:
- `store_src_id` (location+city+state)
- `address_src_id` (city+state+zip)

Any inconsistent normalization (whitespace/casing/placeholder values) can increase near-duplicates and join fan-out risk.

## Keys that should preserve grain (diagnostic expectation)

- `stg.mapping_stores` should be approximately 1 row per business store key (as currently modeled by `store_src_id`).
- `nf.nf_stores.store_src_id` should remain unique (enforced by `UNIQUE`).
- `nf.nf_addresses.address_src_id` should be unique for deterministic store→address resolution.
- Join expectation for store load path should be close to 1:1:
  - mapping store row -> resolved address row -> final nf store row.

## Read-only confirmation queries

Use:
- `sql/troubleshooting/store_cardinality_diagnostics.sql`

The script includes read-only checks for:
1. Base counts in `mapping_stores`, `nf_addresses`, `nf_stores`.
2. Duplicate checks for `store_src_id`, `store_location_nk`, `address_src_id`.
3. Join multiplicity checks from `mapping_stores` to `nf_addresses`.
4. Fan-out distribution by `store_src_id` (max/min/avg matches).
5. Comparison of “current join cardinality” vs “deduped address cardinality”.
6. Downstream join multiplicity check from `mapping_transactions` to `nf_stores` via `store_src_id`.

## How to interpret row-explosion signals

Strong indicators of cardinality issues:
- Any duplicate rows returned for `nf.nf_addresses.address_src_id`.
- `mapping_stores -> nf_addresses` producing substantially more joined rows than source rows.
- `MAX(matched_address_rows)` per `store_src_id` greater than 1 for many keys.
- Transaction→store join showing `store_src_id` mapping to multiple `store_id` values.

## Next step (not in this PR)

After confirming diagnostics in Colab/PostgreSQL, introduce a minimal logic fix in a separate PR, likely around deterministic key usage and/or pre-deduplication before store-address resolution.
