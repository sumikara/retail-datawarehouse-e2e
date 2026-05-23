# Store Entity Cardinality Investigation (Analysis-Only)

## Scope

This document investigates a known store-entity build performance/cardinality issue without changing warehouse business logic.

- Investigation and diagnostics only.
- No production loader logic changes.
- No full ingestion or master-load execution.

## Store-related files/procedures inspected

1. **Mapping layer**
   - `sql/02_mapping/01_ddls_mapping.sql` (`stg.mapping_stores` structure)
   - `sql/02_mapping/02_procedures_mapping.sql` (`stg.load_map_stores()`)

2. **Normalized layer (3NF)**
   - `sql/03_normalized/01_ddls_nf.sql` (`nf.nf_stores`, key/uniqueness)
   - `sql/03_normalized/03_procedures_nf.sql` (`stg.load_ce_stores()`)

3. **Dimensional layer**
   - `sql/04_dimensions/03_procedures_dims.sql` (`stg.load_dim_stores()`)

4. **Downstream transaction linkage**
   - `sql/03_normalized/03_procedures_nf.sql` (`stg.load_ce_transactions()` join to `nf.nf_stores`)

## Suspected cardinality/fan-out risks

### Risk 1: Mapping insert-control key mismatch

In `stg.load_map_stores()`, `store_src_id` is derived from location/city/state, but target-side insert control is based on `store_location_nk + source_system + source_table`.

If one location token appears with different city/state combinations, grain can become unstable (possible suppression of distinct store keys or inconsistent rerun behavior).

### Risk 2: Address join fan-out in normalized store load

`stg.load_ce_stores()` builds `address_src_id` (`city-state-zip`) and joins to `nf.nf_addresses` on that key.

If `nf.nf_addresses.address_src_id` is duplicated in runtime data, one source store row can match multiple address rows. Even if a later `ROW_NUMBER()` filter collapses to one row per store key, intermediate row multiplication can significantly increase execution time.

### Risk 3: Downstream propagation

`stg.load_ce_transactions()` joins transactions to stores on `store_src_id`. Upstream store-key instability can propagate into downstream joins/performance.

## Joins/key patterns likely to be risky

- `mapping_stores.address_src_id`-equivalent expression -> `nf_addresses.address_src_id`
- `mapping_transactions.store_src_id` -> `nf_stores.store_src_id`
- Potential mismatch between entity grain key (`store_src_id`) and insert-control key (`store_location_nk + source triplet`)

## Keys that should preserve grain

- `store_src_id` should represent the stable store entity key at mapping/NF boundaries.
- `(source_system, source_table, store_src_id)` should not produce duplicates unexpectedly.
- `nf.nf_addresses.address_src_id` should be unique for deterministic store-address resolution.
- Transaction-to-store should resolve 1 store key to 1 NF store row.

## Diagnostic SQL and interpretation

Use `sql/troubleshooting/store_cardinality_diagnostics.sql`.

Interpret results as follows:

1. **Baseline counts**
   - Large unexpected jumps between source and joined resultsets indicate fan-out.

2. **Duplicate checks (`store_src_id`, source triplet)**
   - Any repeated store source keys indicate grain drift.

3. **Location-to-multiple-store-src ambiguity**
   - If one `store_location_nk` maps to many `store_src_id` values, insert-control key mismatch risk is real.

4. **`nf.nf_addresses.address_src_id` duplicates**
   - Any duplicates here can directly create join multiplication in store-address resolution.

5. **Store->Address join multiplicity summary**
   - `stores_with_multi_address_match > 0` or high `max_address_matches_per_store_src_id` are strong row-multiplication signals.

6. **Current join rows vs deduped-address join rows**
   - If `join_rows_current` materially exceeds `join_rows_dedup_addresses`, duplicates in address join targets are likely inflating runtime.

7. **Transactions->stores multiplicity**
   - Any store key mapping to multiple NF store IDs suggests downstream ambiguity.

## Next step (separate PR)

After confirming diagnostics in Colab/PostgreSQL, apply a minimal, targeted fix in loader keying/join strategy. Keep that change separate from this investigation-only PR.
