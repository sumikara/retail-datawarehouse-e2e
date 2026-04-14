# Naming Conventions

## Schemas
- `sl_*` for source-layer landing
- `stg` for staging/mapping/orchestration metadata
- `nf` for normalized integration entities
- `dim` for dimensional marts

## Tables
- `mapping_<entity>` for source harmonization entities
- `nf_<entity>` for normalized entities
- `dim_<entity>` and `fct_<process>` for marts

## Keys
- `<entity>_id_nk` for natural key evidence
- `<entity>_src_id` for standardized source identity
- `<entity>_id` in `nf` / `<entity>_surr_id` in `dim` for surrogate keys

## Procedures
- `load_map_*`, `load_ce_*`, `load_dim_*`, `master_*`
