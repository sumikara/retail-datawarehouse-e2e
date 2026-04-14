# Entity & Key Design Decisions

## Key Principles
1. Preserve natural key evidence from source (`*_nk`).
2. Derive standardized source identity (`*_src_id`) for dedup and joins.
3. Assign warehouse surrogate keys in normalized and dimensional layers.

## Entity-specific summary
- **Customer**: `customer_id_nk` retained for provenance and security context; normalized surrogate `customer_id` used in joins.
- **Store**: location-city-state composition used as stable source identity when raw IDs are weak.
- **Product**: source identity composed from core descriptors to stabilize cross-source matching.
- **Promotion/Delivery/Engagement**: component-based source IDs map to reference entities and main entities.
- **Employee**: SCD-oriented source identity supports historical versions.
- **Transaction**: business id + `row_sig` used for uniqueness and idempotent loading.

## Trade-off
Composite source keys are explicit and transparent, but require disciplined standardization to avoid drift.
