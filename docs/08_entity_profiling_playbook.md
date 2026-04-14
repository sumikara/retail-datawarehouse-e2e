# Entity-by-Entity Profiling Playbook

## Objective
Derive stable entity boundaries and key rules from profiling outputs before irreversible modeling decisions.

## Per-entity checklist
- Grain assumption statement
- Candidate natural keys and uniqueness ratio
- Null profile for critical attributes
- Domain/value normalization needs
- Source overlap and conflict behavior

## Deliverable template
For each entity (`customer`, `store`, `product`, `promotion`, `delivery`, `engagement`, `employee`, `transaction`):
1. Business definition
2. Key strategy (`*_nk`, `*_src_id`, surrogate)
3. Data quality risks
4. Normalization and mart implications
