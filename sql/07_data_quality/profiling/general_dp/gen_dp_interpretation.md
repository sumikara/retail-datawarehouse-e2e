# Grain Definition from Data Profiling

## Purpose

Before designing the dimensional model, source-level profiling was used to determine the **true grain** of the retail transaction data.

The goal is not only to inspect uniqueness patterns, but also to answer four modeling questions:

1. Is the source stored at **transaction-header** level or **transaction-line** level?
2. Which identifiers behave as **grouping keys** and which behave as **row-level context**?
3. Which composite key best explains **row-level uniqueness**?
4. What should be preserved in the **mapping layer**, and what should be resolved later in the **NF / 3NF layer**?

This interpretation follows the **Four-Step Dimensional Design Process**:

1. Select the business process
2. Declare the grain
3. Identify the dimensions
4. Identify the facts

---

## Step 1 — Business Process

The source data represents **retail sales transactions** across two channels:

- offline retail sales
- online retail sales

This is evidenced by transaction identifiers, timestamps, customer and product references, promotion and delivery attributes, and measurable sales values (`quantity`, `unit_price`, `total_sales`).

So this is **sales-event data**, not static master data.

---

## Profiling Scope

To infer grain, six profiling angles were used:

1. **High-level cardinality snapshot**
2. **Transaction-level grain signals**
3. **Online engagement behavior**
4. **Candidate composite key strength**
5. **Exact duplicate-group check**
6. **Diagnostic sample of repeated transactions**

---

## Profiling Results & Interpretation

### 1) High-level cardinality snapshot

Both sources have equal total volume:

- **offline:** 475,000 rows
- **online:** 475,000 rows

But transaction IDs are substantially less unique:

- **offline:** 378,278 distinct transaction IDs
- **online:** 378,155 distinct transaction IDs

So the data is clearly **not transaction-header grain**.

Also observed:

- offline has **4 store locations** and **100 employees**
- online has **475,000 distinct engagement IDs**
- both sources have **475,000 distinct customer IDs** and **475,000 distinct delivery IDs**

These very high cardinalities (especially customer/delivery/engagement) should be interpreted as **source behavior**, not automatically as business truth.

---

### 2) Repeated transaction behavior

Repeated transaction rows are strong in both channels:

- **offline:** 96,722 repeated rows
- **online:** 96,845 repeated rows

This indicates `transaction_id` is a **grouping identifier**, not a row-unique key.

---

### 3) Multi-value behavior inside repeated transactions

Multi-product transactions are common:

- **offline:** 82,781
- **online:** 82,644

And similar counts appear for delivery/promotion variation:

- offline: `multi_delivery_txn = 82,786`, `multi_promotion_txn = 82,712`
- online: `multi_delivery_txn = 82,651`, `multi_promotion_txn = 82,579`

Offline-specific variation is also strong:

- `multi_store_location_txn = 64,293`
- `multi_employee_txn = 82,090`

Online engagement variation:

- `multi_engagement_txn = 82,651`

This shows repeated transaction groups are **structural multi-line events**, not header duplicates.

---

### 4) Composite key strength

#### Candidate A
`transaction_id + product_id`

Almost unique, but not fully:

- offline: 474,989
- online: 474,988

#### Candidate B
`transaction_id + product_id + customer_id + transaction_dt`

Fully unique in both sources:

- offline: 475,000
- online: 475,000

#### Candidate C (offline-specific)
`transaction_id + product_id + store_location + employee_name + transaction_dt`

Also fully unique in offline:

- offline: 475,000

Conclusion: row-level atomic boundary is best explained by a **wider composite key**, not `transaction_id` alone.

---

### 5) Duplicate-group check

Interpretation target is correct: no meaningful exact duplicate groups were detected.

Note: the SQL was updated so this section always returns two rows (`offline`, `online`) with explicit `0` when duplicates do not exist, rather than returning an empty result set.

---

### 6) Diagnostic sample check

Sample rows confirm repeated transactions usually have aligned line-level variation:

- line counts typically 5–7
- product/delivery/promotion counts move with line_count
- online engagement counts often move with line_count
- offline store/employee counts vary within the same transaction groups

So behavior is consistent with a **multi-line event dataset**.

---

## Step 2 — Grain Declaration

### Final grain

> **One row per product-level sales event within a retail transaction.**

Expanded definition:

> Each row represents a product-level sales event for a specific customer in a specific transaction at a specific transaction timestamp, with promotion/delivery context and channel-specific attributes (offline: store & employee, online: engagement).

Therefore, source behavior matches a **transaction-line fact source**, not a transaction-header source.

---

## Step 3 — Dimension Implications

Given this grain, primary dimensional candidates are:

- Date / Time
- Customer
- Product
- Promotion
- Delivery
- Channel
- Store (offline)
- Employee (offline)
- Engagement context (online)
- Transaction ID (degenerate dimension)

Important caution: `promotion_id`, `delivery_id`, and `engagement_id` should be modeled by **observed source behavior**, not by name-based assumptions.

---

## Step 4 — Fact Implications

Measures aligned to this grain:

- `quantity`
- `unit_price`
- `total_sales`

These are valid atomic facts for a sales-line fact table.

---

## Final Modeling Decision

Model should be anchored on an atomic sales-line fact table.

### Recommended fact concept

**FactRetailSalesLine**

One row per product-level sales event within a transaction.

This preserves the most reliable detail level and supports later aggregation by transaction, customer, product, promotion, channel, and time.

---

## Architectural Implication by Layer

### Mapping Layer
Because source is already transaction-line grain and no true exact duplicate groups were found, mapping should:

- standardize and clean
- map source values
- preserve line-level variation
- block only true row-level duplication

### NF / 3NF Layer
Entity consolidation should occur in NF/3NF using:

- engineered source keys
- survivorship rules
- `ROW_NUMBER()` logic
- SCD handling
- entity resolution rules

So the layering principle is:

- mapping preserves source behavior
- NF/3NF resolves business entities

---

## Conclusion

The interpretation is **correct** and well supported by results:

- `transaction_id` is a grouping key, not row key
- repeated transactions are structural
- multi-product behavior is common
- delivery/promotion/engagement/store/employee vary within repeated groups
- exact duplicates are absent in practical terms
- row uniqueness is explained by wider composite keys

Therefore, the defensible grain is:

> **One row per product-level sales event within a retail transaction.**
