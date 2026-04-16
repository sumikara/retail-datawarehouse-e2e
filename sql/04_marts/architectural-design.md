## dim Data Mart Layer

### Purpose and Design Intent

The **dim** layer is the warehouse’s **data mart / star schema layer**, designed for **reporting, analytical querying, and BI consumption**. While the **nf** layer focuses on normalization, integration, and anomaly-safe entity resolution, the **dim** layer reshapes that integrated data into dimensional models optimized for business analysis.

Its main objective is to support **dimensional modeling** by organizing data into **facts and dimensions**, improving analytical usability, and preparing the warehouse for BI tools and reporting workloads.

### Core Rules

- The primary purpose of this layer is **reporting and analytics**.
- Dimensional modeling must be implemented in this layer.
- Partitioning is allowed **only** in `dim`.
- Rolling window logic must be handled in `dim`.
- Rolling window operations should be parameterized through **dynamic SQL**.
- The dimensional layer must use **surrogate keys**, not 3NF-style composite primary key logic.
- In `dim`, `source_system` must always be set to `'nf'`.

### Partitioning and Rolling Window Strategy

**Partitioning is allowed only in `dim`**, because this is the layer where active reporting workloads and large fact-table access patterns justify physical segmentation.

The **rolling window** approach also belongs exclusively to the data mart layer. Since reporting usually focuses on active or recent periods, rolling window operations should be implemented dynamically and parameterized through **dynamic SQL**. This keeps analytical storage manageable and makes time-based retention logic explicit and reusable.

### Key Management

The **dim** layer does not follow the same key logic as **nf**. This layer is built for analytical modeling, so it must rely on **surrogate keys**.

Key rules are:

- 3NF surrogate keys become `*_src_id` in `dim`
- `dim` generates its own new surrogate keys
- dimension tables must use DM-level surrogate keys
- fact tables must reference dimension surrogate keys
- `source_system` must always be `'nf'`

This creates a clean separation between:

- **enterprise integration identity** in `nf`
- **analytical identity** in `dim`

In dimensional modeling terms, this is the layer where the effective “composite key kingdom” emerges: not as a traditional relational composite PK, but as a **fact-level structure built from multiple dimension surrogate keys** such as `customer_surr_id`, `product_surr_id`, `date_surr_id`, `store_surr_id`, and similar dimension references. This combination is what gives the fact table its analytical context and makes it optimal for BI tools.

In cases where source-side logic previously involved composite keys, **the DM layer should only carry the 3NF identifier as `*_src_id`**. It should **not** carry technical uniqueness through `start_dt` inside a primary key structure. Instead, `start_dt` must exist as a business-valid timestamp column where required, especially for historized dimensions, but not as part of a `(src_id, start_dt)` primary key or mandatory unique-key pattern.

### Constraint Strategy

Constraint usage in **dim** must remain pragmatic and workload-aware.

Rules:

- avoid relying on **UNIQUE constraints** as the main mechanism for controlling dimensional uniqueness
- uniqueness should primarily be managed through **load logic**
- **do not apply unique or primary key constraints on `(src_id, start_dt)` in `dim`**
- **do not assign primary keys on `start_dt`**
- unique constraints may be used if justified, but they are **optional, not mandatory**

This means dimensional correctness can be enforced through:

- controlled `MERGE` logic
- `INSERT + UPDATE` patterns
- procedural matching rules
- source-aware change handling

The key idea is that analytical models should remain flexible enough for reporting workloads, while still being protected by deterministic load logic.

### Timestamp Standard

Timestamp handling in the DM layer must be explicit and consistent.

Required standard:

- `start_dt` must be `TIMESTAMP`, **not** `DATE`
- `insert_dt` must be `TIMESTAMP`
- `update_dt` must be `TIMESTAMP`

This is especially important in historized dimensions, because reporting and SCD interpretation often require time precision beyond calendar-day granularity.

### Fact-Dimension Modeling Logic

The **dim** layer is where normalized entities are transformed into dimensional structures. In this layer:

- dimension tables receive their own surrogate keys
- fact tables reference those dimension surrogate keys
- the fact table gains analytical richness by combining multiple dimension references into one business event

So while the layer does not use a strict relational composite PK in the classical 3NF sense, it does operate with a **large analytical composite key mindset**: the business meaning of a fact row is defined by the intersection of multiple dimension keys.

Typical examples include combinations such as:

- `customer_surr_id`
- `product_surr_id`
- `date_surr_id`
- `store_surr_id`
- `promotion_surr_id`
- `employee_surr_id`

This is what makes the star schema highly efficient for BI and analytical queries.

### nf to dim Transition Rule

The relationship between the normalized and dimensional layers must remain explicit:

- the surrogate key generated in **nf** becomes the `*_src_id` reference in **dim**
- **dim** then creates its own analytical surrogate key for the target dimension
- `source_system` in the DM layer must always be `'nf'`

This ensures that dimensional models stay analytically optimized without losing integration lineage.

### Summary Table

| Area | Rule |
|---|---|
| Layer purpose | Reporting, analytics, and dimensional modeling |
| Partitioning | Allowed only in `dim` |
| Rolling window | Implement only in `dim`, parameterized with dynamic SQL |
| Key strategy | Use DM-level surrogate keys |
| 3NF handoff | 3NF surrogate keys become `*_src_id` in `dim` |
| Source system | Always `'nf'` |
| Fact modeling | Fact tables reference dimension surrogate keys |
| Composite key mindset | Analytical meaning comes from the combination of dimension SKs in the fact table |
| Constraint rule | Do not apply PK or UNIQUE on `(src_id, start_dt)` |
| `start_dt` rule | Must be `TIMESTAMP`, not `DATE`, and not part of PK |
| Timestamp standard | `insert_dt`, `update_dt`, `start_dt` must all be `TIMESTAMP` |
| Uniqueness control | Prefer load logic (`MERGE`, `INSERT + UPDATE`, matching rules) over mandatory unique constraints |
