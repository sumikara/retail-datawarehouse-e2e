# Data Dictionary (Core Fields)

## Scope
This dictionary covers business-critical fields across mapping (`stg.mapping_*`), normalized (`nf.*`), and marts (`dim.*`) layers.

## Transaction Domain
| Field | Layer | Type | Meaning | Notes |
|---|---|---|---|---|
| `transaction_id` / `transaction_src_id` | mapping, nf, dim | varchar | Transaction business identifier | Degenerate key in fact.
| `transaction_dt` / `transaction_date` | nf, dim | timestamp/date | Transaction timestamp/date | Date key derived for reporting.
| `total_sales` | nf, dim | numeric(10,2) | Total sales amount | Main additive measure.
| `quantity` | nf, dim | int | Units sold | Additive measure.
| `unit_price` | nf, dim | numeric(10,2) | Unit-level price | Used for quality checks.
| `discount_applied` | nf, dim | numeric(10,2) | Discount amount | Should be non-negative.

## Customer Domain
| Field | Layer | Type | Meaning | Notes |
|---|---|---|---|---|
| `customer_id_nk` | mapping, nf | varchar | Raw/natural customer id | Used for lineage and RLS context.
| `customer_src_id` | mapping, nf | varchar | Standardized source identity | Used for source harmonization.
| `address_id` | nf | bigint | Address foreign key | Resolves to city/state.

## Product Domain
| Field | Layer | Type | Meaning | Notes |
|---|---|---|---|---|
| `product_id_nk` | mapping | varchar | Raw product id | Source provenance.
| `product_src_id` | mapping, nf | varchar | Derived product identity | Name/category/brand/material lineage.
| `product_category_id` | nf | bigint | Product category FK | Drives dim product category analysis.

## Dimensional Surrogate Keys
| Key | Table | Usage |
|---|---|---|
| `customer_surr_id` | `dim.dim_customers` | Fact FK for customer analytics |
| `store_surr_id` | `dim.dim_stores` | Fact FK for channel/location analytics |
| `product_surr_id` | `dim.dim_products` | Fact FK for assortment analytics |
| `promotion_surr_id` | `dim.dim_promotions` | Fact FK for campaign analytics |
| `delivery_surr_id` | `dim.dim_deliveries` | Fact FK for logistics analytics |
| `engagement_surr_id` | `dim.dim_engagements` | Fact FK for service/engagement analytics |
| `employee_surr_id` | `dim.dim_employees_scd` | Fact FK for workforce analytics |
| `transaction_date_sk` | `dim.dim_dates` | Calendar conformance |
