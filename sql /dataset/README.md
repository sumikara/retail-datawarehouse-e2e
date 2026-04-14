# Dataset

## Source

This project is based on the Kaggle dataset **“Retail Sales and Customer Behavior Analysis”** by `utkalk`.

According to the dataset description, the source data **simulates a retail environment** with around **1 million rows** and **100+ columns**, covering customer information, transactions, product details, promotions, geography, customer behavior metrics, and derived business features. It is presented as a broad practice dataset for tasks such as sales prediction and churn analysis rather than as a documented production export from a real operational retail system.

For that reason, in this project I treat it as a **simulated source dataset** and then reshape it into a structure that better fits a Data Warehouse learning scenario.

## Why I did not use the Kaggle file as-is

The original Kaggle file is wide and already contains many precomputed or analysis-oriented fields such as customer behavior metrics, sales summaries, and derived features. For a DWH project, I did not want to start from a dataset that already embeds too much reporting logic.

Instead, I intentionally simplified the source and used it as a **raw training base** for designing cleaner operational-style inputs and deriving business metrics later in the warehouse and mart layers.

## Data adaptation strategy for the DWH project

After downloading the CSV, I adapted the dataset in three steps.

### 1. Splitting one source into two operational channels

To simulate the case where the same company keeps data in multiple source systems, I split the 1,000,000-row dataset into two 500,000-row parts:

- the first **500,000** rows were used as an **online retail** source
- the remaining **500,000** rows were used as an **offline retail** source

This was not intended to claim that the original Kaggle dataset naturally contains two true production channels. It was a deliberate modeling decision to demonstrate **source integration, conformance, and merging logic** inside the warehouse.

### 2. Creating bulk-load and incremental-load scenarios

To simulate initial and ongoing ingestion, I further divided each 500,000-row source into:

- **95% bulk load**
- **5% incremental load**

I applied this logic to both the online and offline versions. The goal was to create a realistic training setup for testing initial population and subsequent incremental loading patterns.

### 3. Adding source-specific entities

To make the two source systems similar enough to be integrated, but different enough to require modeling decisions, I extended the data with additional synthetic entities and columns using Python.

Common/shared business structure was preserved across both sources, while channel-specific entities were added:

- **shared/common structure** across both sources
- **offline-specific entities** such as `employee` and store-related attributes
- **online-specific entities** such as `engagement`
- additional delivery-related attributes used across the modeled flow

This step was important because a DWH project becomes more meaningful when multiple sources share core entities but still keep channel-specific differences.

## Design decision on derived metrics

I intentionally did **not** keep many of the precomputed analytical columns from the Kaggle source, even though they were available.

Examples include:
- pre-aggregated customer behavior metrics
- pre-derived business indicators
- reporting-oriented summary fields

Instead, I preferred to calculate such fields later in downstream layers, especially in the mart/reporting side, where I could model them explicitly as part of the analytical design.

This decision let me:
- keep the source layer simpler
- avoid mixing source data with reporting logic
- recreate business calculations myself
- demonstrate how metrics can be derived in a more transparent DWH flow

In other words, rather than inheriting analysis-ready fields, I chose to rebuild them in a controlled way through warehouse design and later analytical transformations.

## Final modeled source structures

### Online source

```text
customer_id
gender
marital_status
transaction_id
transaction_date
product_id
product_category
quantity
unit_price
discount_applied
day_of_week
week_of_year
month_of_year
product_name
product_brand
product_stock
product_material
promotion_id
promotion_type
promotion_start_date
promotion_end_date
customer_zip_code
customer_city
customer_state
customer_support_calls
date_of_birth
payment_method
delivery_id
delivery_type
delivery_status
shipping_partner
membership_date
website_address
order_channel
customer_support_method
issue_status
product_manufacture_date
product_expiry_date
total_sales
promotion_channel
last_purchase_date
app_usage
website_visits
social_media_engagement
engagement_id
```

### Offline source

```text
customer_id
gender
marital_status
transaction_id
transaction_date
product_id
product_category
quantity
unit_price
discount_applied
day_of_week
week_of_year
month_of_year
product_name
product_brand
product_stock
product_material
promotion_id
promotion_type
promotion_start_date
promotion_end_date
customer_zip_code
customer_city
customer_state
store_zip_code
store_city
store_state
date_of_birth
payment_method
delivery_id
delivery_type
delivery_status
shipping_partner
employee_salary
membership_date
store_location
last_purchase_date
total_sales
product_manufacture_date
product_expiry_date
promotion_channel
employee_name
employee_position
employee_hire_date
```

## Project note

This dataset adaptation was a deliberate educational design choice for the DWH project. The goal was not to preserve the Kaggle file exactly as published, but to transform a simulated retail dataset into a more suitable multi-source warehouse scenario with:

- source-system separation
- bulk and incremental loading
- shared and channel-specific entities
- clearer control over downstream metric creation
