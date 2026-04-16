# DWH SCHEMA (FOR DATA QUALITY TEST DESIGN)

## LAYER FLOW
SOURCE -> LANDING -> MAPPING -> NF -> DIM

## SOURCE
- ONLINE CSV
- OFFLINE CSV

## LANDING
- sl_online_retail
  - frg_online_retail
  - src_online_retail_raw
  - src_online_retail
- sl_offline_retail
  - frg_offline_retail
  - src_offline_retail_raw
  - src_offline_retail

## MAPPING

### stg.mapping_customers
- customer_id_nk : VARCHAR(20)
- gender : VARCHAR(20)
- marital_status : VARCHAR(20)
- birth_of_dt : DATE
- membership_dt : DATE
- customer_zip_code : VARCHAR(30)
- customer_city : VARCHAR(100)
- customer_state : VARCHAR(100)
- last_purchase_dt : TIMESTAMP
- customer_src_id : VARCHAR(255)
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_stores
- store_src_id : VARCHAR(255)
- store_name : VARCHAR(100)
- store_zip_code : VARCHAR(30)
- store_city : VARCHAR(100)
- store_state : VARCHAR(100)
- store_location_nk : VARCHAR(100)
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_products
- product_id_nk : VARCHAR(100)
- product_src_id : VARCHAR(255)
- product_category : VARCHAR(100)
- product_name : VARCHAR(100)
- product_brand : VARCHAR(100)
- product_stock : INTEGER
- product_material : VARCHAR(100)
- product_manufacture_dt : TIMESTAMP
- product_expiry_dt : TIMESTAMP
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_promotions
- promotion_id_nk : VARCHAR(100)
- promotion_type : VARCHAR(100)
- promotion_channel : VARCHAR(100)
- promotion_start_dt : TIMESTAMP
- promotion_end_dt : TIMESTAMP
- promotion_src_id : VARCHAR(255)
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_deliveries
- delivery_id_nk : VARCHAR(100)
- delivery_type : VARCHAR(100)
- delivery_status : VARCHAR(100)
- shipping_partner : VARCHAR(100)
- delivery_src_id : VARCHAR(255)
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_engagements
- engagement_id_nk : VARCHAR(100)
- customer_support_calls : INTEGER
- website_address : VARCHAR(250)
- order_channel : VARCHAR(100)
- customer_support_method : VARCHAR(100)
- issue_status : VARCHAR(100)
- app_usage : VARCHAR(100)
- website_visits : INTEGER
- social_media_engagement : VARCHAR(100)
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_employees
- employee_src_id : VARCHAR(100) [NOT NULL]
- employee_name_nk : VARCHAR(100)
- employee_position : VARCHAR(100)
- employee_salary : NUMERIC(10,2)
- employee_hire_date : DATE
- observed_ts : TIMESTAMP
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### stg.mapping_transactions
- transaction_id : VARCHAR(100)
- transaction_dt : TIMESTAMP
- total_sales : NUMERIC(10,2)
- payment_method : VARCHAR(100)
- quantity : INTEGER
- unit_price : NUMERIC(10,2)
- discount_applied : NUMERIC(10,2)
- day_of_week : VARCHAR(100)
- week_of_year : INTEGER
- month_of_year : INTEGER
- customer_id_nk : VARCHAR(100)
- product_id_nk : VARCHAR(100)
- promotion_id_nk : VARCHAR(100)
- delivery_id_nk : VARCHAR(100)
- engagement_id_nk : VARCHAR(100)
- employee_name_nk : VARCHAR(100)
- employee_hire_date : DATE
- customer_city : VARCHAR(100)
- customer_state : VARCHAR(100)
- store_zip_code : VARCHAR(30)
- store_city : VARCHAR(100)
- store_state : VARCHAR(100)
- store_location_nk : VARCHAR(100)
- product_name : VARCHAR(100)
- product_category : VARCHAR(100)
- product_brand : VARCHAR(100)
- product_material : VARCHAR(100)
- promotion_type : VARCHAR(100)
- promotion_channel : VARCHAR(100)
- promotion_start_dt : TIMESTAMP
- promotion_end_dt : TIMESTAMP
- delivery_type : VARCHAR(100)
- shipping_partner : VARCHAR(100)
- customer_src_id : VARCHAR(255)
- product_src_id : VARCHAR(255)
- promotion_src_id : VARCHAR(255)
- delivery_src_id : VARCHAR(255)
- store_src_id : VARCHAR(255)
- city_src_id : VARCHAR(255)
- employee_src_id : VARCHAR(255)
- row_sig : TEXT [UNIQUE INDEX ux_mapping_transactions_rowsig]
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

## NF

### nf.nf_states
- state_id : BIGINT [PK]
- state_src_id : VARCHAR(100) [NOT NULL, UNIQUE]
- state_name : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_cities
- city_id : BIGINT [PK]
- city_src_id : VARCHAR(150) [NOT NULL, UNIQUE]
- city_name : VARCHAR(100) [NOT NULL]
- state_id : BIGINT [NOT NULL, FK -> nf.nf_states.state_id]
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_addresses
- address_id : BIGINT [PK]
- address_src_id : VARCHAR(200) [NOT NULL, UNIQUE]
- zip_code : VARCHAR(30)
- city_id : BIGINT [NOT NULL, FK -> nf.nf_cities.city_id]
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_product_categories
- product_category_id : BIGINT [PK]
- product_category_src_id : VARCHAR(100) [NOT NULL, UNIQUE]
- product_category_name : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_promotion_types
- promotion_type_id : BIGINT [PK]
- promotion_type_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- promotion_type_name : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_shipping_partners
- shipping_partner_id : BIGINT [PK]
- shipping_partner_src_id : VARCHAR(100) [NOT NULL, UNIQUE]
- shipping_partner_name : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_customers
- customer_id : BIGINT [PK]
- customer_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- customer_id_nk : VARCHAR(100) [NOT NULL]
- gender : VARCHAR(20) [NOT NULL]
- marital_status : VARCHAR(20) [NOT NULL]
- birth_of_dt : DATE
- membership_dt : DATE
- last_purchase_dt : TIMESTAMP
- address_id : BIGINT [NOT NULL, FK -> nf.nf_addresses.address_id]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_stores
- store_id : BIGINT [PK]
- store_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- store_name : VARCHAR(100)
- store_location_nk : VARCHAR(100)
- address_id : BIGINT [NOT NULL, FK -> nf.nf_addresses.address_id]
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_products
- product_id : BIGINT [PK]
- product_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- product_id_nk : VARCHAR(100)
- product_category_id : BIGINT [NOT NULL, FK -> nf.nf_product_categories.product_category_id]
- product_name : VARCHAR(100) [NOT NULL]
- product_brand : VARCHAR(100) [NOT NULL]
- product_stock : INTEGER
- product_material : VARCHAR(100) [NOT NULL]
- product_manufacture_dt : TIMESTAMP
- product_expiry_dt : TIMESTAMP
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_promotions
- promotion_id : BIGINT [PK]
- promotion_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- promotion_id_nk : VARCHAR(100)
- promotion_type_id : BIGINT [NOT NULL, FK -> nf.nf_promotion_types.promotion_type_id]
- promotion_channel : VARCHAR(100) [NOT NULL]
- promotion_start_dt : TIMESTAMP
- promotion_end_dt : TIMESTAMP
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_deliveries
- delivery_id : BIGINT [PK]
- delivery_src_id : VARCHAR(255) [NOT NULL, UNIQUE]
- delivery_id_nk : VARCHAR(100)
- shipping_partner_id : BIGINT [NOT NULL, FK -> nf.nf_shipping_partners.shipping_partner_id]
- delivery_type : VARCHAR(100) [NOT NULL]
- delivery_status : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_engagements
- engagement_id : BIGINT [PK]
- engagement_src_id : VARCHAR(100) [NOT NULL, UNIQUE]
- engagement_id_nk : VARCHAR(100)
- customer_support_calls : INTEGER
- website_address : VARCHAR(250) [NOT NULL]
- order_channel : VARCHAR(100) [NOT NULL]
- customer_support_method : VARCHAR(100) [NOT NULL]
- issue_status : VARCHAR(100) [NOT NULL]
- app_usage : VARCHAR(100) [NOT NULL]
- website_visits : INTEGER
- social_media_engagement : VARCHAR(100) [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### nf.nf_employees_scd
- employee_id : BIGINT [PK PART]
- employee_src_id : VARCHAR(255) [NOT NULL]
- employee_name_nk : VARCHAR(100)
- employee_position : VARCHAR(100)
- employee_salary : NUMERIC(10,2)
- employee_hire_date : DATE
- start_dt : TIMESTAMP [NOT NULL, PK PART]
- end_dt : TIMESTAMP [NOT NULL]
- is_active : BOOLEAN [NOT NULL]
- source_system : VARCHAR(100)
- source_table : VARCHAR(100)
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### nf.nf_transactions
- transaction_id : VARCHAR(100) [NOT NULL]
- transaction_dt : TIMESTAMP [NOT NULL]
- total_sales : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- payment_method : VARCHAR(100) [NOT NULL, DEFAULT 'n.a.']
- quantity : INTEGER [NOT NULL, DEFAULT 0]
- unit_price : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- discount_applied : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- day_of_week : VARCHAR(20) [NOT NULL, DEFAULT 'n.a.']
- week_of_year : INTEGER [NOT NULL, DEFAULT -1]
- month_of_year : INTEGER [NOT NULL, DEFAULT -1]
- store_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_stores.store_id]
- customer_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_customers.customer_id]
- promotion_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_promotions.promotion_id]
- delivery_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_deliveries.delivery_id]
- product_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_products.product_id]
- engagement_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_engagements.engagement_id]
- city_id : BIGINT [NOT NULL, DEFAULT -1, FK -> nf.nf_cities.city_id]
- employee_id : BIGINT [NOT NULL, DEFAULT -1]
- row_sig : TEXT [NOT NULL, UNIQUE INDEX ux_nf_transactions_row_sig]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

## DIM

### dim.dim_customers
- customer_surr_id : BIGINT [PK]
- customer_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- gender : VARCHAR(20) [NOT NULL]
- marital_status : VARCHAR(20) [NOT NULL]
- birth_of_dt : DATE
- membership_dt : DATE
- last_purchase_dt : TIMESTAMP
- customer_zip_code : VARCHAR(30) [NOT NULL]
- customer_city : VARCHAR(100) [NOT NULL]
- customer_state : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_stores
- store_surr_id : BIGINT [PK]
- store_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- store_name : VARCHAR(100)
- store_zip_code : VARCHAR(30) [NOT NULL]
- store_city : VARCHAR(100) [NOT NULL]
- store_state : VARCHAR(100) [NOT NULL]
- store_location : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_products
- product_surr_id : BIGINT [PK]
- product_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- product_category : VARCHAR(100) [NOT NULL]
- product_name : VARCHAR(100) [NOT NULL]
- product_brand : VARCHAR(100) [NOT NULL]
- product_stock : INTEGER
- product_material : VARCHAR(100) [NOT NULL]
- product_manufacture_dt : TIMESTAMP
- product_expiry_dt : TIMESTAMP
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_promotions
- promotion_surr_id : BIGINT [PK]
- promotion_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- promotion_channel : VARCHAR(100) [NOT NULL]
- promotion_type : VARCHAR(100) [NOT NULL]
- promotion_start_dt : TIMESTAMP
- promotion_end_dt : TIMESTAMP
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_deliveries
- delivery_surr_id : BIGINT [PK]
- delivery_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- delivery_type : VARCHAR(100) [NOT NULL]
- delivery_status : VARCHAR(100) [NOT NULL]
- shipping_partner : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_engagements
- engagement_surr_id : BIGINT [PK]
- engagement_src_id : BIGINT [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- customer_support_calls : INTEGER
- website_address : VARCHAR(250) [NOT NULL]
- order_channel : VARCHAR(100) [NOT NULL]
- customer_support_method : VARCHAR(100) [NOT NULL]
- issue_status : VARCHAR(100) [NOT NULL]
- app_usage : VARCHAR(100) [NOT NULL]
- website_visits : INTEGER
- social_media_engagement : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.dim_employees_scd
- employee_surr_id : BIGINT [PK]
- employee_src_id : VARCHAR(255) [NOT NULL]
- employee_name : VARCHAR(100)
- employee_position : VARCHAR(100)
- employee_salary : NUMERIC(10,2)
- employee_hire_date : DATE
- start_dt : TIMESTAMP [NOT NULL]
- end_dt : TIMESTAMP [NOT NULL]
- is_active : BOOLEAN [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [DEFAULT CURRENT_TIMESTAMP]

### dim.dim_dates
- date_surr_id : BIGINT [PK]
- full_date : DATE [NOT NULL, UNIQUE]
- day_of_month : INTEGER [NOT NULL]
- month_of_year : INTEGER [NOT NULL]
- year_of_date : INTEGER [NOT NULL]
- quarter_of_year : INTEGER [NOT NULL]
- week_of_year : INTEGER [NOT NULL]
- day_name : VARCHAR(20) [NOT NULL]
- month_name : VARCHAR(20) [NOT NULL]
- is_weekend : BOOLEAN [NOT NULL]
- is_month_start : BOOLEAN [NOT NULL]
- is_month_end : BOOLEAN [NOT NULL]
- is_quarter_start : BOOLEAN [NOT NULL]
- is_quarter_end : BOOLEAN [NOT NULL]
- is_year_start : BOOLEAN [NOT NULL]
- is_year_end : BOOLEAN [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]

### dim.fct_transactions_dd_dd
- transaction_src_id : VARCHAR(100) [NOT NULL]
- total_sales : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- quantity : INTEGER [NOT NULL, DEFAULT 0]
- unit_price : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- discount_applied : NUMERIC(10,2) [NOT NULL, DEFAULT 0]
- payment_method : VARCHAR(100) [NOT NULL, DEFAULT 'n.a.']
- product_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_products.product_surr_id]
- promotion_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_promotions.promotion_surr_id]
- delivery_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_deliveries.delivery_surr_id]
- engagement_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_engagements.engagement_surr_id]
- store_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_stores.store_surr_id]
- customer_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_customers.customer_surr_id]
- employee_surr_id : BIGINT [NOT NULL, DEFAULT -1, FK -> dim.dim_employees_scd.employee_surr_id]
- transaction_date_sk : BIGINT [NOT NULL, FK -> dim.dim_dates.date_surr_id]
- transaction_date : DATE [NOT NULL]
- source_system : VARCHAR(100) [NOT NULL]
- source_table : VARCHAR(100) [NOT NULL]
- insert_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- update_dt : TIMESTAMP [NOT NULL, DEFAULT CURRENT_TIMESTAMP]
- PK : (transaction_date, transaction_src_id, product_surr_id, promotion_surr_id, delivery_surr_id, engagement_surr_id, store_surr_id, customer_surr_id, employee_surr_id)
