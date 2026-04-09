ALTER FOREIGN TABLE sl_offline_retail.frg_offline_retail
  OPTIONS (SET filename '/content/data/03_empty_5_off.csv');

ALTER FOREIGN TABLE sl_online_retail.frg_online_retail
  OPTIONS (SET filename '/content/data/04_empty_5_on.csv');

INSERT INTO sl_offline_retail.src_offline_retail_raw (
  customer_id, gender, marital_status, transaction_id, transaction_date,
  product_id, product_category, quantity, unit_price, discount_applied,
  day_of_week, week_of_year, month_of_year, product_name, product_brand,
  product_stock, product_material, promotion_id, promotion_type,
  promotion_start_date, promotion_end_date, customer_zip_code,
  customer_city, customer_state, store_zip_code, store_city, store_state,
  date_of_birth, payment_method, delivery_id, delivery_type,
  delivery_status, shipping_partner, employee_salary, membership_date,
  store_location, last_purchase_date, total_sales, product_manufacture_date,
  product_expiry_date, promotion_channel, employee_name, employee_position,
  employee_hire_date,
  batch_id, load_type, source_file_name, load_dts, source_row_num
)
SELECT
  customer_id, gender, marital_status, transaction_id, transaction_date,
  product_id, product_category, quantity, unit_price, discount_applied,
  day_of_week, week_of_year, month_of_year, product_name, product_brand,
  product_stock, product_material, promotion_id, promotion_type,
  promotion_start_date, promotion_end_date, customer_zip_code,
  customer_city, customer_state, store_zip_code, store_city, store_state,
  date_of_birth, payment_method, delivery_id, delivery_type,
  delivery_status, shipping_partner, employee_salary, membership_date,
  store_location, last_purchase_date, total_sales, product_manufacture_date,
  product_expiry_date, promotion_channel, employee_name, employee_position,
  employee_hire_date,
  2 AS batch_id,
  'INCREMENTAL' AS load_type,
  '03_empty_5_off.csv' AS source_file_name,
  CURRENT_TIMESTAMP AS load_dts,
  ROW_NUMBER() OVER () AS source_row_num
FROM sl_offline_retail.frg_offline_retail;

INSERT INTO sl_online_retail.src_online_retail_raw (
  customer_id, gender, marital_status, transaction_id, transaction_date,
  product_id, product_category, quantity, unit_price, discount_applied,
  day_of_week, week_of_year, month_of_year, product_name, product_brand,
  product_stock, product_material, promotion_id, promotion_type,
  promotion_start_date, promotion_end_date, customer_zip_code,
  customer_city, customer_state, customer_support_calls, date_of_birth,
  payment_method, delivery_id, delivery_type, delivery_status,
  shipping_partner, membership_date, website_address, order_channel,
  customer_support_method, issue_status, product_manufacture_date,
  product_expiry_date, total_sales, promotion_channel, last_purchase_date,
  app_usage, website_visits, social_media_engagement, engagement_id,
  batch_id, load_type, source_file_name, load_dts, source_row_num
)
SELECT
  customer_id, gender, marital_status, transaction_id, transaction_date,
  product_id, product_category, quantity, unit_price, discount_applied,
  day_of_week, week_of_year, month_of_year, product_name, product_brand,
  product_stock, product_material, promotion_id, promotion_type,
  promotion_start_date, promotion_end_date, customer_zip_code,
  customer_city, customer_state, customer_support_calls, date_of_birth,
  payment_method, delivery_id, delivery_type, delivery_status,
  shipping_partner, membership_date, website_address, order_channel,
  customer_support_method, issue_status, product_manufacture_date,
  product_expiry_date, total_sales, promotion_channel, last_purchase_date,
  app_usage, website_visits, social_media_engagement, engagement_id,
  2 AS batch_id,
  'INCREMENTAL' AS load_type,
  '04_empty_5_on.csv' AS source_file_name,
  CURRENT_TIMESTAMP AS load_dts,
  ROW_NUMBER() OVER () AS source_row_num
FROM sl_online_retail.frg_online_retail;

