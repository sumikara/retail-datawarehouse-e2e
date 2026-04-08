
   -- 1) FOREIGN TABLE FILE POINTERS -> 95% 

ALTER FOREIGN TABLE sl_offline_retail.frg_offline_retail
  OPTIONS (SET filename '/content/data/01_empty_95_off.csv');

ALTER FOREIGN TABLE sl_online_retail.frg_online_retail
  OPTIONS (SET filename '/content/data/02_empty_95_on.csv');

 -- 2) RAW TABLES CLEAN START FOR BULK DEMO
DROP FOREIGN TABLE IF EXISTS sl_offline_retail.src_offline_retail_raw;
CREATE TABLE sl_offline_retail.src_offline_retail_raw (
  customer_id              VARCHAR(1000),
  gender                   VARCHAR(1000),
  marital_status           VARCHAR(1000),
  transaction_id           VARCHAR(1000),
  transaction_date         VARCHAR(1000),
  product_id               VARCHAR(1000),
  product_category         VARCHAR(1000),
  quantity                 VARCHAR(1000),
  unit_price               VARCHAR(1000),
  discount_applied         VARCHAR(1000),
  day_of_week              VARCHAR(1000),
  week_of_year             VARCHAR(1000),
  month_of_year            VARCHAR(1000),
  product_name             VARCHAR(1000),
  product_brand            VARCHAR(1000),
  product_stock            VARCHAR(1000),
  product_material         VARCHAR(1000),
  promotion_id             VARCHAR(1000),
  promotion_type           VARCHAR(1000),
  promotion_start_date     VARCHAR(1000),
  promotion_end_date       VARCHAR(1000),
  customer_zip_code        VARCHAR(1000),
  customer_city            VARCHAR(1000),
  customer_state           VARCHAR(1000),
  store_zip_code           VARCHAR(1000),
  store_city               VARCHAR(1000),
  store_state              VARCHAR(1000),
  date_of_birth            VARCHAR(1000),
  payment_method           VARCHAR(1000),
  delivery_id              VARCHAR(1000),
  delivery_type            VARCHAR(1000),
  delivery_status          VARCHAR(1000),
  shipping_partner         VARCHAR(1000),
  employee_salary          VARCHAR(1000),
  membership_date          VARCHAR(1000),
  store_location           VARCHAR(1000),
  last_purchase_date       VARCHAR(1000),
  total_sales              VARCHAR(1000),
  product_manufacture_date VARCHAR(1000),
  product_expiry_date      VARCHAR(1000),
  promotion_channel        VARCHAR(1000),
  employee_name            VARCHAR(1000),
  employee_position        VARCHAR(1000),
  employee_hire_date       VARCHAR(1000),
  insert_dt                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  batch_id                 BIGINT,
  load_type                VARCHAR(20),
  source_file_name         VARCHAR(255),
  load_dts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_row_num           BIGINT
  );

DROP FOREIGN TABLE IF EXISTS sl_online_retail.src_online_retail_raw;
CREATE TABLE sl_online_retail.src_online_retail_raw (
  customer_id              VARCHAR(1000),
  gender                   VARCHAR(1000),
  marital_status           VARCHAR(1000),
  transaction_id           VARCHAR(1000),
  transaction_date         VARCHAR(1000),
  product_id               VARCHAR(1000),
  product_category         VARCHAR(1000),
  quantity                 VARCHAR(1000),
  unit_price               VARCHAR(1000),
  discount_applied         VARCHAR(1000),
  day_of_week              VARCHAR(1000),
  week_of_year             VARCHAR(1000),
  month_of_year            VARCHAR(1000),
  product_name             VARCHAR(1000),
  product_brand            VARCHAR(1000),
  product_stock            VARCHAR(1000),
  product_material         VARCHAR(1000),
  promotion_id             VARCHAR(1000),
  promotion_type           VARCHAR(1000),
  promotion_start_date     VARCHAR(1000),
  promotion_end_date       VARCHAR(1000),
  customer_zip_code        VARCHAR(1000),
  customer_city            VARCHAR(1000),
  customer_state           VARCHAR(1000),
  customer_support_calls   VARCHAR(1000),
  date_of_birth            VARCHAR(1000),
  payment_method           VARCHAR(1000),
  delivery_id              VARCHAR(1000),
  delivery_type            VARCHAR(1000),
  delivery_status          VARCHAR(1000),
  shipping_partner         VARCHAR(1000),
  membership_date          VARCHAR(1000),
  website_address          VARCHAR(1000),
  order_channel            VARCHAR(1000),
  customer_support_method  VARCHAR(1000),
  issue_status             VARCHAR(1000),
  product_manufacture_date VARCHAR(1000),
  product_expiry_date      VARCHAR(1000),
  total_sales              VARCHAR(1000),
  promotion_channel        VARCHAR(1000),
  last_purchase_date       VARCHAR(1000),
  app_usage                VARCHAR(1000),
  website_visits           VARCHAR(1000),
  social_media_engagement  VARCHAR(1000),
  engagement_id            VARCHAR(1000),
  insert_dt                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  batch_id                 BIGINT,
  load_type                VARCHAR(20),
  source_file_name         VARCHAR(255),
  load_dts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_row_num           BIGINT
  );

TRUNCATE TABLE sl_offline_retail.src_offline_retail_raw;
INSERT INTO sa_offline_retail.src_offline_retail_raw (
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
  1 AS batch_id,
  'BULK' AS load_type,
  '01_empty_95_off.csv' AS source_file_name,
  CURRENT_TIMESTAMP AS load_dts,
  ROW_NUMBER() OVER () AS source_row_num
FROM sa_offline_retail.ext_offline_retail;

TRUNCATE TABLE sl_online_retail.src_online_retail_raw;
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
      1 AS batch_id,
  'BULK' AS load_type,
  '02_empty_95_on.csv' AS source_file_name,
  CURRENT_TIMESTAMP AS load_dts,
  ROW_NUMBER() OVER () AS source_row_num
FROM sa_online_retail.ext_online_retail;

/* quick check */
SELECT 'offline_raw_bulk_95' AS check_name, COUNT(*) FROM sl_offline_retail.src_offline_retail_raw
UNION ALL
SELECT 'online_raw_bulk_95', COUNT(*) FROM sl_online_retail.src_online_retail_raw;

