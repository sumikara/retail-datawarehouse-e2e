--
DROP FOREIGN TABLE IF EXISTS sa_offline_retail.ext_offline_retail_employee_inc;
DROP TABLE IF EXISTS sa_offline_retail.src_offline_retail_employee_inc;

/* foreign table over the 1-row incremental employee file */
CREATE FOREIGN TABLE sa_offline_retail.ext_offline_retail_employee_inc (
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
  employee_hire_date       VARCHAR(1000)
)
SERVER csv_server
OPTIONS (
  filename  '/content/data/src_offline_retail_employee_inc.csv',
  format    'csv',
  header    'true',
  delimiter ',',
  quote     '"',
  escape    '"'
);

/* typed table compatible with load_map_employees() */
CREATE TABLE sa_offline_retail.src_offline_retail_employee_inc AS
SELECT
    COALESCE(NULLIF(LOWER(TRIM(customer_id)), ''), 'n.a.') AS customer_id,
    COALESCE(NULLIF(LOWER(TRIM(gender)), ''), 'n.a.') AS gender,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(marital_status),' ','_')), ''), 'n.a.') AS marital_status,

    CASE
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
         AND TRIM(transaction_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
         AND TRIM(transaction_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD/MM/YYYY HH24:MI')
    END AS transaction_dt,

    COALESCE(NULLIF(LOWER(REPLACE(TRIM(employee_name),' ','_')), ''), 'n.a.') AS employee_name,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(employee_position),' ','_')), ''), 'n.a.') AS employee_position,

    CASE
      WHEN TRIM(employee_salary) ~ '^-?\d+(\.\d+)?$'
      THEN employee_salary::NUMERIC(10,2)
    END AS employee_salary,

    CASE
        WHEN NULLIF(TRIM(employee_hire_date), '') IS NOT NULL
         AND TRIM(employee_hire_date) ~ '^\d{2}-\d{2}-\d{4}$'
        THEN TO_DATE(TRIM(employee_hire_date), 'DD-MM-YYYY')
        WHEN NULLIF(TRIM(employee_hire_date), '') IS NOT NULL
         AND TRIM(employee_hire_date) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(TRIM(employee_hire_date), 'DD/MM/YYYY')
    END AS employee_hire_date

FROM sa_offline_retail.ext_offline_retail_employee_inc;

/* check */
SELECT * FROM sa_offline_retail.src_offline_retail_employee_inc;

