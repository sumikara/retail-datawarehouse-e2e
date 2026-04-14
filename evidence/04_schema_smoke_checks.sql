/* Schema/object smoke checks evidence */

-- Schema presence
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('sl_online_retail','sl_offline_retail','stg','nf','dim')
ORDER BY schema_name;

-- Core tables presence
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
  ('stg','etl_batch_run'),
  ('stg','etl_step_run'),
  ('stg','etl_file_registry'),
  ('stg','etl_log'),
  ('nf','nf_transactions'),
  ('dim','fct_transactions_dd_dd')
)
ORDER BY table_schema, table_name;

-- Key columns and data types (nf transactions)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='nf' AND table_name='nf_transactions'
  AND column_name IN ('transaction_id','transaction_dt','total_sales','quantity','unit_price','discount_applied','row_sig')
ORDER BY column_name;
