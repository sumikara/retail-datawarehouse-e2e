/* Measure reconciliation evidence */

-- total_sales parity
SELECT
  COALESCE((SELECT SUM(total_sales) FROM nf.nf_transactions), 0) AS nf_total_sales,
  COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd), 0) AS fact_total_sales,
  COALESCE((SELECT SUM(total_sales) FROM nf.nf_transactions), 0)
  - COALESCE((SELECT SUM(total_sales) FROM dim.fct_transactions_dd_dd), 0) AS delta_total_sales;

-- quantity parity
SELECT
  COALESCE((SELECT SUM(quantity) FROM nf.nf_transactions), 0) AS nf_quantity,
  COALESCE((SELECT SUM(quantity) FROM dim.fct_transactions_dd_dd), 0) AS fact_quantity,
  COALESCE((SELECT SUM(quantity) FROM nf.nf_transactions), 0)
  - COALESCE((SELECT SUM(quantity) FROM dim.fct_transactions_dd_dd), 0) AS delta_quantity;
