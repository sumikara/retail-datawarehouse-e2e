/* Performance evidence template (capture plans to file) */

EXPLAIN (ANALYZE, BUFFERS)
SELECT
  t.transaction_id,
  t.transaction_dt,
  t.total_sales,
  c.customer_src_id,
  p.product_src_id,
  s.store_src_id
FROM nf.nf_transactions t
JOIN nf.nf_customers c ON c.customer_id = t.customer_id
JOIN nf.nf_products p ON p.product_id = t.product_id
JOIN nf.nf_stores s ON s.store_id = t.store_id
WHERE t.transaction_dt >= CURRENT_DATE - INTERVAL '30 days';

EXPLAIN (ANALYZE, BUFFERS)
SELECT
  f.transaction_date,
  SUM(f.total_sales) AS sales
FROM dim.fct_transactions_dd_dd f
GROUP BY f.transaction_date
ORDER BY f.transaction_date DESC
LIMIT 31;
