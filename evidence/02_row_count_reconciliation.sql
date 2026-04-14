/* Row-count reconciliation evidence */

-- 1) Source total vs mapping transactions
SELECT
  (SELECT COUNT(*) FROM (
      SELECT transaction_id FROM sl_online_retail.src_online_retail
      UNION ALL
      SELECT transaction_id FROM sl_offline_retail.src_offline_retail
  ) s) AS source_rows,
  (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows;

-- 2) Mapping vs normalized
SELECT
  (SELECT COUNT(*) FROM stg.mapping_transactions) AS mapping_rows,
  (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows;

-- 3) Normalized vs fact
SELECT
  (SELECT COUNT(*) FROM nf.nf_transactions) AS nf_rows,
  (SELECT COUNT(*) FROM dim.fct_transactions_dd_dd) AS fact_rows;
