CALL bl_cl.master_full_load( 
    p_source_system => 'incremental_5_demo', 
    p_file_id       => NULL,
    p_trigger_type  => 'MANUAL'
);
 -- or
SELECT
    source_file_name,
    DATE_TRUNC('month',
        CASE
            WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
            THEN TO_TIMESTAMP(transaction_date, 'DD-MM-YYYY HH24:MI')
            WHEN transaction_date ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
            THEN TO_TIMESTAMP(transaction_date, 'DD/MM/YYYY HH24:MI')
        END
    )::DATE AS month_start,
    COUNT(*) AS row_count
FROM sa_offline_retail.src_offline_retail_raw
WHERE source_file_name = '03_empty_5_off.csv'
GROUP BY source_file_name, month_start

UNION ALL

SELECT
    source_file_name,
    DATE_TRUNC('month',
        CASE
            WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
            THEN TO_TIMESTAMP(transaction_date, 'DD-MM-YYYY HH24:MI')
            WHEN transaction_date ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
            THEN TO_TIMESTAMP(transaction_date, 'DD/MM/YYYY HH24:MI')
        END
    )::DATE AS month_start,
    COUNT(*) AS row_count
FROM sa_online_retail.src_online_retail_raw
WHERE source_file_name = '04_empty_5_on.csv'
GROUP BY source_file_name, month_start
ORDER BY month_start;

--result set: 
        -- year:x, month: y 
        -- year:a, month: b
--  attention!!! x, y, a, b are numeric values only, not other than that.
CALL bl_dm.load_fct_transactions_by_month(x, y);
CALL bl_dm.load_fct_transactions_by_month(a, b);

