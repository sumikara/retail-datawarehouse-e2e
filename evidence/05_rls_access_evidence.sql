/* RLS evidence script (run with appropriate roles) */

-- Employee scope example
-- SET ROLE role_employee_app;
-- SET app.current_employee_src_id = 'EMP_001';
SELECT employee_src_id, employee_position, is_active
FROM nf.nf_employees_scd
ORDER BY employee_src_id
LIMIT 20;

-- Customer scope example
-- SET ROLE role_customer_app;
-- SET app.current_customer_id_nk = 'CUST_001';
SELECT customer_id_nk, address_id
FROM nf.nf_customers
ORDER BY customer_id_nk
LIMIT 20;

SELECT transaction_id, total_sales, transaction_dt
FROM nf.nf_transactions
ORDER BY transaction_dt DESC
LIMIT 20;
