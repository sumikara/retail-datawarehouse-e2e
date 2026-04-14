/* =============================================================================
   RETAIL DWH SECURITY BASELINE
   - Admin owner: sumiadmin
   - Role-based schema boundaries
   - Default privileges for future objects
   - Row-Level Security for employee/customer self-service
   ============================================================================= */

/* -----------------------------------------------------------------------------
   1) PLATFORM ADMIN (warehouse owner / lead engineer)
   ----------------------------------------------------------------------------- */
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sumiadmin') THEN
        CREATE ROLE sumiadmin LOGIN;
    END IF;
END $$;

ALTER ROLE sumiadmin CREATEROLE CREATEDB BYPASSRLS;

GRANT USAGE, CREATE ON SCHEMA sl_online_retail, sl_offline_retail, stg, nf, dim TO sumiadmin;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA sl_online_retail, sl_offline_retail, stg, nf, dim TO sumiadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA stg, nf, dim TO sumiadmin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA stg, nf, dim TO sumiadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA sl_online_retail, sl_offline_retail, stg, nf, dim
GRANT ALL PRIVILEGES ON TABLES TO sumiadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg, nf, dim
GRANT ALL PRIVILEGES ON SEQUENCES TO sumiadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg, nf, dim
GRANT ALL PRIVILEGES ON FUNCTIONS TO sumiadmin;

/* -----------------------------------------------------------------------------
   2) WORKLOAD / CONSUMER ROLES
   ----------------------------------------------------------------------------- */
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_etl_runner') THEN
        CREATE ROLE role_etl_runner NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_bi_reader') THEN
        CREATE ROLE role_bi_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_dq_analyst') THEN
        CREATE ROLE role_dq_analyst NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_auditor') THEN
        CREATE ROLE role_auditor NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_employee_app') THEN
        CREATE ROLE role_employee_app NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_customer_app') THEN
        CREATE ROLE role_customer_app NOLOGIN;
    END IF;
END $$;

/* ETL boundary */
GRANT USAGE ON SCHEMA stg, nf, dim TO role_etl_runner;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg, nf, dim TO role_etl_runner;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA stg, nf, dim TO role_etl_runner;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA stg, nf, dim TO role_etl_runner;

/* BI boundary: read-only marts */
GRANT USAGE ON SCHEMA dim TO role_bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dim TO role_bi_reader;

/* Data quality boundary */
GRANT USAGE ON SCHEMA stg, nf, dim TO role_dq_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA stg, nf, dim TO role_dq_analyst;
GRANT SELECT ON stg.etl_log, stg.etl_batch_run, stg.etl_step_run, stg.etl_file_registry TO role_dq_analyst;

/* Audit boundary */
GRANT USAGE ON SCHEMA stg, nf, dim TO role_auditor;
GRANT SELECT ON stg.etl_log, stg.etl_batch_run, stg.etl_step_run, stg.etl_file_registry TO role_auditor;

/* Customer application boundary */
GRANT USAGE ON SCHEMA nf TO role_customer_app;
GRANT SELECT ON nf.nf_customers, nf.nf_transactions, nf.nf_addresses, nf.nf_cities, nf.nf_states TO role_customer_app;

/* Employee application boundary */
GRANT USAGE ON SCHEMA nf, dim TO role_employee_app;
GRANT SELECT ON nf.nf_employees_scd, dim.dim_employees_scd TO role_employee_app;

/* Default privileges for future objects */
ALTER DEFAULT PRIVILEGES IN SCHEMA stg, nf, dim
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_etl_runner;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg, nf, dim
GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO role_etl_runner;

ALTER DEFAULT PRIVILEGES IN SCHEMA dim
GRANT SELECT ON TABLES TO role_bi_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg, nf, dim
GRANT SELECT ON TABLES TO role_dq_analyst;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg
GRANT SELECT ON TABLES TO role_auditor;

/* -----------------------------------------------------------------------------
   3) ROW-LEVEL SECURITY HELPERS
   App should set:
     SET app.current_employee_src_id = 'EMP_001';
     SET app.current_customer_id_nk  = 'CUST_123';
     SET app.allowed_employee_positions = 'manager,finance';
   ----------------------------------------------------------------------------- */
CREATE SCHEMA IF NOT EXISTS sec;

CREATE OR REPLACE FUNCTION sec.current_employee_src_id()
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
    SELECT NULLIF(current_setting('app.current_employee_src_id', true), '');
$$;

CREATE OR REPLACE FUNCTION sec.current_customer_id_nk()
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
    SELECT NULLIF(current_setting('app.current_customer_id_nk', true), '');
$$;

CREATE OR REPLACE FUNCTION sec.allowed_employee_positions()
RETURNS TEXT[]
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(string_to_array(NULLIF(current_setting('app.allowed_employee_positions', true), ''), ','), ARRAY[]::TEXT[]);
$$;

/* -----------------------------------------------------------------------------
   4) EMPLOYEE SELF / DEPARTMENT-LIKE RLS
   ----------------------------------------------------------------------------- */
ALTER TABLE nf.nf_employees_scd ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim.dim_employees_scd ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_nf_employee_self ON nf.nf_employees_scd;
CREATE POLICY p_nf_employee_self
ON nf.nf_employees_scd
FOR SELECT
TO role_employee_app
USING (
    employee_src_id = sec.current_employee_src_id()
    OR (
        cardinality(sec.allowed_employee_positions()) > 0
        AND employee_position = ANY (sec.allowed_employee_positions())
    )
);

DROP POLICY IF EXISTS p_dim_employee_self ON dim.dim_employees_scd;
CREATE POLICY p_dim_employee_self
ON dim.dim_employees_scd
FOR SELECT
TO role_employee_app
USING (
    employee_src_id = sec.current_employee_src_id()
    OR (
        cardinality(sec.allowed_employee_positions()) > 0
        AND employee_position = ANY (sec.allowed_employee_positions())
    )
);

/* -----------------------------------------------------------------------------
   5) CUSTOMER SELF RLS (own profile + own transactions + own address lineage)
   ----------------------------------------------------------------------------- */
ALTER TABLE nf.nf_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE nf.nf_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE nf.nf_addresses ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_nf_customer_self ON nf.nf_customers;
CREATE POLICY p_nf_customer_self
ON nf.nf_customers
FOR SELECT
TO role_customer_app
USING (
    customer_id_nk = sec.current_customer_id_nk()
);

DROP POLICY IF EXISTS p_nf_customer_transactions ON nf.nf_transactions;
CREATE POLICY p_nf_customer_transactions
ON nf.nf_transactions
FOR SELECT
TO role_customer_app
USING (
    EXISTS (
        SELECT 1
        FROM nf.nf_customers c
        WHERE c.customer_id = nf.nf_transactions.customer_id
          AND c.customer_id_nk = sec.current_customer_id_nk()
    )
);

DROP POLICY IF EXISTS p_nf_customer_addresses ON nf.nf_addresses;
CREATE POLICY p_nf_customer_addresses
ON nf.nf_addresses
FOR SELECT
TO role_customer_app
USING (
    EXISTS (
        SELECT 1
        FROM nf.nf_customers c
        WHERE c.address_id = nf.nf_addresses.address_id
          AND c.customer_id_nk = sec.current_customer_id_nk()
    )
);

/* Reference tables for authorized customer rows */
ALTER TABLE nf.nf_cities ENABLE ROW LEVEL SECURITY;
ALTER TABLE nf.nf_states ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_nf_customer_cities ON nf.nf_cities;
CREATE POLICY p_nf_customer_cities
ON nf.nf_cities
FOR SELECT
TO role_customer_app
USING (
    EXISTS (
        SELECT 1
        FROM nf.nf_addresses a
        JOIN nf.nf_customers c ON c.address_id = a.address_id
        WHERE a.city_id = nf.nf_cities.city_id
          AND c.customer_id_nk = sec.current_customer_id_nk()
    )
);

DROP POLICY IF EXISTS p_nf_customer_states ON nf.nf_states;
CREATE POLICY p_nf_customer_states
ON nf.nf_states
FOR SELECT
TO role_customer_app
USING (
    EXISTS (
        SELECT 1
        FROM nf.nf_cities ci
        JOIN nf.nf_addresses a ON a.city_id = ci.city_id
        JOIN nf.nf_customers c ON c.address_id = a.address_id
        WHERE ci.state_id = nf.nf_states.state_id
          AND c.customer_id_nk = sec.current_customer_id_nk()
    )
);
