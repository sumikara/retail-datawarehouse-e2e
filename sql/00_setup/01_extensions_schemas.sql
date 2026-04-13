-- Extensions
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Foreign server (csv_server)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'csv_server') THEN
    CREATE SERVER csv_server FOREIGN DATA WRAPPER file_fdw;
  END IF;
END $$;

-- Schemas
CREATE SCHEMA IF NOT EXISTS sl_online_retail;
CREATE SCHEMA IF NOT EXISTS sl_offline_retail;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS nf; -- normalized, Immon
CREATE SCHEMA IF NOT EXISTS dim; -- denormalized, Kimball
