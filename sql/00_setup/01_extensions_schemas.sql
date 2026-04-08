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
CREATE SCHEMA IF NOT EXISTS sl_online_retail; -- sl:source layer
CREATE SCHEMA IF NOT EXISTS sl_offline_retail;
CREATE SCHEMA IF NOT EXISTS stg; -- staging
CREATE SCHEMA IF NOT EXISTS 3nf; -- normalized, Immon
CREATE SCHEMA IF NOT EXISTS dim; -- denormalized, Kimball
