-- =============================================================================
-- 00_create_database.sql — create the nse_data database.
-- Run while connected to the default "postgres" database, e.g.
--   psql -h <host> -U <user> -d postgres -f 00_create_database.sql
-- (python -m db.pipeline init-db does this step automatically.)
-- CREATE DATABASE cannot run inside a transaction block.
-- =============================================================================

SELECT 'CREATE DATABASE nse_data ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'nse_data')
\gexec
