-- PostgreSQL least-privilege role model template for quant backend.
--
-- Customize values before applying in production:
--   - login roles: quant_app_login / quant_report_login
--   - passwords: replace CHANGE_ME_* via secret manager
--   - database/schema names when not using quant/public
--
-- Apply with:
--   psql "$API_DB_POSTGRES_DSN" -f ops/postgres/postgres_permission_model_template.sql

BEGIN;

-- 1) Group roles (NOLOGIN): privilege boundaries
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'quant_app_rw') THEN
        CREATE ROLE quant_app_rw NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'quant_report_ro') THEN
        CREATE ROLE quant_report_ro NOLOGIN;
    END IF;
END;
$$;

-- 2) Login roles (LOGIN): bind credentials to group roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'quant_app_login') THEN
        CREATE ROLE quant_app_login
            LOGIN
            PASSWORD 'CHANGE_ME_APP_PASSWORD'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
            INHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'quant_report_login') THEN
        CREATE ROLE quant_report_login
            LOGIN
            PASSWORD 'CHANGE_ME_REPORT_PASSWORD'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
            INHERIT;
    END IF;
END;
$$;

GRANT quant_app_rw TO quant_app_login;
GRANT quant_report_ro TO quant_report_login;

-- 3) Database/schema access
-- Replace database name if not using "quant".
GRANT CONNECT ON DATABASE quant TO quant_app_rw, quant_report_ro;
GRANT USAGE ON SCHEMA public TO quant_app_rw, quant_report_ro;

-- 4) App write role: DML-only (no DDL ownership)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO quant_app_rw;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO quant_app_rw;

-- 5) Report read role: read-only
GRANT SELECT ON ALL TABLES IN SCHEMA public TO quant_report_ro;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO quant_report_ro;

-- 6) Default privileges for future objects (run as object owner or superuser)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO quant_app_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO quant_app_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO quant_report_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO quant_report_ro;

COMMIT;

-- 7) Credential rotation template (example)
-- Step A: create next login role and join same group
--   CREATE ROLE quant_app_login_next LOGIN PASSWORD 'CHANGE_ME_NEXT' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;
--   GRANT quant_app_rw TO quant_app_login_next;
-- Step B: switch application secret / connection string to quant_app_login_next
-- Step C: verify writes and report queries
-- Step D: revoke old role and drop
--   REVOKE quant_app_rw FROM quant_app_login;
--   DROP ROLE quant_app_login;
--
-- Alternative fast rotation (single role):
--   ALTER ROLE quant_app_login WITH PASSWORD 'CHANGE_ME_ROTATED';
