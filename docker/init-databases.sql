-- Runs once on first container start (mounted into /docker-entrypoint-initdb.d/).
-- POSTGRES_DB already creates one default database; these are the other three,
-- one per service, so no service ever shares a physical table with another.
CREATE DATABASE api_db;
CREATE DATABASE workflow_db;
CREATE DATABASE worker_db;
CREATE DATABASE notification_db;
