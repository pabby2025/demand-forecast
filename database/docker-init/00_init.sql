-- =============================================================================
-- Docker container init script
-- Runs automatically when the postgres container first starts (empty volume).
-- Executes in order: 00_ before 01_ before 02_ etc.
-- Database: demand_forecast_db  (created by POSTGRES_DB env var)
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Try pgvector — only available if the pgvector image is used.
-- Using ankane/pgvector image in docker-compose enables this.
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pgvector extension not available — skipping (install ankane/pgvector image for vector search)';
END $$;

-- Schema
CREATE SCHEMA IF NOT EXISTS dfc;
