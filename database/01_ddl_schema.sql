-- =============================================================================
-- DEMAND FORECAST PLANNER — PostgreSQL Schema (DDL)
-- Database: demand_forecast_db
-- Schema:   dfc  (Demand Fulfilment Cycle)
--
-- Architecture:
--   STG layer  → raw ingest from EDS/Data Mart (Cognizant upstream systems)
--   DIM layer  → conformed dimension tables (slowly changing)
--   FACT layer → immutable demand & prediction facts
--   APP layer  → application entities (scenarios, feedback, tasks, alerts)
--   ML  layer  → ML pipeline artefacts (model metadata, predictions, guardrail)
--
-- Upstream source systems feeding the Data Mart:
--   1. QuickSO / SO System   → Service Order records (SO_LINE_STATUS, UNIQUE_ID, etc.)
--   2. Resource/BU System    → BU hierarchy, SBU-BU mapping
--   3. Skills Database       → TECHNICAL_SKILLS_REQUIRED per SO
--   4. Project/Billing System → PROJECT_TYPE, PROJECT_BILLABILITY_TYPE
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Database & extensions
-- ---------------------------------------------------------------------------
-- CREATE DATABASE demand_forecast_db WITH ENCODING = 'UTF8';
-- \c demand_forecast_db

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;          -- pgvector for skill embeddings
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- trigram index for skill name search

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dfc;
SET search_path = dfc, public;

-- ---------------------------------------------------------------------------
-- ENUMERATIONS
-- ---------------------------------------------------------------------------

CREATE TYPE dfc.market_enum AS ENUM ('Americas', 'EMEA');
CREATE TYPE dfc.xyz_segment_enum AS ENUM ('X', 'Y', 'Z');
-- X: CV < 0.5 (Stable/high-demand)
-- Y: 0.5 ≤ CV ≤ 1.0 (Variable)
-- Z: CV > 1.0 (Sporadic/lumpy)

CREATE TYPE dfc.prediction_source_enum AS ENUM ('AutoML', 'Override', 'Guardrail_Corrected');
CREATE TYPE dfc.trajectory_enum AS ENUM (
  'Fast Growing', 'Growing', 'Stable', 'Declining', 'Fast Declining'
);
CREATE TYPE dfc.onsite_offshore_enum AS ENUM ('Onsite', 'Offshore');
CREATE TYPE dfc.demand_type_enum AS ENUM ('New Demand', 'Backfill');
CREATE TYPE dfc.billability_enum AS ENUM ('BFD', 'BTB', 'BTM');
-- BFD = Billable For Development, BTB = Billable To Business, BTM = Billable To Margins

CREATE TYPE dfc.task_status_enum AS ENUM ('New', 'In Review', 'Completed');
CREATE TYPE dfc.alert_status_enum AS ENUM ('Action Required', 'Pending Review', 'Finalized');
CREATE TYPE dfc.scenario_status_enum AS ENUM ('Draft', 'Submitted', 'Approved');
CREATE TYPE dfc.feedback_status_enum AS ENUM ('Pending', 'Approved', 'Rejected');
CREATE TYPE dfc.user_role_enum AS ENUM ('SL_COO', 'MARKET_COO', 'CFT_PLANNER');

-- =============================================================================
-- STG LAYER — Raw ingest staging (EDS → Data Mart → here)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- stg.raw_service_order
-- Purpose : Exact copy of source CSV columns from Cognizant Data Mart.
--           Loaded via EDS workflow in bulk/delta fashion.
-- Source  : QuickSO + Resource/BU + Skills DB + Project/Billing systems
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.stg_raw_service_order (
  -- ── Ingestion metadata ────────────────────────────────────────────────────
  stg_id            BIGSERIAL PRIMARY KEY,
  batch_id          UUID         NOT NULL DEFAULT uuid_generate_v4(),
  ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  source_file       TEXT,                 -- originating CSV filename (YTD_2024.csv etc.)
  source_system     TEXT,                 -- 'QuickSO' | 'DataMart_YTD' | 'DataMart_MTD'
  is_processed      BOOLEAN      NOT NULL DEFAULT FALSE,

  -- ── Source fields (kept — DEFAULT_COLS_TO_KEEP in preprocess.py) ─────────
  -- Naming follows exact source column names from upstream CSVs.

  so_line_status                    TEXT,   -- "OPEN", "Cancelled", "Fulfilled"
  unique_id                         TEXT,   -- SO Line unique identifier (SO-XXXXX)
  vertical                          TEXT,   -- Business vertical
  practice                          TEXT,   -- High-level practice
  sub_vertical                      TEXT,
  sub_practice                      TEXT,
  bu                                TEXT,   -- Business Unit (raw, pre-correction)
  parent_customer                   TEXT,
  project_type                      TEXT,   -- "EXTN" | "EXANT" | others (EXTN/EXANT kept)
  project_billability_type          TEXT,   -- "BFD" | "BTB" | "BTM" (filter kept)
  quantity                          INTEGER,-- Headcount / FTE demand count (1 per SO line)
  so_submission_date                DATE,   -- When SO was submitted (SSD for guardrail)
  cancellation_reason               TEXT,
  off_on                            TEXT,   -- "Off" (Offshore) | "On" (Onsite)
  geography                         TEXT,
  country                           TEXT,
  city                              TEXT,
  fulfilment_cancellation_month     TEXT,
  requirement_start_date            DATE,   -- RSD: the target month for demand forecasting
  market                            TEXT,   -- "Americas" | "EMEA" | "GGM" | "APJ"
  so_type                           TEXT,   -- "STA" (kept) | others (dropped)
  so_grade                          TEXT,   -- Raw grade: SA, A, M, PT, PAT, PA, P, cont, etc.
  technical_skills_required         TEXT,   -- Pipe/comma/semicolon-delimited raw skill list
  requirement_type                  TEXT,   -- "New Demand" | "Backfill"
  practice_area                     TEXT,   -- "Digital Engineering" | "ADM" | "EPS" etc.
  service_line                      TEXT,
  original_requirement_start_date   DATE,   -- Kept in STG; dropped before ML training
  revenue_potential                 NUMERIC(15,2), -- Stripped before ML; kept for reference
  sbu1                              TEXT,   -- Strategic BU1; used for EMEA Benelux split
  account_id                        TEXT,
  account_name                      TEXT,   -- Stripped before ML training
  parent_customer_id                TEXT,
  market_unit                       TEXT,   -- Used to map GGM → EMEA/APJ

  -- ── Dropped-column archive (selected high-value fields kept for auditing) ─
  -- Fields from DEFAULT_COLS_TO_REMOVE that have analytical value
  recruiter_id                      TEXT,
  recruiter_name                    TEXT,
  assignment_start_date             DATE,
  requirement_end_date              DATE,
  billability_start_date            DATE,
  job_code                          TEXT,
  skill_family                      TEXT,
  domain_skill_layer_1              TEXT,
  domain_skill_layer_2              TEXT,
  domain_skill_layer_3              TEXT,
  so_priority                       TEXT,
  pe_flagged                        BOOLEAN,
  open_so_ageing                    INTEGER, -- Days open
  cancellation_ageing               INTEGER,

  CONSTRAINT stg_raw_so_unique_id_batch UNIQUE (unique_id, batch_id)
);

CREATE INDEX stg_raw_so_batch_idx        ON dfc.stg_raw_service_order (batch_id);
CREATE INDEX stg_raw_so_processed_idx    ON dfc.stg_raw_service_order (is_processed) WHERE NOT is_processed;
CREATE INDEX stg_raw_so_rsd_idx          ON dfc.stg_raw_service_order (requirement_start_date);
CREATE INDEX stg_raw_so_unique_id_idx    ON dfc.stg_raw_service_order (unique_id);

-- =============================================================================
-- DIM LAYER — Conformed reference dimensions
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_practice_area
-- Source: Practice Area filter values in preprocess.py DEFAULT_FILTERS
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_practice_area (
  pa_id         SERIAL       PRIMARY KEY,
  pa_code       TEXT         NOT NULL UNIQUE, -- "DE" | "ADM" | "EPS"
  pa_name       TEXT         NOT NULL,        -- "Digital Engineering" | "Application Development & Modernization" | "Enterprise Platform Services"
  is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- dim_market
-- Source: Market column (Americas, EMEA after GGM conversion, APJ dropped)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_market (
  market_id     SERIAL       PRIMARY KEY,
  market_code   TEXT         NOT NULL UNIQUE, -- "Americas" | "EMEA"
  region_desc   TEXT,
  is_active     BOOLEAN      NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- dim_business_unit
-- Source: BU column, corrected via SBU-BU mapping file (preprocess.py step 9)
-- EMEA corrections: Mobility dropped, Transport-UK → T&H-UK, Benelux split
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_business_unit (
  bu_id         SERIAL       PRIMARY KEY,
  bu_name       TEXT         NOT NULL,        -- Corrected BU name
  bu_name_raw   TEXT,                         -- Original raw BU before correction
  sbu1          TEXT,                         -- SBU1 value used for Benelux split
  market_id     INTEGER      REFERENCES dfc.dim_market(market_id),
  is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT dim_bu_name_market_uq UNIQUE (bu_name, market_id)
);

CREATE INDEX dim_bu_market_idx ON dfc.dim_business_unit (market_id);

-- ---------------------------------------------------------------------------
-- dim_country
-- Source: Country column; Geography = Off/On; Market for region context
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_country (
  country_id    SERIAL       PRIMARY KEY,
  country_name  TEXT         NOT NULL UNIQUE,
  iso2_code     CHAR(2),
  market_id     INTEGER      REFERENCES dfc.dim_market(market_id),
  default_delivery_mode dfc.onsite_offshore_enum, -- predominant mode for this country
  is_active     BOOLEAN      NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- dim_so_grade
-- Source: SO GRADE column; normalized in preprocess.py
-- Combines: PT/PAT/PA/P → GenC; removes: cont, D, SR. DIR., VP, AVP, Admin Staff, TD Trainee
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_so_grade (
  grade_id          SERIAL   PRIMARY KEY,
  grade_code        TEXT     NOT NULL UNIQUE, -- "SA" | "A" | "M" | "GenC" | "SM" | "AD"
  grade_label       TEXT     NOT NULL,        -- "Senior Associate" | "Analyst" | "Manager" | ...
  grade_raw_values  TEXT[],                   -- Source values merged into this grade
  sort_order        INTEGER,                  -- Display ordering (seniority)
  is_active         BOOLEAN  NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- dim_skill
-- Source: Technical Skills Required → normalized via skill_normalization_llm2.json
-- This table is the master list of all normalized leaf skill names.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_skill (
  skill_id              SERIAL    PRIMARY KEY,
  skill_name            TEXT      NOT NULL UNIQUE, -- Normalized name (e.g., "Java", ".NET")
  skill_name_variants   TEXT[],                    -- Raw variants mapped to this name
  xyz_segment_americas  dfc.xyz_segment_enum,      -- XYZ for Americas market
  xyz_segment_emea      dfc.xyz_segment_enum,      -- XYZ for EMEA market
  total_demand          INTEGER   DEFAULT 0,        -- Σ demand across all years
  demand_2023           INTEGER   DEFAULT 0,
  demand_2024           INTEGER   DEFAULT 0,
  demand_2025           INTEGER   DEFAULT 0,
  rank_change           INTEGER,                    -- Rank delta 2023→2025
  trend                 TEXT,                       -- "Rising" | "Declining" | "Stable"
  is_high_demand        BOOLEAN   NOT NULL DEFAULT FALSE, -- Above MIN_TOTAL_DEMAND_FOR_GROUPS
  is_low_demand         BOOLEAN   NOT NULL DEFAULT FALSE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dim_skill_name_trgm_idx ON dfc.dim_skill USING gin (skill_name gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- dim_skill_cluster
-- Source: skill_clusters.json per market/PA (skills/ directory)
--         Built by skill_clusters_demand.py + apply_clusters.py
-- Cluster naming convention: MSC-{skill1}-{skill2}-{skill3}-...
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_skill_cluster (
  cluster_id        SERIAL       PRIMARY KEY,
  cluster_name      TEXT         NOT NULL,     -- "MSC-.NET-Angular-Azure-C#-Java"
  pa_id             INTEGER      REFERENCES dfc.dim_practice_area(pa_id),
  market_id         INTEGER      REFERENCES dfc.dim_market(market_id),
  num_skills        INTEGER,                   -- Count of leaf skills in cluster
  mapped_demand     INTEGER      DEFAULT 0,    -- Total SOs assigned to this cluster
  cv_score          NUMERIC(6,4) DEFAULT 0,    -- CV across all years
  cv_2025           NUMERIC(6,4) DEFAULT 0,    -- CV for 2025 only (used in XYZ filter)
  xyz_segment       dfc.xyz_segment_enum,
  stability         NUMERIC(6,4) GENERATED ALWAYS AS (GREATEST(0, LEAST(1, 1 - cv_score))) STORED,
  jaccard_threshold NUMERIC(5,4) DEFAULT 0.30, -- Assignment threshold used in apply_clusters.py
  min_jaccard_coverage NUMERIC(5,4),           -- Coverage at this threshold
  trajectory        dfc.trajectory_enum,       -- CAGR-based trajectory classification
  is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT dim_cluster_name_pa_market_uq UNIQUE (cluster_name, pa_id, market_id)
);

CREATE INDEX dim_cluster_pa_idx     ON dfc.dim_skill_cluster (pa_id);
CREATE INDEX dim_cluster_market_idx ON dfc.dim_skill_cluster (market_id);

-- ---------------------------------------------------------------------------
-- dim_cluster_skill_map
-- Resolves the M:N between skill clusters and leaf skills.
-- Source: skill_clusters.json clusters[] array (Jaccard co-occurrence clustering)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.dim_cluster_skill_map (
  cluster_id  INTEGER NOT NULL REFERENCES dfc.dim_skill_cluster(cluster_id),
  skill_id    INTEGER NOT NULL REFERENCES dfc.dim_skill(skill_id),
  weight      NUMERIC(5,4) NOT NULL DEFAULT 1.0, -- Jaccard-based co-occurrence weight
  is_priority BOOLEAN      NOT NULL DEFAULT FALSE, -- Force-assignment priority skill
  is_primary  BOOLEAN      NOT NULL DEFAULT FALSE, -- Second-pass primary skill flag
  PRIMARY KEY (cluster_id, skill_id)
);

-- =============================================================================
-- FACT LAYER — Demand & Prediction facts
-- =============================================================================

-- ---------------------------------------------------------------------------
-- fact_service_order
-- Purpose : Cleaned, validated, de-duped SO records (post preprocess.py).
-- Grain   : One row per SO line (Unique ID).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.fact_service_order (
  so_id                     BIGSERIAL    PRIMARY KEY,
  -- ── Source key ────────────────────────────────────────────────────────────
  unique_id                 TEXT         NOT NULL UNIQUE,  -- Unique ID from SO system
  stg_id                    BIGINT       REFERENCES dfc.stg_raw_service_order(stg_id),

  -- ── Status / type ─────────────────────────────────────────────────────────
  so_line_status            TEXT,         -- Retained for SSD guardrail lookup
  so_type                   TEXT,         -- "STA" (after filter)
  project_type              TEXT,         -- "EXTN" | "EXANT" (after filter)
  requirement_type          dfc.demand_type_enum,   -- New Demand | Backfill
  billability_type          dfc.billability_enum,   -- BFD | BTB | BTM
  cancellation_reason       TEXT,
  is_cancelled              BOOLEAN      GENERATED ALWAYS AS (cancellation_reason <> 'NA' AND cancellation_reason IS NOT NULL) STORED,

  -- ── Dimensions (FK to dim tables) ─────────────────────────────────────────
  pa_id                     INTEGER      NOT NULL REFERENCES dfc.dim_practice_area(pa_id),
  bu_id                     INTEGER      REFERENCES dfc.dim_business_unit(bu_id),
  country_id                INTEGER      REFERENCES dfc.dim_country(country_id),
  grade_id                  INTEGER      NOT NULL REFERENCES dfc.dim_so_grade(grade_id),
  market_id                 INTEGER      NOT NULL REFERENCES dfc.dim_market(market_id),
  cluster_id                INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id), -- NULL until apply_clusters runs

  -- ── Delivery mode ─────────────────────────────────────────────────────────
  delivery_mode             dfc.onsite_offshore_enum, -- Derived from Off/ On field

  -- ── Dates ─────────────────────────────────────────────────────────────────
  requirement_start_date    DATE         NOT NULL,  -- RSD: key forecasting target date
  so_submission_date        DATE,                   -- SSD: used in guardrail
  original_rsd              DATE,                   -- Original Requirement Start date
  rsd_year                  SMALLINT     GENERATED ALWAYS AS (EXTRACT(YEAR FROM requirement_start_date)::SMALLINT) STORED,
  rsd_month                 SMALLINT     GENERATED ALWAYS AS (EXTRACT(MONTH FROM requirement_start_date)::SMALLINT) STORED,
  rsd_quarter               SMALLINT     GENERATED ALWAYS AS (CEIL(EXTRACT(MONTH FROM requirement_start_date) / 3.0)::SMALLINT) STORED,
  is_valid_chronology       BOOLEAN      GENERATED ALWAYS AS (requirement_start_date >= so_submission_date) STORED,

  -- ── Measures ──────────────────────────────────────────────────────────────
  quantity                  INTEGER      NOT NULL DEFAULT 1, -- FTE demand count

  -- ── Reference / non-ML fields ─────────────────────────────────────────────
  account_id                TEXT,
  account_name              TEXT,        -- Stripped before ML; kept here for reference
  parent_customer_id        TEXT,
  parent_customer           TEXT,
  service_line              TEXT,
  vertical                  TEXT,
  sub_vertical              TEXT,
  sub_practice              TEXT,
  revenue_potential         NUMERIC(15,2), -- Stripped before ML

  -- ── Raw skills (pre-normalization) ────────────────────────────────────────
  technical_skills_raw      TEXT,         -- Technical Skills Required (original delimited list)

  -- ── Pipeline processing metadata ──────────────────────────────────────────
  skills_normalized         TEXT,         -- Comma-separated normalized skill names
  not_found_skills          TEXT,         -- Skills not found in normalization mapping
  skill_groups              TEXT,         -- High-demand groupable skills (Skill Groups col)
  cluster_assignment_pass   SMALLINT,     -- 1=Jaccard, 2=Priority, 3=Second-pass, NULL=unmapped
  jaccard_score             NUMERIC(5,4), -- Best Jaccard score at cluster assignment

  -- ── Audit ─────────────────────────────────────────────────────────────────
  loaded_at                 TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX fact_so_rsd_idx      ON dfc.fact_service_order (requirement_start_date);
CREATE INDEX fact_so_rsd_ym_idx   ON dfc.fact_service_order (rsd_year, rsd_month);
CREATE INDEX fact_so_cluster_idx  ON dfc.fact_service_order (cluster_id);
CREATE INDEX fact_so_bu_idx       ON dfc.fact_service_order (bu_id);
CREATE INDEX fact_so_country_idx  ON dfc.fact_service_order (country_id);
CREATE INDEX fact_so_grade_idx    ON dfc.fact_service_order (grade_id);
CREATE INDEX fact_so_pa_idx       ON dfc.fact_service_order (pa_id);
CREATE INDEX fact_so_market_idx   ON dfc.fact_service_order (market_id);
CREATE INDEX fact_so_status_idx   ON dfc.fact_service_order (so_line_status);

-- ---------------------------------------------------------------------------
-- fact_so_skill
-- Purpose : Exploded normalized skills per SO (one row per skill per SO).
--           Supports skill-level demand analysis and co-occurrence computation.
-- Source  : skill_normalized.py → Skills Normalized column (exploded)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.fact_so_skill (
  so_skill_id   BIGSERIAL  PRIMARY KEY,
  so_id         BIGINT     NOT NULL REFERENCES dfc.fact_service_order(so_id) ON DELETE CASCADE,
  skill_id      INTEGER    NOT NULL REFERENCES dfc.dim_skill(skill_id),
  is_normalized BOOLEAN    NOT NULL DEFAULT TRUE, -- FALSE if still in "Not found"
  CONSTRAINT fact_so_skill_uq UNIQUE (so_id, skill_id)
);

CREATE INDEX fact_so_skill_so_idx    ON dfc.fact_so_skill (so_id);
CREATE INDEX fact_so_skill_skill_idx ON dfc.fact_so_skill (skill_id);

-- ---------------------------------------------------------------------------
-- fact_demand_monthly
-- Purpose : Pre-aggregated monthly demand facts for API and frontend use.
--           Grain: (cluster_id, bu_id, country_id, grade_id, pa_id, market_id,
--                   demand_year, demand_month, requirement_type, billability_type)
-- Refreshed: After each data_split.py / preprocess.py run (scheduled or on-demand).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.fact_demand_monthly (
  monthly_id        BIGSERIAL    PRIMARY KEY,
  -- ── Group dimensions ──────────────────────────────────────────────────────
  cluster_id        INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id),
  bu_id             INTEGER      REFERENCES dfc.dim_business_unit(bu_id),
  country_id        INTEGER      REFERENCES dfc.dim_country(country_id),
  grade_id          INTEGER      REFERENCES dfc.dim_so_grade(grade_id),
  pa_id             INTEGER      NOT NULL REFERENCES dfc.dim_practice_area(pa_id),
  market_id         INTEGER      NOT NULL REFERENCES dfc.dim_market(market_id),
  delivery_mode     dfc.onsite_offshore_enum,
  requirement_type  dfc.demand_type_enum,
  billability_type  dfc.billability_enum,
  -- ── Time ──────────────────────────────────────────────────────────────────
  demand_year       SMALLINT     NOT NULL,
  demand_month      SMALLINT     NOT NULL,   -- 1-12
  demand_quarter    SMALLINT     GENERATED ALWAYS AS (CEIL(demand_month / 3.0)::SMALLINT) STORED,
  demand_week       SMALLINT,               -- ISO week (for short-fuse W1-W5 view)
  -- ── Measures ──────────────────────────────────────────────────────────────
  demand_fte        INTEGER      NOT NULL DEFAULT 0,  -- Σ Quantity
  open_fte          INTEGER      NOT NULL DEFAULT 0,  -- Σ where so_line_status = 'OPEN'
  cancelled_fte     INTEGER      NOT NULL DEFAULT 0,  -- Σ where is_cancelled = TRUE
  cancellation_pct  NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE WHEN demand_fte > 0 THEN ROUND(cancelled_fte * 100.0 / demand_fte, 2) ELSE 0 END
  ) STORED,
  -- ── Audit ─────────────────────────────────────────────────────────────────
  computed_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT fact_demand_monthly_uq UNIQUE (
    cluster_id, bu_id, country_id, grade_id, pa_id, market_id,
    delivery_mode, requirement_type, billability_type,
    demand_year, demand_month
  )
);

CREATE INDEX fact_dm_cluster_ym_idx  ON dfc.fact_demand_monthly (cluster_id, demand_year, demand_month);
CREATE INDEX fact_dm_bu_ym_idx       ON dfc.fact_demand_monthly (bu_id, demand_year, demand_month);
CREATE INDEX fact_dm_country_ym_idx  ON dfc.fact_demand_monthly (country_id, demand_year, demand_month);
CREATE INDEX fact_dm_pa_market_idx   ON dfc.fact_demand_monthly (pa_id, market_id);

-- =============================================================================
-- ML LAYER — Model artefacts, predictions, guardrail
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ml_model_run
-- Purpose : Registry of each AutoGluon training run (train_and_predict.py).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.ml_model_run (
  run_id          UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_name        TEXT,
  pa_id           INTEGER      REFERENCES dfc.dim_practice_area(pa_id),
  market_id       INTEGER      REFERENCES dfc.dim_market(market_id),
  group_mode      TEXT         NOT NULL, -- "S" | "RLC" | "BS" | "BRLC"
  -- S=Skill Cluster, RLC=Country+Grade+Cluster, BS=BU+Cluster, BRLC=BU+Grade+Country+Cluster
  model_framework TEXT         NOT NULL DEFAULT 'AutoGluon',
  model_name      TEXT         NOT NULL, -- "Gluon::LightGBM_BAG_L1_FULL" | "Gluon::NeuralNetTorch_BAG_L1"
  train_cutoff    DATE         NOT NULL, -- data_split.py split date (e.g. 2025-06-30)
  forecast_start  DATE         NOT NULL, -- First forecast month (M0)
  months_ahead    SMALLINT     NOT NULL DEFAULT 6,  -- Number of horizon months (M0-M5)
  overall_mape    NUMERIC(6,3),          -- Mean Absolute Percentage Error (%)
  overall_accuracy NUMERIC(6,3),         -- 100 - overall_mape
  overall_rmse    NUMERIC(10,4),
  overall_mae     NUMERIC(10,4),
  demand_pct_filter NUMERIC(5,2),        -- --demand-pct filter value used
  shap_run        BOOLEAN      NOT NULL DEFAULT FALSE,
  run_notes       TEXT,
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- ml_forecast_prediction
-- Purpose : M0-M5 predictions per group (output of train_and_predict.py).
--           After ssd_guardrail.py the corrected columns are populated.
-- Grain   : (run_id, group_label, months_ahead) — one row per horizon month per group
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.ml_forecast_prediction (
  pred_id           BIGSERIAL    PRIMARY KEY,
  run_id            UUID         NOT NULL REFERENCES dfc.ml_model_run(run_id),

  -- ── Group identifier ──────────────────────────────────────────────────────
  group_label       TEXT         NOT NULL, -- Human-readable group key (e.g. "BS_DE_Americas_TechNA")
  prediction_source dfc.prediction_source_enum NOT NULL DEFAULT 'AutoML',

  -- ── Grouping dimension FKs (populated based on group_mode) ───────────────
  cluster_id        INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id),
  bu_id             INTEGER      REFERENCES dfc.dim_business_unit(bu_id),
  country_id        INTEGER      REFERENCES dfc.dim_country(country_id),
  grade_id          INTEGER      REFERENCES dfc.dim_so_grade(grade_id),

  -- ── Forecast horizon ──────────────────────────────────────────────────────
  months_ahead      SMALLINT     NOT NULL, -- 0=M0, 1=M1, ... 5=M5
  forecast_month    DATE         NOT NULL, -- First day of the target month

  -- ── Actuals vs Predictions ────────────────────────────────────────────────
  -- train_and_predict.py All_Predictions sheet columns:
  actual_count      INTEGER,              -- target_count from historical data (NULL if future)
  predicted_count   INTEGER,              -- Raw AutoML prediction
  -- ssd_guardrail.py adds:
  ssd_floor         INTEGER,              -- Confirmed SOs with RSD in forecast month
  predicted_corrected INTEGER,            -- max(predicted_count, ssd_floor)
  correction_applied BOOLEAN    NOT NULL DEFAULT FALSE,

  -- ── Accuracy metrics ──────────────────────────────────────────────────────
  accuracy_pct      NUMERIC(6,3),         -- MAPE-based accuracy (before guardrail)
  accuracy_corrected_pct NUMERIC(6,3),    -- MAPE after guardrail correction

  -- ── Engineered feature snapshot (for explainability) ─────────────────────
  lag_30d_count     INTEGER,
  sma_3m            NUMERIC(10,4),
  sma_6m            NUMERIC(10,4),
  sma_12m           NUMERIC(10,4),
  ema_3m            NUMERIC(10,4),
  ema_6m            NUMERIC(10,4),
  ema_12m           NUMERIC(10,4),
  growth_3m_yoy     NUMERIC(8,4),
  growth_6m_yoy     NUMERIC(8,4),
  growth_9m_yoy     NUMERIC(8,4),
  trend_slope       NUMERIC(10,6),
  trajectory        dfc.trajectory_enum,
  target_month      SMALLINT,             -- 0-11 calendar month index
  fiscal_year_flag  BOOLEAN,

  created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

  CONSTRAINT ml_pred_run_group_horizon_uq UNIQUE (run_id, group_label, months_ahead)
);

CREATE INDEX ml_pred_run_idx      ON dfc.ml_forecast_prediction (run_id);
CREATE INDEX ml_pred_cluster_idx  ON dfc.ml_forecast_prediction (cluster_id);
CREATE INDEX ml_pred_bu_idx       ON dfc.ml_forecast_prediction (bu_id);
CREATE INDEX ml_pred_horizon_idx  ON dfc.ml_forecast_prediction (forecast_month, months_ahead);

-- ---------------------------------------------------------------------------
-- ml_ssd_floor
-- Purpose : Pre-computed SSD floor per (group × forecast window).
--           Written by data_split.py → ssd_floors.csv.
-- Guardrail: max(predicted, ssd_floor) → Predicted_Corrected
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.ml_ssd_floor (
  ssd_id          BIGSERIAL PRIMARY KEY,
  run_id          UUID      NOT NULL REFERENCES dfc.ml_model_run(run_id),
  -- Group dims (matching build_training_groups.py group_by_cols)
  cluster_id      INTEGER   REFERENCES dfc.dim_skill_cluster(cluster_id),
  bu_id           INTEGER   REFERENCES dfc.dim_business_unit(bu_id),
  country_id      INTEGER   REFERENCES dfc.dim_country(country_id),
  grade_id        INTEGER   REFERENCES dfc.dim_so_grade(grade_id),
  -- Window
  forecast_month  DATE      NOT NULL,
  months_ahead    SMALLINT  NOT NULL,
  -- Floor
  confirmed_count INTEGER   NOT NULL DEFAULT 0, -- SOs already in system with RSD in window
  CONSTRAINT ml_ssd_floor_uq UNIQUE (run_id, cluster_id, bu_id, country_id, grade_id, forecast_month, months_ahead)
);

-- ---------------------------------------------------------------------------
-- ml_skill_growth
-- Purpose : Per-skill growth analytics from skill_growth_analysis.json
--           (output of skill_normalized.py step 9)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.ml_skill_growth (
  growth_id       BIGSERIAL    PRIMARY KEY,
  skill_id        INTEGER      NOT NULL REFERENCES dfc.dim_skill(skill_id),
  market_id       INTEGER      NOT NULL REFERENCES dfc.dim_market(market_id),
  pa_id           INTEGER      REFERENCES dfc.dim_practice_area(pa_id),
  -- Demand by year
  demand_2023     INTEGER      DEFAULT 0,
  demand_2024     INTEGER      DEFAULT 0,
  demand_2025     INTEGER      DEFAULT 0,
  -- Rank in each year (1 = highest demand)
  rank_2023       SMALLINT,
  rank_2024       SMALLINT,
  rank_2025       SMALLINT,
  rank_change     SMALLINT,                -- rank_2025 - rank_2023 (negative = improved)
  -- Trajectory
  cagr_pct        NUMERIC(8,4),            -- CAGR from 2023-2025
  trend           TEXT,                    -- "Rising" | "Declining" | "Stable"
  xyz_segment     dfc.xyz_segment_enum,
  computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ml_skill_growth_uq UNIQUE (skill_id, market_id, pa_id)
);

-- =============================================================================
-- APP LAYER — Application entities
-- =============================================================================

-- ---------------------------------------------------------------------------
-- app_user
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_user (
  user_id     UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  email       TEXT         NOT NULL UNIQUE,
  name        TEXT         NOT NULL,
  role        dfc.user_role_enum NOT NULL,
  pa_id       INTEGER      REFERENCES dfc.dim_practice_area(pa_id), -- assigned practice area
  market_id   INTEGER      REFERENCES dfc.dim_market(market_id),
  is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- app_scenario
-- Purpose : Persisted what-if scenarios (POST /api/scenarios)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_scenario (
  scenario_id     UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  name            TEXT         NOT NULL,
  description     TEXT,
  status          dfc.scenario_status_enum NOT NULL DEFAULT 'Draft',
  created_by      UUID         REFERENCES dfc.app_user(user_id),
  -- Filters applied when scenario was created
  filter_pa       TEXT,
  filter_bu       TEXT,
  filter_location TEXT,
  filter_grade    TEXT,
  filter_cluster  TEXT,
  -- Scenario driver inputs (ScenarioDrivers interface)
  -- Maps to POST /api/v1/scenarios/simulate → drivers
  bu_level_growth_pct              NUMERIC(6,3), -- BU-level growth % adjustment
  industry_level_market_spend_pct  NUMERIC(6,3), -- Industry macro signal
  win_rate_strategic_pct           NUMERIC(6,3), -- Win rate on strategic deals
  growth_strategic_pct             NUMERIC(6,3), -- Strategic growth target
  -- Result KPIs (from ScenarioSimulateResponse)
  result_total_base       INTEGER,
  result_scenario_adj     INTEGER,
  result_net_change       INTEGER,
  result_explainability   TEXT[],
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- app_scenario_month_result
-- Purpose : Month-level comparison data from scenario simulation result.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_scenario_month_result (
  result_id       BIGSERIAL  PRIMARY KEY,
  scenario_id     UUID       NOT NULL REFERENCES dfc.app_scenario(scenario_id) ON DELETE CASCADE,
  forecast_month  DATE       NOT NULL,
  baseline_fte    INTEGER    NOT NULL,   -- ML forecast total_base for this month
  scenario_fte    INTEGER    NOT NULL,   -- After driver adjustment
  adjustment_fte  INTEGER    GENERATED ALWAYS AS (scenario_fte - baseline_fte) STORED,
  CONSTRAINT app_scenario_month_uq UNIQUE (scenario_id, forecast_month)
);

-- ---------------------------------------------------------------------------
-- app_feedback
-- Purpose : Management adjustments on top of ML forecasts.
--           Submitted via POST /api/v1/feedback/submit
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_feedback (
  feedback_id   UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  cluster_id    INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id),
  forecast_month DATE        NOT NULL,
  -- ML prediction reference
  pred_id       BIGINT       REFERENCES dfc.ml_forecast_prediction(pred_id),
  system_forecast   INTEGER  NOT NULL,  -- Predicted_Corrected (post guardrail)
  mgmt_adjustment   INTEGER  NOT NULL DEFAULT 0,
  final_forecast    INTEGER  GENERATED ALWAYS AS (system_forecast + mgmt_adjustment) STORED,
  reason        TEXT,
  status        dfc.feedback_status_enum NOT NULL DEFAULT 'Pending',
  submitted_by  UUID         REFERENCES dfc.app_user(user_id),
  reviewed_by   UUID         REFERENCES dfc.app_user(user_id),
  submitted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  reviewed_at   TIMESTAMPTZ,
  CONSTRAINT app_feedback_cluster_month_uq UNIQUE (cluster_id, forecast_month, submitted_by)
);

-- ---------------------------------------------------------------------------
-- app_feedback_skill_update
-- Purpose : Skill taxonomy change requests submitted with feedback.
--           Maps to SkillUpdate[] in FeedbackSubmitRequest
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_feedback_skill_update (
  update_id     BIGSERIAL  PRIMARY KEY,
  feedback_id   UUID       NOT NULL REFERENCES dfc.app_feedback(feedback_id) ON DELETE CASCADE,
  update_type   TEXT       NOT NULL, -- "Newly Added" | "Updated" | "Removed"
  cluster_id    INTEGER    REFERENCES dfc.dim_skill_cluster(cluster_id),
  old_skills    TEXT,
  new_skills    TEXT,
  applied       BOOLEAN    NOT NULL DEFAULT FALSE,
  applied_at    TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- app_task
-- Purpose : Workflow tasks for planners (GET /api/tasks)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_task (
  task_id       UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  task_type     TEXT         NOT NULL,   -- "Update Skill Micro Cluster" | "Conduct Scenario Planning" | "Feedback to Forecast"
  description   TEXT         NOT NULL,
  due_date      DATE         NOT NULL,
  is_overdue    BOOLEAN      NOT NULL DEFAULT FALSE,
  status        dfc.task_status_enum NOT NULL DEFAULT 'New',
  assigned_to   UUID         REFERENCES dfc.app_user(user_id),
  assigned_by   UUID         REFERENCES dfc.app_user(user_id),
  cluster_id    INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id),
  view_link     TEXT,        -- Route path within frontend app
  priority      TEXT         NOT NULL DEFAULT 'Medium', -- "High" | "Medium" | "Low"
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- app_alert
-- Purpose : System-generated alerts for planners (GET /api/alerts)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.app_alert (
  alert_id      UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  alert_type    TEXT         NOT NULL,   -- "Demand Spike" | "Model Accuracy Drop" | "Short Fuse Surge" | etc.
  description   TEXT         NOT NULL,
  due_date      DATE,
  is_overdue    BOOLEAN      NOT NULL DEFAULT FALSE,
  status        dfc.alert_status_enum NOT NULL DEFAULT 'Action Required',
  severity      TEXT         NOT NULL DEFAULT 'Medium', -- "High" | "Medium" | "Low"
  cluster_id    INTEGER      REFERENCES dfc.dim_skill_cluster(cluster_id),
  run_id        UUID         REFERENCES dfc.ml_model_run(run_id), -- triggering model run
  view_link     TEXT,
  category      TEXT,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- =============================================================================
-- AUDIT / PIPELINE LOG
-- =============================================================================

CREATE TABLE IF NOT EXISTS dfc.pipeline_run_log (
  log_id        BIGSERIAL    PRIMARY KEY,
  pipeline_step TEXT         NOT NULL,  -- "preprocess" | "skill_normalized" | "apply_clusters" | "train" | "guardrail"
  status        TEXT         NOT NULL,  -- "started" | "completed" | "failed"
  rows_in       INTEGER,
  rows_out      INTEGER,
  rows_dropped  INTEGER,
  pa_code       TEXT,
  market_code   TEXT,
  run_id        UUID         REFERENCES dfc.ml_model_run(run_id),
  message       TEXT,
  started_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  ended_at      TIMESTAMPTZ
);

-- =============================================================================
-- VIEWS — For API layer convenience
-- =============================================================================

-- Enriched prediction view joining all dims
CREATE OR REPLACE VIEW dfc.v_forecast_prediction AS
SELECT
  p.pred_id,
  p.run_id,
  p.group_label,
  p.prediction_source,
  p.months_ahead,
  p.forecast_month,
  p.actual_count,
  p.predicted_count,
  p.ssd_floor,
  p.predicted_corrected,
  p.correction_applied,
  p.accuracy_pct,
  p.accuracy_corrected_pct,
  p.trajectory,
  -- Cluster dims
  sc.cluster_name,
  sc.cv_score,
  sc.cv_2025,
  sc.xyz_segment,
  sc.stability,
  -- BU
  bu.bu_name,
  -- Country
  c.country_name,
  -- Grade
  g.grade_code,
  g.grade_label,
  -- PA
  pa.pa_code,
  pa.pa_name,
  -- Market
  m.market_code
FROM dfc.ml_forecast_prediction p
LEFT JOIN dfc.dim_skill_cluster  sc ON sc.cluster_id = p.cluster_id
LEFT JOIN dfc.dim_business_unit  bu ON bu.bu_id      = p.bu_id
LEFT JOIN dfc.dim_country        c  ON c.country_id  = p.country_id
LEFT JOIN dfc.dim_so_grade       g  ON g.grade_id    = p.grade_id
LEFT JOIN dfc.dim_practice_area  pa ON pa.pa_id      = sc.pa_id
LEFT JOIN dfc.dim_market         m  ON m.market_id   = sc.market_id;

-- Monthly demand summary view for frontend grid
CREATE OR REPLACE VIEW dfc.v_demand_monthly_summary AS
SELECT
  dm.demand_year,
  dm.demand_month,
  dm.demand_quarter,
  dm.demand_fte,
  dm.open_fte,
  dm.cancelled_fte,
  dm.cancellation_pct,
  dm.delivery_mode,
  dm.requirement_type,
  dm.billability_type,
  sc.cluster_name,
  sc.cv_score,
  sc.xyz_segment,
  bu.bu_name,
  c.country_name,
  g.grade_code,
  g.grade_label,
  pa.pa_code,
  pa.pa_name,
  mk.market_code
FROM dfc.fact_demand_monthly dm
LEFT JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = dm.cluster_id
LEFT JOIN dfc.dim_business_unit bu ON bu.bu_id      = dm.bu_id
LEFT JOIN dfc.dim_country       c  ON c.country_id  = dm.country_id
LEFT JOIN dfc.dim_so_grade      g  ON g.grade_id    = dm.grade_id
LEFT JOIN dfc.dim_practice_area pa ON pa.pa_id      = dm.pa_id
LEFT JOIN dfc.dim_market        mk ON mk.market_id  = dm.market_id;
