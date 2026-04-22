-- =============================================================================
-- DEMAND FORECAST PLANNER — EDS Workflow Ingestion Guide & Helper Queries
-- =============================================================================
-- This file documents how the EDS (Extract, Deliver, Stage) workflow from
-- Cognizant's Data Mart integrates with the PostgreSQL database.
--
-- PRODUCTION DATA FLOW:
--
--  [Source Systems]            [Data Mart]          [PostgreSQL - demand_forecast_db]
--
--  QuickSO (SS1)  ──┐
--  Resource/BU (SS2) ├──→ Cognizant Data Mart ──→ EDS Workflow ──→ stg_raw_service_order
--  Skills DB (SS3)  ─┤         (monthly extract)    (COPY or     (bulk load)
--  Project/Billing (SS4)       YTD + MTD CSVs         INSERT)
--                              DFC_YTD_2024.csv
--                              DFC_YTD_2025.csv
--                              DFC_MTD_current.csv
--
--  After load:
--    sp_full_pipeline_run() ──→ fact_service_order ──→ fact_demand_monthly
--                           ──→ dim_skill updates    ──→ fact_so_skill
--
--  After ML pipeline runs externally (Python):
--    sp_load_ml_predictions()  ──→ ml_forecast_prediction
--    sp_apply_ssd_guardrail()  ──→ (corrections applied)
--    sp_generate_alerts()      ──→ app_alert
--
-- =============================================================================

SET search_path = dfc, public;

-- ---------------------------------------------------------------------------
-- STEP A: Bulk-load YTD CSV via COPY (run via psql or EDS connector)
-- The COPY command maps source CSV headers directly to stg_raw_service_order columns.
-- ---------------------------------------------------------------------------

/*
-- Example COPY command for psql:
\COPY dfc.stg_raw_service_order (
  so_line_status, unique_id, vertical, practice, sub_vertical, sub_practice,
  bu, parent_customer, project_type, project_billability_type, quantity,
  so_submission_date, cancellation_reason, off_on, geography, country, city,
  fulfilment_cancellation_month, requirement_start_date, market, so_type,
  so_grade, technical_skills_required, requirement_type, practice_area,
  service_line, original_requirement_start_date, revenue_potential, sbu1,
  account_id, account_name, parent_customer_id, market_unit,
  recruiter_id, recruiter_name, assignment_start_date, requirement_end_date,
  billability_start_date, job_code, skill_family, domain_skill_layer_1,
  domain_skill_layer_2, domain_skill_layer_3, open_so_ageing,
  cancellation_ageing, so_priority, pe_flagged
)
FROM '/data/DFC_YTD_2024.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');
*/

-- ---------------------------------------------------------------------------
-- STEP B: Run the full pipeline
-- ---------------------------------------------------------------------------

-- Full auto-pipeline (recommended for scheduled EDS loads):
-- SELECT * FROM dfc.sp_full_pipeline_run('DataMart_YTD', 'DFC_YTD_2025.csv');

-- Or run steps individually for MTD loads:
-- SELECT * FROM dfc.sp_ingest_batch('DataMart_MTD', 'DFC_MTD_Jan2026.csv');
-- SELECT   dfc.sp_apply_grade_normalization();
-- SELECT * FROM dfc.sp_apply_market_correction();
-- SELECT   dfc.sp_apply_bu_correction();
-- SELECT * FROM dfc.sp_transform_to_fact();
-- SELECT   dfc.sp_explode_skills();
-- SELECT   dfc.sp_refresh_demand_monthly(2023, 2026);

-- ---------------------------------------------------------------------------
-- STEP C: After ML pipeline run, load predictions
-- (Python wrapper parses Excel → JSON → calls this)
-- ---------------------------------------------------------------------------

/*
-- Register a model run:
INSERT INTO dfc.ml_model_run
  (run_name, pa_id, market_id, group_mode, model_name, train_cutoff, forecast_start)
VALUES (
  'BS_DE_Americas_Jan2026',
  (SELECT pa_id FROM dfc.dim_practice_area WHERE pa_code = 'DE'),
  (SELECT market_id FROM dfc.dim_market WHERE market_code = 'Americas'),
  'BS',
  'Gluon::LightGBM_BAG_L1_FULL',
  '2025-06-30',
  '2026-01-01'
) RETURNING run_id;

-- Load predictions JSON:
SELECT dfc.sp_load_ml_predictions(
  '<run_id_from_above>',
  '[{ "group_label":"...", "cluster_name":"MSC-Java-...", "months_ahead": 0, ... }]'::JSONB
);

-- Apply guardrail:
SELECT * FROM dfc.sp_apply_ssd_guardrail('<run_id>');

-- Generate alerts:
SELECT dfc.sp_generate_alerts('<run_id>');
*/

-- =============================================================================
-- MONITORING QUERIES
-- =============================================================================

-- Check pipeline health
CREATE OR REPLACE VIEW dfc.v_pipeline_health AS
SELECT
  pipeline_step,
  status,
  rows_in,
  rows_out,
  rows_dropped,
  ROUND(rows_dropped::NUMERIC / NULLIF(rows_in, 0) * 100, 1) AS drop_pct,
  started_at,
  ended_at,
  EXTRACT(EPOCH FROM (ended_at - started_at))::INTEGER AS duration_sec,
  message
FROM dfc.pipeline_run_log
ORDER BY started_at DESC;

-- Demand data freshness
CREATE OR REPLACE VIEW dfc.v_data_freshness AS
SELECT
  mk.market_code,
  pa.pa_code,
  MAX(f.loaded_at)                   AS last_load_ts,
  COUNT(*)                           AS total_so_records,
  MIN(f.requirement_start_date)      AS earliest_rsd,
  MAX(f.requirement_start_date)      AS latest_rsd,
  COUNT(DISTINCT f.cluster_id)       AS clusters_assigned,
  COUNT(*) FILTER (WHERE f.cluster_id IS NULL) AS unassigned_rows,
  ROUND(COUNT(*) FILTER (WHERE f.cluster_id IS NULL)::NUMERIC / COUNT(*) * 100, 1) AS unassigned_pct
FROM dfc.fact_service_order f
JOIN dfc.dim_market        mk ON mk.market_id = f.market_id
JOIN dfc.dim_practice_area pa ON pa.pa_id     = f.pa_id
GROUP BY mk.market_code, pa.pa_code;

-- Latest ML run accuracy summary
CREATE OR REPLACE VIEW dfc.v_latest_model_accuracy AS
SELECT
  r.run_name,
  r.group_mode,
  r.model_name,
  r.train_cutoff,
  r.forecast_start,
  r.overall_accuracy,
  r.overall_mape,
  r.overall_rmse,
  COUNT(p.pred_id)                                         AS prediction_rows,
  COUNT(p.pred_id) FILTER (WHERE p.correction_applied)     AS guardrail_corrections,
  ROUND(AVG(p.accuracy_corrected_pct), 2)                  AS avg_corrected_accuracy,
  r.created_at
FROM dfc.ml_model_run r
LEFT JOIN dfc.ml_forecast_prediction p ON p.run_id = r.run_id
GROUP BY r.run_id, r.run_name, r.group_mode, r.model_name,
         r.train_cutoff, r.forecast_start, r.overall_accuracy,
         r.overall_mape, r.overall_rmse, r.created_at
ORDER BY r.created_at DESC;

-- Open alerts summary
CREATE OR REPLACE VIEW dfc.v_open_alerts AS
SELECT
  a.alert_type,
  a.severity,
  sc.cluster_name,
  a.description,
  a.is_overdue,
  a.status,
  a.created_at
FROM dfc.app_alert a
LEFT JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = a.cluster_id
WHERE a.status <> 'Finalized'
ORDER BY
  CASE a.severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
  a.created_at DESC;

-- Skill growth summary (for Taxonomy page)
CREATE OR REPLACE VIEW dfc.v_skill_growth_summary AS
SELECT
  s.skill_name,
  sg.demand_2023,
  sg.demand_2024,
  sg.demand_2025,
  sg.rank_2023,
  sg.rank_2025,
  sg.rank_change,
  sg.cagr_pct,
  sg.trend,
  s.xyz_segment_americas AS xyz_americas,
  s.xyz_segment_emea     AS xyz_emea,
  s.is_high_demand,
  mk.market_code
FROM dfc.ml_skill_growth sg
JOIN dfc.dim_skill  s  ON s.skill_id  = sg.skill_id
JOIN dfc.dim_market mk ON mk.market_id = sg.market_id
ORDER BY sg.demand_2025 DESC NULLS LAST;

-- =============================================================================
-- FIELD MAPPING QUICK REFERENCE (queryable)
-- =============================================================================

-- All kept fields with their full mapping chain:
-- SELECT
--   source_system,
--   source_field_name                                    AS "Source Field (Data Mart)",
--   stg_column                                           AS "STG Column",
--   COALESCE(fact_column, '(derived)')                   AS "Fact/Dim Column",
--   COALESCE(api_field,   '(not exposed)')               AS "API JSON Field",
--   COALESCE(frontend_field, '(not in UI)')              AS "Frontend TypeScript Field",
--   transformation                                       AS "Business Rule / Transform"
-- FROM dfc.field_mapping_catalog
-- WHERE drop_reason IS NULL
-- ORDER BY source_system, source_field_name;

-- All dropped fields:
-- SELECT source_system, source_field_name, drop_reason
-- FROM dfc.field_mapping_catalog
-- WHERE drop_reason IS NOT NULL
-- ORDER BY source_system, source_field_name;

-- =============================================================================
-- INDEX MAINTENANCE
-- =============================================================================

-- Run after large batch loads:
-- ANALYZE dfc.stg_raw_service_order;
-- ANALYZE dfc.fact_service_order;
-- ANALYZE dfc.fact_demand_monthly;
-- ANALYZE dfc.ml_forecast_prediction;
-- VACUUM ANALYZE dfc.fact_service_order;
