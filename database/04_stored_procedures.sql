-- =============================================================================
-- DEMAND FORECAST PLANNER — Stored Procedures & EDS Ingestion Functions
-- =============================================================================
-- These procedures support the EDS (Extract, Deliver, Stage) workflow that
-- flows data from Cognizant's Data Mart into the PostgreSQL demand_forecast_db.
--
-- Call sequence:
--   1. sp_ingest_batch()              → bulk-load from stg_raw_service_order
--   2. sp_transform_to_fact()         → STG → fact_service_order (cleaned + FK-resolved)
--   3. sp_apply_grade_normalization() → normalize SO GRADE per preprocess.py rules
--   4. sp_apply_market_correction()   → GGM → EMEA/APJ, drop APJ
--   5. sp_apply_bu_correction()       → SBU-BU mapping for Americas EMEA corrections
--   6. sp_refresh_demand_monthly()    → rebuild fact_demand_monthly aggregates
--   7. sp_load_ml_predictions()       → load AutoGluon output Excel → ml_forecast_prediction
--   8. sp_apply_ssd_guardrail()       → apply guardrail corrections
-- =============================================================================

SET search_path = dfc, public;

-- =============================================================================
-- 1. sp_ingest_batch
-- Purpose : Marks all unprocessed STG rows in a batch and returns counts.
--           Called by EDS after CSV files are bulk-loaded via COPY.
-- Usage   : SELECT * FROM dfc.sp_ingest_batch('DataMart_YTD', 'YTD_2025.csv');
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_ingest_batch(
  p_source_system TEXT,
  p_source_file   TEXT
)
RETURNS TABLE (batch_id UUID, rows_staged INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
  v_batch_id  UUID := uuid_generate_v4();
  v_rows_in   INTEGER;
BEGIN
  -- Tag unprocessed rows with the new batch_id
  UPDATE dfc.stg_raw_service_order
  SET    batch_id      = v_batch_id,
         source_system = p_source_system,
         source_file   = p_source_file
  WHERE  is_processed  = FALSE
    AND  source_system IS NULL;

  GET DIAGNOSTICS v_rows_in = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_in, pa_code, message)
  VALUES ('ingest_batch', 'completed', v_rows_in, NULL,
          format('Batch %s tagged: %s rows from %s / %s',
                 v_batch_id, v_rows_in, p_source_system, p_source_file));

  RETURN QUERY SELECT v_batch_id, v_rows_in;
END;
$$;

-- =============================================================================
-- 2. sp_apply_grade_normalization
-- Purpose : Normalize SO GRADE values per preprocess.py rules.
--           Combines PT/PAT/PA/P → GenC
--           Nullifies (marks for drop) rows with removed grades.
-- Matches : DEFAULT_GRADES_TO_COMBINE + DEFAULT_GRADES_TO_REMOVE in preprocess.py
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_apply_grade_normalization(p_batch_id UUID DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_rows_updated INTEGER;
BEGIN
  -- Combine grades: PT, PAT, PA, P → GenC
  UPDATE dfc.stg_raw_service_order
  SET    so_grade = 'GenC'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(so_grade)) = ANY(ARRAY['PT','PAT','PA','P']);

  -- Nullify removed grades (will be filtered out in sp_transform_to_fact)
  UPDATE dfc.stg_raw_service_order
  SET    so_grade = NULL
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  lower(trim(so_grade)) = ANY(
           ARRAY['cont','d','sr. dir.','vp','avp','admin staff','td trainee']
         );

  GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out, message)
  VALUES ('grade_normalization', 'completed', v_rows_updated,
          format('Batch %s: %s grade rows updated/nulled', p_batch_id, v_rows_updated));

  RETURN v_rows_updated;
END;
$$;

-- =============================================================================
-- 3. sp_apply_market_correction
-- Purpose : Convert GGM market rows to EMEA/Americas using Market Unit.
--           Drop all APJ rows.
-- Matches : preprocess.py step 10
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_apply_market_correction(p_batch_id UUID DEFAULT NULL)
RETURNS TABLE (rows_converted INTEGER, rows_dropped INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
  v_converted INTEGER;
  v_dropped   INTEGER;
BEGIN
  -- GGM → Americas (Market Unit contains Americas identifiers)
  UPDATE dfc.stg_raw_service_order
  SET    market = 'Americas'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'GGM'
    AND  (upper(trim(market_unit)) LIKE '%AMERICAS%'
       OR upper(trim(market_unit)) LIKE '%NA%'
       OR upper(trim(market_unit)) LIKE '%NORTH AMERICA%');

  -- GGM → EMEA (Market Unit contains EMEA identifiers)
  UPDATE dfc.stg_raw_service_order
  SET    market = 'EMEA'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'GGM'
    AND  (upper(trim(market_unit)) LIKE '%EMEA%'
       OR upper(trim(market_unit)) LIKE '%EUROPE%'
       OR upper(trim(market_unit)) LIKE '%UK%'
       OR upper(trim(market_unit)) LIKE '%MIDDLE EAST%'
       OR upper(trim(market_unit)) LIKE '%AFRICA%');

  GET DIAGNOSTICS v_converted = ROW_COUNT;

  -- Mark APJ rows as processed (effectively dropping them from the pipeline)
  UPDATE dfc.stg_raw_service_order
  SET    is_processed = TRUE,  -- skip these in transform
         market       = 'APJ_DROPPED'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) IN ('APJ', 'ASIA PACIFIC');

  GET DIAGNOSTICS v_dropped = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_in, rows_dropped, message)
  VALUES ('market_correction', 'completed', v_converted, v_dropped,
          format('Batch %s: %s rows market-corrected, %s APJ rows dropped',
                 p_batch_id, v_converted, v_dropped));

  RETURN QUERY SELECT v_converted, v_dropped;
END;
$$;

-- =============================================================================
-- 4. sp_apply_bu_correction
-- Purpose : Apply BU corrections for Americas (SBU-BU mapping) and EMEA.
--           Americas: lookup corrected BU from dim_business_unit by sbu1.
--           EMEA: Mobility dropped, Transport-UK → T&H-UK,
--                 RCGT&H-UK merge, South Europe → SPAI,
--                 Benelux → Belux / Netherlands by SBU1.
-- Matches : preprocess.py steps 9, 11
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_apply_bu_correction(p_batch_id UUID DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_rows INTEGER;
BEGIN
  -- EMEA: Drop Mobility BU rows
  UPDATE dfc.stg_raw_service_order
  SET    is_processed = TRUE
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'EMEA'
    AND  upper(trim(bu)) LIKE '%MOBILITY%';

  -- EMEA: Transport-UK → T&H-UK
  UPDATE dfc.stg_raw_service_order
  SET    bu = 'T&H-UK'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'EMEA'
    AND  upper(trim(bu)) LIKE '%TRANSPORT%UK%';

  -- EMEA: South Europe → SPAI
  UPDATE dfc.stg_raw_service_order
  SET    bu = 'SPAI'
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'EMEA'
    AND  (upper(trim(bu)) LIKE '%SOUTH EUROPE%'
       OR upper(trim(bu)) LIKE '%SPAIN%');

  -- EMEA: Benelux split by SBU1
  UPDATE dfc.stg_raw_service_order
  SET    bu = CASE
               WHEN upper(trim(sbu1)) LIKE '%NETHER%' OR upper(trim(sbu1)) LIKE '%NL%' THEN 'Netherlands'
               ELSE 'Belux'
             END
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE
    AND  upper(trim(market)) = 'EMEA'
    AND  upper(trim(bu)) LIKE '%BENELUX%';

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out, message)
  VALUES ('bu_correction', 'completed', v_rows,
          format('Batch %s: EMEA BU corrections applied to %s rows', p_batch_id, v_rows));

  RETURN v_rows;
END;
$$;

-- =============================================================================
-- 5. sp_transform_to_fact
-- Purpose : Move validated STG rows into fact_service_order with FK resolution.
--           Applies all business filters from preprocess.py DEFAULT_FILTERS.
--           Rows failing filters are skipped (is_processed = TRUE, not inserted).
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_transform_to_fact(p_batch_id UUID DEFAULT NULL)
RETURNS TABLE (rows_inserted INTEGER, rows_skipped INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
  v_inserted INTEGER;
  v_skipped  INTEGER;
BEGIN
  -- Insert validated rows into fact_service_order
  INSERT INTO dfc.fact_service_order (
    unique_id, stg_id,
    so_line_status, so_type, project_type, requirement_type, billability_type, cancellation_reason,
    pa_id, bu_id, country_id, grade_id, market_id,
    delivery_mode,
    requirement_start_date, so_submission_date, original_rsd,
    quantity,
    account_id, account_name, parent_customer_id, parent_customer,
    service_line, vertical, sub_vertical, sub_practice, revenue_potential,
    technical_skills_raw
  )
  SELECT
    s.unique_id,
    s.stg_id,
    s.so_line_status,
    s.so_type,
    s.project_type,
    s.requirement_type::dfc.demand_type_enum,
    s.project_billability_type::dfc.billability_enum,
    s.cancellation_reason,
    -- FK lookups
    pa.pa_id,
    bu.bu_id,
    c.country_id,
    g.grade_id,
    m.market_id,
    -- Delivery mode: "Off" → Offshore, "On" → Onsite
    CASE WHEN lower(trim(s.off_on)) = 'off' THEN 'Offshore'::dfc.onsite_offshore_enum
         WHEN lower(trim(s.off_on)) = 'on'  THEN 'Onsite'::dfc.onsite_offshore_enum
         ELSE NULL END,
    -- Dates (already parsed in STG load)
    s.requirement_start_date,
    s.so_submission_date,
    s.original_requirement_start_date,
    COALESCE(s.quantity, 1),
    s.account_id, s.account_name, s.parent_customer_id, s.parent_customer,
    s.service_line, s.vertical, s.sub_vertical, s.sub_practice, s.revenue_potential,
    s.technical_skills_required
  FROM dfc.stg_raw_service_order s
  -- FK joins
  LEFT JOIN dfc.dim_practice_area pa
    ON  upper(trim(pa.pa_name)) = upper(trim(s.practice_area))
     OR upper(trim(pa.pa_code)) = upper(trim(s.practice_area))
  LEFT JOIN dfc.dim_market m
    ON  upper(trim(m.market_code)) = upper(trim(s.market))
  LEFT JOIN dfc.dim_business_unit bu
    ON  upper(trim(bu.bu_name)) = upper(trim(s.bu))
    AND bu.market_id = m.market_id
  LEFT JOIN dfc.dim_country c
    ON  upper(trim(c.country_name)) = upper(trim(s.country))
  LEFT JOIN dfc.dim_so_grade g
    ON  upper(trim(g.grade_code)) = upper(trim(s.so_grade))
  WHERE
    (p_batch_id IS NULL OR s.batch_id = p_batch_id)
    AND s.is_processed = FALSE
    -- ── Filters matching preprocess.py DEFAULT_FILTERS ────────────────────
    AND upper(trim(s.so_type)) = 'STA'
    AND upper(trim(s.project_type)) = ANY(ARRAY['EXTN','EXANT'])
    AND upper(trim(s.market)) = ANY(ARRAY['AMERICAS','EMEA'])
    AND upper(trim(s.project_billability_type)) = ANY(ARRAY['BFD','BTB','BTM'])
    -- Cancellation reason filter (10 kept values)
    AND (
      s.cancellation_reason IS NULL
      OR trim(s.cancellation_reason) = 'NA'
      OR trim(s.cancellation_reason) = ANY(ARRAY[
        'Project/Requirement postponed or on hold by client',
        'Opportunity Lost',
        'Alternate Transactional SO created',
        'Replace by Internal Fulfilment - Allocation',
        'Requirement staffed by client/other vendor',
        'SO Criticality Change',
        'Project Preponement',
        'Staffing Challenge',
        'Labor Market Testing Unsuccessful'
      ])
    )
    -- Grade filter: NULL grades (removed) are excluded
    AND s.so_grade IS NOT NULL
    -- Date validation: RSD must be >= SSD (chronological check)
    AND (s.so_submission_date IS NULL OR s.requirement_start_date >= s.so_submission_date)
    -- Year range filter (2023-2025)
    AND EXTRACT(YEAR FROM s.requirement_start_date) BETWEEN 2023 AND 2025
    -- PA must exist in dim table
    AND pa.pa_id IS NOT NULL
  ON CONFLICT (unique_id) DO UPDATE
    SET so_line_status   = EXCLUDED.so_line_status,
        updated_at       = now();

  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  -- Mark all processed (inserted + skipped) as done
  UPDATE dfc.stg_raw_service_order
  SET    is_processed = TRUE
  WHERE  (p_batch_id IS NULL OR batch_id = p_batch_id)
    AND  is_processed = FALSE;

  GET DIAGNOSTICS v_skipped = ROW_COUNT;
  v_skipped := GREATEST(0, v_skipped - v_inserted);

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_in, rows_out, rows_dropped)
  VALUES ('transform_to_fact', 'completed', v_inserted + v_skipped, v_inserted, v_skipped);

  RETURN QUERY SELECT v_inserted, v_skipped;
END;
$$;

-- =============================================================================
-- 6. sp_explode_skills
-- Purpose : Explode skills_normalized (comma-separated) into fact_so_skill rows.
--           Resolves each skill name to skill_id in dim_skill.
--           Creates new dim_skill rows for skills not yet catalogued.
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_explode_skills(p_batch_id UUID DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_rows INTEGER;
BEGIN
  -- Auto-create dim_skill entries for any new normalized skills
  INSERT INTO dfc.dim_skill (skill_name, is_high_demand)
  SELECT DISTINCT trim(skill_token), FALSE
  FROM dfc.fact_service_order f
  JOIN dfc.stg_raw_service_order s ON s.stg_id = f.stg_id
  CROSS JOIN LATERAL regexp_split_to_table(f.skills_normalized, ',\s*') AS skill_token
  WHERE (p_batch_id IS NULL OR s.batch_id = p_batch_id)
    AND trim(skill_token) <> ''
  ON CONFLICT (skill_name) DO NOTHING;

  -- Insert exploded skill rows
  INSERT INTO dfc.fact_so_skill (so_id, skill_id, is_normalized)
  SELECT DISTINCT
    f.so_id,
    d.skill_id,
    TRUE
  FROM dfc.fact_service_order f
  JOIN dfc.stg_raw_service_order s  ON s.stg_id = f.stg_id
  CROSS JOIN LATERAL regexp_split_to_table(f.skills_normalized, ',\s*') AS skill_token
  JOIN dfc.dim_skill d ON d.skill_name = trim(skill_token)
  WHERE (p_batch_id IS NULL OR s.batch_id = p_batch_id)
    AND trim(skill_token) <> ''
    AND f.skills_normalized IS NOT NULL
  ON CONFLICT (so_id, skill_id) DO NOTHING;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out)
  VALUES ('explode_skills', 'completed', v_rows);

  RETURN v_rows;
END;
$$;

-- =============================================================================
-- 7. sp_refresh_demand_monthly
-- Purpose : Rebuild fact_demand_monthly aggregation table from fact_service_order.
--           Called after every batch ingest or cluster assignment update.
--           Uses UPSERT to handle incremental refreshes.
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_refresh_demand_monthly(
  p_year_from SMALLINT DEFAULT 2023,
  p_year_to   SMALLINT DEFAULT 2025
)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_rows INTEGER;
BEGIN
  INSERT INTO dfc.fact_demand_monthly (
    cluster_id, bu_id, country_id, grade_id, pa_id, market_id,
    delivery_mode, requirement_type, billability_type,
    demand_year, demand_month,
    demand_fte, open_fte, cancelled_fte
  )
  SELECT
    f.cluster_id,
    f.bu_id,
    f.country_id,
    f.grade_id,
    f.pa_id,
    f.market_id,
    f.delivery_mode,
    f.requirement_type,
    f.billability_type,
    f.rsd_year  AS demand_year,
    f.rsd_month AS demand_month,
    SUM(f.quantity)                                           AS demand_fte,
    SUM(CASE WHEN f.so_line_status = 'OPEN' THEN f.quantity ELSE 0 END) AS open_fte,
    SUM(CASE WHEN f.is_cancelled THEN f.quantity ELSE 0 END)  AS cancelled_fte
  FROM dfc.fact_service_order f
  WHERE f.rsd_year BETWEEN p_year_from AND p_year_to
  GROUP BY
    f.cluster_id, f.bu_id, f.country_id, f.grade_id, f.pa_id, f.market_id,
    f.delivery_mode, f.requirement_type, f.billability_type,
    f.rsd_year, f.rsd_month
  ON CONFLICT (cluster_id, bu_id, country_id, grade_id, pa_id, market_id,
               delivery_mode, requirement_type, billability_type,
               demand_year, demand_month)
  DO UPDATE SET
    demand_fte   = EXCLUDED.demand_fte,
    open_fte     = EXCLUDED.open_fte,
    cancelled_fte = EXCLUDED.cancelled_fte,
    computed_at  = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out)
  VALUES ('refresh_demand_monthly', 'completed', v_rows);

  RETURN v_rows;
END;
$$;

-- =============================================================================
-- 8. sp_load_ml_predictions
-- Purpose : Load ML predictions from train_and_predict.py Excel output
--           (parsed externally to JSON/CSV by a Python wrapper before calling this).
--
-- p_predictions: JSON array of prediction rows with structure:
--   [{
--     "group_label": "BS_DE_Americas_TechNA",
--     "model_name":  "Gluon::LightGBM_BAG_L1_FULL",
--     "prediction_source": "AutoML",
--     "cluster_name": "MSC-Java-Kafka-Microservices-Python-Spring_Boot",
--     "bu_name":  "Technology NA",
--     "country_name": null,
--     "grade_code": null,
--     "months_ahead": 0,
--     "forecast_month": "2026-01-01",
--     "actual_count": 145,
--     "predicted_count": 138,
--     "accuracy_pct": 95.17,
--     "lag_30d_count": 112,
--     "sma_3m": 128.3,
--     "trend_slope": 0.043,
--     "trajectory": "Growing"
--   }]
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_load_ml_predictions(
  p_run_id        UUID,
  p_predictions   JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_row        JSONB;
  v_cluster_id INTEGER;
  v_bu_id      INTEGER;
  v_country_id INTEGER;
  v_grade_id   INTEGER;
  v_inserted   INTEGER := 0;
BEGIN
  FOR v_row IN SELECT jsonb_array_elements(p_predictions)
  LOOP
    -- Resolve dimension FKs
    SELECT cluster_id INTO v_cluster_id
    FROM dfc.dim_skill_cluster
    WHERE cluster_name = v_row->>'cluster_name' LIMIT 1;

    SELECT bu_id INTO v_bu_id
    FROM dfc.dim_business_unit
    WHERE upper(bu_name) = upper(v_row->>'bu_name') LIMIT 1;

    SELECT country_id INTO v_country_id
    FROM dfc.dim_country
    WHERE upper(country_name) = upper(v_row->>'country_name') LIMIT 1;

    SELECT grade_id INTO v_grade_id
    FROM dfc.dim_so_grade
    WHERE upper(grade_code) = upper(v_row->>'grade_code') LIMIT 1;

    INSERT INTO dfc.ml_forecast_prediction (
      run_id, group_label, prediction_source,
      cluster_id, bu_id, country_id, grade_id,
      months_ahead, forecast_month,
      actual_count, predicted_count, accuracy_pct,
      lag_30d_count, sma_3m, sma_6m, sma_12m,
      ema_3m, ema_6m, ema_12m,
      growth_3m_yoy, growth_6m_yoy, growth_9m_yoy,
      trend_slope, trajectory,
      target_month, fiscal_year_flag
    ) VALUES (
      p_run_id,
      v_row->>'group_label',
      (v_row->>'prediction_source')::dfc.prediction_source_enum,
      v_cluster_id,
      v_bu_id,
      v_country_id,
      v_grade_id,
      (v_row->>'months_ahead')::SMALLINT,
      (v_row->>'forecast_month')::DATE,
      (v_row->>'actual_count')::INTEGER,
      (v_row->>'predicted_count')::INTEGER,
      (v_row->>'accuracy_pct')::NUMERIC,
      (v_row->>'lag_30d_count')::INTEGER,
      (v_row->>'sma_3m')::NUMERIC,
      (v_row->>'sma_6m')::NUMERIC,
      (v_row->>'sma_12m')::NUMERIC,
      (v_row->>'ema_3m')::NUMERIC,
      (v_row->>'ema_6m')::NUMERIC,
      (v_row->>'ema_12m')::NUMERIC,
      (v_row->>'growth_3m_yoy')::NUMERIC,
      (v_row->>'growth_6m_yoy')::NUMERIC,
      (v_row->>'growth_9m_yoy')::NUMERIC,
      (v_row->>'trend_slope')::NUMERIC,
      (v_row->>'trajectory')::dfc.trajectory_enum,
      (v_row->>'target_month')::SMALLINT,
      (v_row->>'fiscal_year_flag')::BOOLEAN
    )
    ON CONFLICT (run_id, group_label, months_ahead)
    DO UPDATE SET
      predicted_count  = EXCLUDED.predicted_count,
      actual_count     = EXCLUDED.actual_count,
      accuracy_pct     = EXCLUDED.accuracy_pct,
      prediction_source = EXCLUDED.prediction_source;

    v_inserted := v_inserted + 1;
  END LOOP;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out, run_id)
  VALUES ('load_ml_predictions', 'completed', v_inserted, p_run_id);

  RETURN v_inserted;
END;
$$;

-- =============================================================================
-- 9. sp_apply_ssd_guardrail
-- Purpose : Apply SSD guardrail corrections to ml_forecast_prediction.
--           For each prediction where predicted_count < ssd_floor,
--           set predicted_corrected = ssd_floor and correction_applied = TRUE.
-- Matches : ssd_guardrail.py logic: predicted_corrected = max(predicted, ssd_floor)
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_apply_ssd_guardrail(p_run_id UUID)
RETURNS TABLE (rows_corrected INTEGER, rows_unchanged INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
  v_corrected   INTEGER;
  v_unchanged   INTEGER;
BEGIN
  -- Load SSD floors from ml_ssd_floor into predictions
  UPDATE dfc.ml_forecast_prediction p
  SET
    ssd_floor           = f.confirmed_count,
    predicted_corrected = GREATEST(p.predicted_count, f.confirmed_count),
    correction_applied  = (f.confirmed_count > p.predicted_count),
    prediction_source   = CASE
                            WHEN f.confirmed_count > p.predicted_count
                            THEN 'Guardrail_Corrected'::dfc.prediction_source_enum
                            ELSE p.prediction_source
                          END
  FROM dfc.ml_ssd_floor f
  WHERE p.run_id        = p_run_id
    AND f.run_id        = p_run_id
    AND p.cluster_id    = f.cluster_id
    AND (p.bu_id        = f.bu_id      OR (p.bu_id IS NULL AND f.bu_id IS NULL))
    AND (p.country_id   = f.country_id OR (p.country_id IS NULL AND f.country_id IS NULL))
    AND (p.grade_id     = f.grade_id   OR (p.grade_id IS NULL AND f.grade_id IS NULL))
    AND p.forecast_month = f.forecast_month
    AND p.months_ahead   = f.months_ahead;

  GET DIAGNOSTICS v_corrected = ROW_COUNT;

  -- For rows with no SSD floor, set predicted_corrected = predicted_count
  UPDATE dfc.ml_forecast_prediction
  SET    predicted_corrected = predicted_count,
         correction_applied  = FALSE
  WHERE  run_id    = p_run_id
    AND  predicted_corrected IS NULL
    AND  predicted_count IS NOT NULL;

  GET DIAGNOSTICS v_unchanged = ROW_COUNT;

  -- Compute corrected accuracy (MAPE-based, same formula as train_and_predict.py)
  UPDATE dfc.ml_forecast_prediction
  SET    accuracy_corrected_pct =
    CASE WHEN actual_count IS NOT NULL AND actual_count > 0
         THEN ROUND(100.0 - (ABS(predicted_corrected - actual_count) * 100.0 / actual_count), 3)
         ELSE NULL END
  WHERE  run_id = p_run_id
    AND  predicted_corrected IS NOT NULL;

  INSERT INTO dfc.pipeline_run_log (pipeline_step, status, rows_out, rows_dropped, run_id, message)
  VALUES ('ssd_guardrail', 'completed', v_corrected, v_unchanged, p_run_id,
          format('Run %s: %s rows guardrail-corrected, %s unchanged', p_run_id, v_corrected, v_unchanged));

  RETURN QUERY SELECT v_corrected, v_unchanged;
END;
$$;

-- =============================================================================
-- 10. sp_full_pipeline_run
-- Purpose : Orchestrate the complete EDS → fact → ML refresh pipeline.
--           Convenience wrapper to call steps 1-7 in sequence.
-- Usage   : SELECT * FROM dfc.sp_full_pipeline_run('DataMart_YTD', 'YTD_2025.csv');
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_full_pipeline_run(
  p_source_system TEXT DEFAULT 'DataMart_YTD',
  p_source_file   TEXT DEFAULT NULL
)
RETURNS TABLE (step TEXT, result TEXT)
LANGUAGE plpgsql AS $$
DECLARE
  v_batch_id    UUID;
  v_rows        INTEGER;
  v_inserted    INTEGER;
  v_skipped     INTEGER;
BEGIN
  -- Step 1: Tag batch
  SELECT b.batch_id, b.rows_staged INTO v_batch_id, v_rows
  FROM dfc.sp_ingest_batch(p_source_system, COALESCE(p_source_file, 'manual_run')) AS b;
  RETURN QUERY SELECT 'ingest_batch'::TEXT, format('%s rows staged, batch=%s', v_rows, v_batch_id);

  -- Step 2: Grade normalization
  SELECT dfc.sp_apply_grade_normalization(v_batch_id) INTO v_rows;
  RETURN QUERY SELECT 'grade_normalization'::TEXT, format('%s rows normalized', v_rows);

  -- Step 3: Market correction
  SELECT c.rows_converted, c.rows_dropped INTO v_inserted, v_skipped
  FROM dfc.sp_apply_market_correction(v_batch_id) AS c;
  RETURN QUERY SELECT 'market_correction'::TEXT, format('%s converted, %s APJ dropped', v_inserted, v_skipped);

  -- Step 4: BU correction
  SELECT dfc.sp_apply_bu_correction(v_batch_id) INTO v_rows;
  RETURN QUERY SELECT 'bu_correction'::TEXT, format('%s rows corrected', v_rows);

  -- Step 5: Transform to fact
  SELECT t.rows_inserted, t.rows_skipped INTO v_inserted, v_skipped
  FROM dfc.sp_transform_to_fact(v_batch_id) AS t;
  RETURN QUERY SELECT 'transform_to_fact'::TEXT, format('%s inserted, %s skipped', v_inserted, v_skipped);

  -- Step 6: Explode skills
  SELECT dfc.sp_explode_skills(v_batch_id) INTO v_rows;
  RETURN QUERY SELECT 'explode_skills'::TEXT, format('%s skill rows created', v_rows);

  -- Step 7: Refresh monthly aggregates
  SELECT dfc.sp_refresh_demand_monthly(2023, 2025) INTO v_rows;
  RETURN QUERY SELECT 'refresh_demand_monthly'::TEXT, format('%s monthly rows upserted', v_rows);

  RETURN QUERY SELECT 'pipeline_complete'::TEXT, format('Batch %s complete', v_batch_id);
END;
$$;

-- =============================================================================
-- 11. sp_generate_alerts
-- Purpose : Auto-generate alerts based on ML prediction thresholds.
--           Called after sp_apply_ssd_guardrail.
-- Alert types:
--   - Demand Spike: predicted_corrected > 1.5× previous month
--   - Model Accuracy Drop: accuracy_pct < 80
--   - Short Fuse Surge: confirmed SOs (ssd_floor) > predicted_count
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.sp_generate_alerts(p_run_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  -- Demand Spike alert
  INSERT INTO dfc.app_alert (alert_type, description, severity, cluster_id, run_id, view_link)
  SELECT
    'Demand Spike',
    format('Cluster %s shows %.0f%% demand increase in %s vs prior month',
           sc.cluster_name,
           (p.predicted_corrected::FLOAT / NULLIF(p_prev.predicted_corrected, 0) - 1) * 100,
           to_char(p.forecast_month, 'Mon YYYY')),
    CASE WHEN p.predicted_corrected > 2 * COALESCE(p_prev.predicted_corrected, p.predicted_corrected)
         THEN 'High' ELSE 'Medium' END,
    p.cluster_id,
    p_run_id,
    '/forecast/skill-distribution'
  FROM dfc.ml_forecast_prediction p
  JOIN dfc.ml_forecast_prediction p_prev
    ON  p_prev.run_id     = p.run_id
    AND p_prev.cluster_id = p.cluster_id
    AND p_prev.months_ahead = p.months_ahead - 1
  JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = p.cluster_id
  WHERE p.run_id = p_run_id
    AND p.months_ahead > 0
    AND p.predicted_corrected > 1.5 * COALESCE(p_prev.predicted_corrected, 0)
    AND p.predicted_corrected > 50  -- Only alert on meaningful volumes
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS v_count = ROW_COUNT;

  -- Model Accuracy Drop alert
  INSERT INTO dfc.app_alert (alert_type, description, severity, cluster_id, run_id, view_link)
  SELECT
    'Model Accuracy Drop',
    format('Cluster %s accuracy %.1f%% is below threshold (80%%) for %s',
           sc.cluster_name, p.accuracy_pct,
           to_char(p.forecast_month, 'Mon YYYY')),
    'High',
    p.cluster_id,
    p_run_id,
    '/forecast/overview'
  FROM dfc.ml_forecast_prediction p
  JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = p.cluster_id
  WHERE p.run_id = p_run_id
    AND p.accuracy_pct < 80
    AND p.actual_count IS NOT NULL
  ON CONFLICT DO NOTHING;

  -- Short Fuse Surge alert
  INSERT INTO dfc.app_alert (alert_type, description, severity, cluster_id, run_id, view_link)
  SELECT
    'Short Fuse Surge',
    format('Cluster %s: %s confirmed SOs (SSD floor) in %s exceed prediction of %s FTE',
           sc.cluster_name, p.ssd_floor, to_char(p.forecast_month, 'Mon YYYY'), p.predicted_count),
    'High',
    p.cluster_id,
    p_run_id,
    '/forecast/skill-distribution'
  FROM dfc.ml_forecast_prediction p
  JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = p.cluster_id
  WHERE p.run_id = p_run_id
    AND p.correction_applied = TRUE
    AND p.ssd_floor > COALESCE(p.predicted_count, 0) * 1.2  -- 20%+ overshoot
  ON CONFLICT DO NOTHING;

  RETURN v_count;
END;
$$;

-- =============================================================================
-- 12. fn_get_forecast_overview
-- Purpose : Returns all data needed for GET /api/v1/forecast/overview endpoint.
--           Aggregates across the most recent ML model run.
-- =============================================================================
CREATE OR REPLACE FUNCTION dfc.fn_get_forecast_overview(
  p_practice_area TEXT DEFAULT NULL,
  p_bu            TEXT DEFAULT NULL,
  p_location      TEXT DEFAULT NULL,
  p_grade         TEXT DEFAULT NULL,
  p_skill_cluster TEXT DEFAULT NULL,
  p_run_id        UUID DEFAULT NULL  -- NULL = use most recent run
)
RETURNS TABLE (
  -- KPIs
  total_forecast_fte    BIGINT,
  avg_cancellation_pct  NUMERIC,
  -- Trend row
  trend_month           TEXT,
  fte_demand            BIGINT,
  growth_rate_pct       NUMERIC,
  -- Prediction row
  cluster_name          TEXT,
  months_ahead          SMALLINT,
  forecast_month        DATE,
  actual_count          INTEGER,
  predicted_count       INTEGER,
  predicted_corrected   INTEGER,
  accuracy_pct          NUMERIC,
  model_name            TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_run_id UUID;
BEGIN
  -- Resolve the latest run if not specified
  IF p_run_id IS NULL THEN
    SELECT r.run_id INTO v_run_id
    FROM dfc.ml_model_run r
    JOIN dfc.dim_practice_area pa ON pa.pa_id = r.pa_id
    WHERE (p_practice_area IS NULL OR upper(pa.pa_code) = upper(p_practice_area)
                                   OR upper(pa.pa_name) = upper(p_practice_area))
    ORDER BY r.created_at DESC
    LIMIT 1;
  ELSE
    v_run_id := p_run_id;
  END IF;

  RETURN QUERY
  SELECT
    SUM(dm.demand_fte)::BIGINT,
    ROUND(AVG(dm.cancellation_pct), 2),
    to_char(make_date(dm.demand_year, dm.demand_month, 1), 'Mon') AS trend_month,
    SUM(dm.demand_fte)::BIGINT,
    0::NUMERIC,  -- growth_rate_pct: computed by API layer from prior year comparison
    sc.cluster_name,
    p.months_ahead,
    p.forecast_month,
    p.actual_count,
    p.predicted_count,
    p.predicted_corrected,
    p.accuracy_pct,
    r.model_name
  FROM dfc.fact_demand_monthly dm
  JOIN dfc.dim_practice_area  pa ON pa.pa_id  = dm.pa_id
  JOIN dfc.dim_market         mk ON mk.market_id = dm.market_id
  JOIN dfc.dim_business_unit  bu ON bu.bu_id  = dm.bu_id
  JOIN dfc.dim_so_grade       g  ON g.grade_id = dm.grade_id
  LEFT JOIN dfc.dim_skill_cluster sc ON sc.cluster_id = dm.cluster_id
  LEFT JOIN dfc.ml_forecast_prediction p
    ON  p.run_id     = v_run_id
    AND p.cluster_id = dm.cluster_id
  LEFT JOIN dfc.ml_model_run r ON r.run_id = p.run_id
  WHERE
    (p_practice_area IS NULL OR upper(pa.pa_code) = upper(p_practice_area)
                             OR upper(pa.pa_name) = upper(p_practice_area))
    AND (p_bu          IS NULL OR upper(bu.bu_name)     = upper(p_bu))
    AND (p_location    IS NULL OR upper(mk.market_code) = upper(p_location))
    AND (p_grade       IS NULL OR upper(g.grade_code)   = upper(p_grade))
    AND (p_skill_cluster IS NULL OR upper(sc.cluster_name) = upper(p_skill_cluster))
  GROUP BY
    dm.demand_year, dm.demand_month,
    sc.cluster_name, p.months_ahead, p.forecast_month,
    p.actual_count, p.predicted_count, p.predicted_corrected, p.accuracy_pct,
    r.model_name;
END;
$$;
