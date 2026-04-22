-- =============================================================================
-- DEMAND FORECAST PLANNER — Source-to-Target Field Mapping
-- =============================================================================
--
-- LEFT-HAND SIDE  = Source field names exactly as they appear in upstream
--                   system CSVs / Data Mart extract (Cognizant EDS workflow)
--
-- RIGHT-HAND SIDE = Target field names in PostgreSQL (dfc schema) and how
--                   they surface in the ML pipeline and frontend API
--
-- Source Systems feeding Data Mart:
--   [SS1] QuickSO / SO System     → SO header & line fields
--   [SS2] Resource / BU System    → BU hierarchy & SBU mapping
--   [SS3] Skills Database         → Technical Skills Required
--   [SS4] Project / Billing Sys   → Billability, Project Type
--
-- Pipeline transformations are documented inline.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Create a documentation table for the mapping (queryable reference)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dfc.field_mapping_catalog (
  mapping_id        SERIAL  PRIMARY KEY,
  source_system     TEXT    NOT NULL,  -- SS1 / SS2 / SS3 / SS4
  source_field_name TEXT    NOT NULL,  -- EXACT column name in source CSV/Data Mart
  source_data_type  TEXT,
  stg_column        TEXT,             -- Column in dfc.stg_raw_service_order
  fact_column       TEXT,             -- Column in dfc.fact_service_order (or dim table)
  api_field         TEXT,             -- JSON key in REST API response
  frontend_field    TEXT,             -- TypeScript interface field name
  transformation    TEXT,             -- Transformation / business rule applied
  drop_reason       TEXT,             -- NULL if field is kept; else reason for drop
  pipeline_step     TEXT,             -- Which script handles this field
  notes             TEXT
);

-- =============================================================================
-- KEPT FIELDS (DEFAULT_COLS_TO_KEEP in preprocess.py)
-- =============================================================================

INSERT INTO dfc.field_mapping_catalog
  (source_system, source_field_name, source_data_type,
   stg_column, fact_column, api_field, frontend_field,
   transformation, drop_reason, pipeline_step, notes)
VALUES

-- ─── [SS1] QuickSO / SO System ───────────────────────────────────────────────

('SS1', 'SO Line Status',           'TEXT',
 'so_line_status',         'so_line_status',           'so_line_status',      'so_line_status',
 'Kept as-is. Used in SSD guardrail --exclude-open filter. '
 'Values: OPEN, Cancelled, Fulfilled.',
 NULL, 'preprocess.py + ssd_guardrail.py',
 'Used in _RAW_STATUS_COL constant in ssd_guardrail.py'),

('SS1', 'Unique ID',                'TEXT',
 'unique_id',              'unique_id',                'unique_id',           NULL,
 'Deduplication key. Not exposed to frontend API.',
 NULL, 'preprocess.py',
 'SO line identifier (e.g. SO-XXXXX). PK for fact_service_order.'),

('SS1', 'Vertical',                 'TEXT',
 'vertical',               'vertical',                 NULL,                  NULL,
 'Kept for reference. Not used in ML grouping dimensions.',
 NULL, 'preprocess.py', NULL),

('SS1', 'Practice',                 'TEXT',
 'practice',               'practice',                 NULL,                  NULL,
 'Kept for reference. Different from Practice Area.',
 NULL, 'preprocess.py', NULL),

('SS1', 'SubVertical',              'TEXT',
 'sub_vertical',           'sub_vertical',             NULL,                  NULL,
 'Kept for reference.', NULL, 'preprocess.py', NULL),

('SS1', 'SubPractice',              'TEXT',
 'sub_practice',           'sub_practice',             NULL,                  NULL,
 'Kept for reference.', NULL, 'preprocess.py', NULL),

('SS2', 'BU',                       'TEXT',
 'bu',                     'bu_id → dim_business_unit.bu_name',
                                                        'business_unit',       'business_unit',
 'Americas: corrected via SBU-BU Mapping.xlsx (preprocess.py step 9). '
 'EMEA: Mobility dropped; Transport-UK → T&H-UK; RCGT&H-UK merge; '
 'South Europe → SPAI; Benelux split using SBU1 into Belux / Netherlands.',
 NULL, 'preprocess.py (steps 9, 11)',
 'Raw BU stored in stg; corrected BU FK in fact. Group dim in BS/BRLC modes.'),

('SS1', 'Parent Customer',          'TEXT',
 'parent_customer',        'parent_customer',           NULL,                  NULL,
 'Kept for reference. Stripped before ML training (data_split.py).',
 NULL, 'preprocess.py', NULL),

('SS4', 'Project Type',             'TEXT',
 'project_type',           'project_type',             NULL,                  NULL,
 'Filter: keep EXTN (Extension) and EXANT (External Anticipation) only. '
 'All other values dropped in preprocess.py step 3.',
 NULL, 'preprocess.py step 3',
 'DEFAULT_FILTERS["Project Type"] = ["EXTN", "EXANT"]'),

('SS4', 'Project Billability Type', 'TEXT',
 'project_billability_type', 'billability_type → dfc.billability_enum',
                                                        'billability_type',    'billability_type',
 'Kept values: BFD, BTB, BTM. Others dropped. '
 'BFD=Billable For Development, BTB=Billable To Business, BTM=Billable To Margins. '
 'Drives trend_billability in demand-type API response.',
 NULL, 'preprocess.py step 3',
 'DEFAULT_FILTERS["Project Billability Type"] = ["BFD","BTB","BTM"]'),

('SS1', 'Quantity',                 'INTEGER',
 'quantity',               'quantity',                 'fte_demand / demand_fte', 'demand_fte',
 'One FTE per SO line. Aggregated as Σ Quantity for all demand counts. '
 'This is the target_count variable for ML training (build_training_groups.py).',
 NULL, 'preprocess.py → build_training_groups.py',
 'Central measure. target_count in training_dataset.parquet.'),

('SS1', 'SO Submission Date',       'DATE',
 'so_submission_date',     'so_submission_date',        NULL,                  NULL,
 'SSD: used to compute guardrail floor counts. '
 'Rows where RSD < SSD are dropped (preprocess.py step 7). '
 'ssd_guardrail.py uses this to count confirmed open SOs per group × month.',
 NULL, 'preprocess.py step 7; data_split.py _compute_ssd_floors; ssd_guardrail.py',
 '_RAW_SSD_COL in ssd_guardrail.py'),

('SS1', 'Cancellation Reason',      'TEXT',
 'cancellation_reason',    'cancellation_reason',       'avg_cancellation_pct', NULL,
 'Filter: keep specific values (NA, postponed, opportunity lost, etc.) defined '
 'in DEFAULT_FILTERS["Cancellation Reason"]. Others dropped. '
 'is_cancelled computed col = (reason <> "NA" AND reason IS NOT NULL). '
 'avg_cancellation_pct KPI = % cancelled SOs.',
 NULL, 'preprocess.py step 3',
 '10 kept cancellation reason values in DEFAULT_FILTERS.'),

('SS1', 'Off/ On',                  'TEXT',
 'off_on',                 'delivery_mode → dfc.onsite_offshore_enum',
                                                        'onsite_offshore',     'delivery_mode',
 'Values: "Off" → Offshore, "On" → Onsite. '
 'Drives geographic onsite/offshore KPIs and grid_geographic view.',
 NULL, 'preprocess.py',
 'Note: source has trailing space in column name "Off/ On"'),

('SS1', 'Geography',                'TEXT',
 'geography',              'vertical',                  NULL,                  NULL,
 'Kept for reference.', NULL, 'preprocess.py', NULL),

('SS1', 'Country',                  'TEXT',
 'country',                'country_id → dim_country.country_name',
                                                        'location',            'country',
 'FK to dim_country. Group dimension in RLC and BRLC grouping modes. '
 'Top countries KPI: US ~38%, India ~30%, UK ~12%, Philippines ~10%, Poland ~10%.',
 NULL, 'preprocess.py → build_training_groups.py',
 '_RAW_CTRY_COL in ssd_guardrail.py; "location" filter param in API'),

('SS1', 'City',                     'TEXT',
 'city',                   'city',                      NULL,                  NULL,
 'Kept for reference. Not used in ML grouping.', NULL, 'preprocess.py', NULL),

('SS1', 'Fulfilment/Cancellation Month', 'TEXT',
 'fulfilment_cancellation_month', 'fulfilment_cancellation_month', NULL,    NULL,
 'Reference field. Not used in ML pipeline.', NULL, 'preprocess.py', NULL),

('SS1', 'Requirement Start Date',   'DATE',
 'requirement_start_date', 'requirement_start_date',   'forecast_month',      'forecast_month',
 'RSD: KEY DATE for demand forecasting. '
 'Defines which month the resource need falls in. '
 'Parsed via cascading strptime (ISO/slash/dash formats). '
 'Filter: year must be in 2023-2025 (preprocess.py step 6). '
 'Filter: RSD >= SSD (step 7). '
 'Split point: <= 2025-06-30 = train; > 2025-06-30 = test.',
 NULL, 'preprocess.py steps 5-7; data_split.py',
 '_RAW_RSD_COL in ssd_guardrail.py. MIN_YEAR=2024 in skill_normalized.py.'),

('SS1', 'Market',                   'TEXT',
 'market',                 'market_id → dim_market.market_code',
                                                        'location',            'location',
 'Values after correction: Americas, EMEA. '
 'GGM rows: split using Market Unit → Americas or EMEA; APJ rows dropped. '
 'Filter: DEFAULT_FILTERS["Market"] = ["Americas","EMEA","GGM"].',
 NULL, 'preprocess.py steps 3, 10',
 'GGM = Global Growth Markets; split using Market Unit col.'),

('SS1', 'SO TYPE',                  'TEXT',
 'so_type',                'so_type',                   NULL,                  NULL,
 'Filter: keep "STA" only. All other SO types dropped.',
 NULL, 'preprocess.py step 3',
 'DEFAULT_FILTERS["SO TYPE"] = ["STA"]'),

('SS1', 'SO GRADE',                 'TEXT',
 'so_grade',               'grade_id → dim_so_grade.grade_code',
                                                        'so_grade',            'grade',
 'Normalized grades: PT/PAT/PA/P → GenC (combined). '
 'Removed: cont, D, SR. DIR., VP, AVP, Admin Staff, TD Trainee. '
 'Final values: SA | A | M | GenC | SM | AD. '
 'Group dimension in RLC and BRLC modes.',
 NULL, 'preprocess.py step 4',
 '_RAW_GRADE_COL in ssd_guardrail.py. DEFAULT_GRADES_TO_COMBINE + REMOVE.'),

('SS3', 'Technical Skills Required','TEXT',
 'technical_skills_raw',  'technical_skills_raw (STG) → skills_normalized (FACT)',
                                                        NULL,                  NULL,
 'Multi-skill delimited string (commas, semicolons, colons, parentheses). '
 'Parsed by skill_normalized.py: split → normalize via skill_normalization_llm2.json '
 '→ Skills Normalized column. Unmapped skills → Not found column. '
 'High-demand subset → Skill Groups. Used by apply_clusters.py for Jaccard assignment.',
 NULL, 'skill_normalized.py',
 'TECHNICAL_SKILLS_COL constant. Source for all skill taxonomy.'),

('SS3', 'Requirement type',         'TEXT',
 'requirement_type',       'requirement_type → dfc.demand_type_enum',
                                                        'demand_type',         'requirement_type',
 'Values: "New Demand" | "Backfill". '
 'Drives new_vs_backfill KPI and trend_new_vs_backfill chart.',
 NULL, 'preprocess.py',
 'Kept as-is. Enum in DB.'),

('SS1', 'Practice Area',            'TEXT',
 'practice_area',          'pa_id → dim_practice_area.pa_code',
                                                        'practice_area',       'practice_area',
 'Filter: DEFAULT_FILTERS["Practice Area"] (e.g. "Digital Engineering"). '
 'Abbreviated to PA code (DE, ADM, EPS) via pa_abbrev() function. '
 'Used in all grouping dimensions and skill cluster assignment.',
 NULL, 'preprocess.py step 3',
 'PA_ABBREV_OVERRIDES dict handles abbreviation collisions.'),

('SS1', 'ServiceLine',              'TEXT',
 'service_line',           'service_line',             NULL,                  NULL,
 'Kept for reference. Used in top_clusters_in_sl KPI.',
 NULL, 'preprocess.py', NULL),

('SS1', 'Original Requirement Start date', 'DATE',
 'original_rsd',           'original_rsd',              NULL,                  NULL,
 'Kept in STG for auditing. Stripped before ML training (data_split.py leakage prevention).',
 NULL, 'data_split.py',
 'Listed in DEFAULT_COLS_TO_REMOVE for ML training; kept in DB for reference.'),

('SS1', 'Revenue potential',        'NUMERIC',
 'revenue_potential',      'revenue_potential',          NULL,                  NULL,
 'Kept in STG and fact for reference. Stripped before ML training (data_split.py). '
 'Not a forecasting feature (not available at inference time).',
 NULL, 'data_split.py leakage prevention', NULL),

('SS2', 'SBU1',                     'TEXT',
 'sbu1',                   'sbu1',                      NULL,                  NULL,
 'Strategic BU1. Used in EMEA Benelux correction: '
 'SBU1 value determines whether a Benelux row → Belux or Netherlands BU.',
 NULL, 'preprocess.py step 11', NULL),

('SS1', 'Account ID',               'TEXT',
 'account_id',             'account_id',                NULL,                  NULL,
 'Kept for reference. Stripped before ML training.',
 NULL, 'data_split.py', NULL),

('SS1', 'Account Name',             'TEXT',
 'account_name',           'account_name',              NULL,                  NULL,
 'Kept in fact for reference. Stripped before ML training (data_split.py leakage). '
 'Not available at real inference time.',
 NULL, 'data_split.py', NULL),

('SS1', 'Parent Customer ID',       'TEXT',
 'parent_customer_id',     'parent_customer_id',         NULL,                  NULL,
 'Kept for reference.', NULL, 'preprocess.py', NULL),

('SS2', 'Market Unit',              'TEXT',
 'market_unit',            NULL,                        NULL,                  NULL,
 'Used to split GGM market rows into Americas or EMEA (preprocess.py step 10). '
 'Dropped from fact after market correction is applied.',
 NULL, 'preprocess.py step 10',
 'Not stored in fact_service_order; consumed at STG→FACT transform.');

-- =============================================================================
-- DROPPED FIELDS (DEFAULT_COLS_TO_REMOVE in preprocess.py)
-- These are dropped before any ML processing. Stored in STG for selected fields.
-- =============================================================================

INSERT INTO dfc.field_mapping_catalog
  (source_system, source_field_name, stg_column, drop_reason, pipeline_step)
VALUES
('SS2', 'Department',                     NULL, 'Superseded by BU column.',                         'preprocess.py'),
('SS2', 'BusinessUnit Desc',              NULL, 'Superseded by BU column.',                         'preprocess.py'),
('SS2', 'SBU2',                           NULL, 'Not needed; SBU1 used for Benelux split.',          'preprocess.py'),
('SS1', 'Project ID',                     NULL, 'Not needed for demand forecasting.',                'preprocess.py'),
('SS1', 'Project Name',                   NULL, 'Not needed; Account Name used.',                    'preprocess.py'),
('SS1', 'Action Date',                    NULL, 'Not a forecasting signal.',                         'preprocess.py'),
('SS1', 'SO Submission Date 2',           NULL, 'Duplicate of SO Submission Date.',                  'preprocess.py'),
('SS1', 'Offer Created Date',             NULL, 'Staffing process date; not demand signal.',         'preprocess.py'),
('SS1', 'Offer Extended Date',            NULL, 'Staffing process date.',                            'preprocess.py'),
('SS1', 'Available positions in RR',      NULL, 'Operational field; not forecasting input.',         'preprocess.py'),
('SS1', 'Offer Status',                   NULL, 'Post-demand workflow field.',                       'preprocess.py'),
('SS1', 'Offer Sub Status',               NULL, 'Post-demand workflow field.',                       'preprocess.py'),
('SS1', 'No Of Offers',                   NULL, 'Post-demand workflow field.',                       'preprocess.py'),
('SS1', 'Job Opening Status',             NULL, 'Post-demand workflow field.',                       'preprocess.py'),
('SS1', 'Recruiter ID',                   'recruiter_id', 'Operational; kept in STG only.',         'preprocess.py'),
('SS1', 'Recruiter Name',                 'recruiter_name','Operational; kept in STG only.',        'preprocess.py'),
('SS1', 'Subcontractor Allowed by Customer', NULL, 'Policy field; not forecasting input.',         'preprocess.py'),
('SS1', 'Interview Required by Customer', NULL, 'Policy field.',                                   'preprocess.py'),
('SS4', 'T&MRateCard',                    NULL, 'Financial; not demand signal.',                   'preprocess.py'),
('SS1', 'Assignment Start Date',          'assignment_start_date', 'Post-fulfillment date.',       'preprocess.py'),
('SS1', 'Job Code',                       'job_code', 'Kept in STG only.',                         'preprocess.py'),
('SS1', 'Preferred Location 1',           NULL, 'Preference data; Country used instead.',          'preprocess.py'),
('SS1', 'Preferred Location 2',           NULL, 'Preference data; Country used instead.',          'preprocess.py'),
('SS1', 'Requirement End Date',           'requirement_end_date', 'Duration context only.',        'preprocess.py'),
('SS4', 'Additional Revenue',             NULL, 'Financial field.',                                'preprocess.py'),
('SS4', 'Billability Start date',         'billability_start_date', 'Post-fulfillment.',           'preprocess.py'),
('SS1', 'INTERNAL FULFILMENT-TAT',        NULL, 'TAT metric; staffing efficiency, not demand.',    'preprocess.py'),
('SS1', 'EXTERNAL FULFILMENT- WFM -TAT', NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'EXTERNAL FULFILMENT- TAG -TAT', NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'TAT(Flag dt to Interview dt)',   NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'TAT(Int to Offer creation)',     NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'TAT(Offer create to Offer approve)', NULL, 'TAT metric.',                               'preprocess.py'),
('SS1', 'TAT(Offer Apprvd to Offer Extnd)', NULL, 'TAT metric.',                                 'preprocess.py'),
('SS1', 'TAT(Offer extnd -EDOJ)',         NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'TAT(Exp DOJ- DOJ)',              NULL, 'TAT metric.',                                    'preprocess.py'),
('SS1', 'Source category',                NULL, 'Sourcing channel; not demand signal.',           'preprocess.py'),
('SS1', 'Cancellation Ageing',            'cancellation_ageing', 'Kept in STG for auditing.',    'preprocess.py'),
('SS1', 'Open SO Ageing',                 'open_so_ageing', 'Kept in STG; used for alerts.',     'preprocess.py'),
('SS1', 'RR Ageing',                      NULL, 'Recruitment requisition age.',                  'preprocess.py'),
('SS1', 'Open SO Ageing range',           NULL, 'Bucketed version of Open SO Ageing.',           'preprocess.py'),
('SS1', 'RR Ageing range',                NULL, 'Bucketed version.',                             'preprocess.py'),
('SS1', 'CCA Service Line',               NULL, 'Alternative SL taxonomy; ServiceLine used.',    'preprocess.py'),
('SS1', 'CCA Service Line Description',   NULL, 'Same.',                                         'preprocess.py'),
('SS1', 'Track',                          NULL, 'Internal classification.',                      'preprocess.py'),
('SS1', 'Track Description',              NULL, 'Internal classification.',                      'preprocess.py'),
('SS1', 'Sub Track',                      NULL, 'Internal classification.',                      'preprocess.py'),
('SS1', 'Sub Track Description',          NULL, 'Internal classification.',                      'preprocess.py'),
('SS1', 'Demand Role Code',               NULL, 'Role code; SO GRADE used for grading.',         'preprocess.py'),
('SS1', 'Demand Role Description',        NULL, 'Role label; grade_label used instead.',         'preprocess.py'),
('SS1', 'Leadership and Prof. Dev. Comp', NULL, 'Competency tag; not a forecasting feature.',    'preprocess.py'),
('SS3', 'Additional Skills',              NULL, 'Supplementary skills; Technical Skills used.',  'preprocess.py'),
('SS3', 'Skill Family',                   'skill_family', 'Kept in STG. Use dim_skill instead.','preprocess.py'),
('SS1', 'RLC',                            NULL, 'Redundant; Country+Grade+Cluster used.',        'preprocess.py'),
('SS1', 'RSC1',                           NULL, 'Redundant composite.',                          'preprocess.py'),
('SS3', 'Domain Skill Layer 1',           'domain_skill_layer_1', 'Kept in STG only.',          'preprocess.py'),
('SS3', 'Domain Skill Layer 2',           'domain_skill_layer_2', 'Kept in STG only.',          'preprocess.py'),
('SS3', 'Domain Skill Layer 3',           'domain_skill_layer_3', 'Kept in STG only.',          'preprocess.py'),
('SS1', 'Revenue Loss Category',          NULL, 'Post-event analysis field.',                    'preprocess.py'),
('SS1', 'Staffing Team Member',           NULL, 'Operational; not demand signal.',               'preprocess.py'),
('SS1', 'Staffing Team Lead',             NULL, 'Operational; not demand signal.',               'preprocess.py'),
('SS1', 'SoStatus',                       NULL, 'Redundant with SO Line Status.',                'preprocess.py'),
('SS1', 'TMP SO Status',                  NULL, 'TMP-specific status; SO Line Status used.',     'preprocess.py'),
('SS1', 'Probable Fullfilment Date',      NULL, 'Estimated fulfillment; not demand date.',       'preprocess.py'),
('SS1', 'Open Trained Associate',         NULL, 'Supply-side field; not demand input.',          'preprocess.py'),
('SS3', 'Primary Skill Set',              NULL, 'Superseded by Technical Skills Required.',      'preprocess.py'),
('SS1', 'Expected Date Of Joining',       NULL, 'Post-fulfillment field.',                       'preprocess.py'),
('SS1', 'Replaced Associate',             NULL, 'Backfill context; Requirement type used.',      'preprocess.py'),
('SS4', 'Customer Bill rate',             NULL, 'Financial; not demand signal.',                 'preprocess.py'),
('SS4', 'Bill rate currency',             NULL, 'Financial.',                                    'preprocess.py'),
('SS4', 'Customer Profitability',         NULL, 'Financial.',                                    'preprocess.py'),
('SS1', 'OE Approval flag',               NULL, 'Approvals workflow; not forecasting input.',    'preprocess.py'),
('SS1', 'OE Approver Date',               NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'OE Approval Comments',           NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'TSC Approval flag',              NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'TSC Approver ID',                NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'TSC Approver Date',              NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'TSC Approval Comments',          NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'Customer Project',               NULL, 'Project reference; not demand signal.',         'preprocess.py'),
('SS1', 'Primary State tag',              NULL, 'UI tag; not analytical.',                       'preprocess.py'),
('SS1', 'Secondary State tag',            NULL, 'UI tag.',                                       'preprocess.py'),
('SS1', 'status_remark',                  NULL, 'Free-text status; not structured.',             'preprocess.py'),
('SS1', 'Opportunity Status',             NULL, 'Sales pipeline field.',                         'preprocess.py'),
('SS1', 'Job Description',                NULL, 'Free text; skill extraction done separately.',  'preprocess.py'),
('SS4', 'Revenue',                        NULL, 'Financial; stripped at data_split.py.',         'data_split.py'),
('SS1', 'greenchannel',                   NULL, 'Internal flag.',                                'preprocess.py'),
('SS1', 'Forecast Category',              NULL, 'Redundant with ML output.',                     'preprocess.py'),
('SS4', 'Win Probability',                NULL, 'Sales field; not available at inference time.', 'preprocess.py'),
('SS4', 'Estimated Deal close date',      NULL, 'Sales field.',                                  'preprocess.py'),
('SS4', 'Actual Expected Revenue Start date', NULL, 'Financial projection.',                    'preprocess.py'),
('SS1', 'Opportunity Owner',              NULL, 'Sales field.',                                  'preprocess.py'),
('SS1', 'OwnerID',                        NULL, 'Sales field.',                                  'preprocess.py'),
('SS1', 'SO Priority',                    'so_priority', 'Kept in STG only.',                   'preprocess.py'),
('SS1', 'MU Priority',                    NULL, 'Market Unit priority; not a ML feature.',       'preprocess.py'),
('SS1', 'iRise Status',                   NULL, 'Talent platform status.',                       'preprocess.py'),
('SS1', 'PE Flagged',                     'pe_flagged', 'Kept in STG only.',                    'preprocess.py'),
('SS1', 'IJM Allocation',                 NULL, 'Internal job movement; supply side.',           'preprocess.py'),
('SS1', 'Original TAT',                   NULL, 'TAT metric.',                                   'preprocess.py'),
('SS1', 'Approver ID',                    NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'Approver Name',                  NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'Delivery/Non-Delivery',          NULL, 'Billing classification; Billability Type used.','preprocess.py'),
('SS1', 'Project Classification',         NULL, 'Internal project tag.',                         'preprocess.py'),
('SS1', 'Service Description',            NULL, 'Free text.',                                    'preprocess.py'),
('SS1', 'Cluster Description',            NULL, 'Old skill cluster taxonomy; replaced.',         'preprocess.py'),
('SS1', 'Demand Unit Description',        NULL, 'Free text.',                                    'preprocess.py'),
('SS1', 'Is this demand open for all Cognizant locations across India?', NULL,
 'Boolean flag; Country field used.',                                                             'preprocess.py'),
('SS3', 'Skills(Anchor/Supplementary)',   NULL, 'Superseded by Technical Skills Required.',      'preprocess.py'),
('SS1', 'Assignment Staging Date',        NULL, 'Post-fulfillment date.',                        'preprocess.py'),
('SS1', 'SO Work Model',                  NULL, 'Redundant with Off/ On field.',                 'preprocess.py'),
('SS1', 'State',                          NULL, 'US state; City + Country used.',                'preprocess.py'),
('SS1', 'Order Description',              NULL, 'Free text.',                                    'preprocess.py'),
('SS1', 'Data/Voice',                     NULL, 'Telecom-specific tag.',                         'preprocess.py'),
('SS1', 'Active RR Status',               NULL, 'Recruitment requisition; supply side.',         'preprocess.py'),
('SS4', 'SO Billability',                 NULL, 'Redundant with Project Billability Type.',       'preprocess.py'),
('SS1', 'Cancelled BY ID',                NULL, 'Approvals workflow.',                           'preprocess.py'),
('SS1', 'cancellation_comments',          NULL, 'Free text; Cancellation Reason used.',          'preprocess.py'),
('SS2', 'Owning Organization',            NULL, 'Org hierarchy; BU used.',                       'preprocess.py'),
('SS1', 'Pool ID',                        NULL, 'Supply-side talent pool.',                      'preprocess.py'),
('SS1', 'Pool Name',                      NULL, 'Supply-side talent pool.',                      'preprocess.py'),
('SS1', 'Associate Hired Grade',          NULL, 'Post-fulfillment; SO GRADE used for demand.',   'preprocess.py'),
('SS1', 'Flagged for Recruitment',        NULL, 'Recruitment flag; not demand signal.',          'preprocess.py'),
('SS1', 'When Flagged for Recruitment',   NULL, 'Recruitment flag date.',                        'preprocess.py'),
('SS3', 'Technical Skills Desired',       NULL, 'Desired vs required; Technical Skills Required used.', 'preprocess.py'),
('SS3', 'Functional Skills',              NULL, 'Functional domain; Technical Skills Required used.', 'preprocess.py'),
('SS1', 'Original Requirement Start date', NULL,
 'Listed in DEFAULT_COLS_TO_REMOVE for ML. Kept as original_rsd in fact for auditing.',          'data_split.py'),
('SS1', 'Deflag MFR',                     NULL, 'Internal flag.',                                'preprocess.py'),
('SS1', 'Deflag MFR Date',                NULL, 'Internal flag date.',                           'preprocess.py');

-- =============================================================================
-- ML PIPELINE DERIVED / OUTPUT FIELDS
-- Fields produced BY the pipeline (not from source systems)
-- =============================================================================

INSERT INTO dfc.field_mapping_catalog
  (source_system, source_field_name, stg_column, fact_column, api_field, frontend_field,
   transformation, drop_reason, pipeline_step, notes)
VALUES

-- skill_normalized.py outputs
('ML_PIPELINE', 'Skills Normalized', NULL,
 'skills_normalized', NULL, NULL,
 'Produced by skill_normalized.py. Technical Skills Required split into leaf skills, '
 'each mapped via skill_normalization_llm2.json. Deduped and joined as comma-space string.',
 NULL, 'skill_normalized.py', 'NORMALIZED_COL constant'),

('ML_PIPELINE', 'Not found', NULL,
 'not_found_skills', NULL, NULL,
 'Leaf skills in Technical Skills Required that had no match in skill_normalization_llm2.json.',
 NULL, 'skill_normalized.py', 'NOT_FOUND_COL constant'),

('ML_PIPELINE', 'Skill Groups', NULL,
 'skill_groups', NULL, NULL,
 'High-demand, non-low-demand subset of Skills Normalized. '
 'Filters: total demand ≥ MIN_TOTAL_DEMAND_FOR_GROUPS (900 Americas, 250 EMEA), '
 'CV_2025 < MAX_CV_FOR_GROUPS (0.9), demand_2025 ≥ 50 and ≥ 20% of total. '
 'Rows with empty Skill Groups are dropped before clustering.',
 NULL, 'skill_normalized.py', NULL),

-- apply_clusters.py output
('ML_PIPELINE', 'Skill Cluster', NULL,
 'cluster_id → dim_skill_cluster.cluster_name',
 'cluster / skill_cluster', 'cluster',
 'Assigned by apply_clusters.py via Jaccard similarity between row Skill Groups '
 'and cluster skill sets. '
 'Pass 1: Priority skills (COBOL, ServiceNow, SAP, Pega, etc.) → force-assign. '
 'Pass 2: Jaccard ≥ threshold (0.3-0.4 by union size). '
 'Pass 3: Jaccard on primary skills only for unmapped rows. '
 'Group dimension in all ML grouping modes (S, RLC, BS, BRLC).',
 NULL, 'apply_clusters.py', '_RAW_SC_COL in ssd_guardrail.py'),

-- build_training_groups.py feature outputs
('ML_PIPELINE', 'target_count', NULL,
 NULL, 'demand_fte', 'demand_fte',
 'Σ Quantity grouped by (group_by_cols, cutoff_date, months_ahead). '
 'This is the ML training label — what the model learns to predict.',
 NULL, 'build_training_groups.py', 'target variable in training_dataset.parquet'),

('ML_PIPELINE', 'lag_30d_count', NULL,
 'lag_30d_count (ml_forecast_prediction)', NULL, NULL,
 'Count of SO events in the 30 days before the cutoff date for this group.',
 NULL, 'build_training_groups.py', 'Lag feature family'),

('ML_PIPELINE', 'sma_3m / sma_6m / sma_12m', NULL,
 'sma_3m / sma_6m / sma_12m', NULL, NULL,
 'Simple moving averages of monthly demand over 3, 6, 12 months.',
 NULL, 'build_training_groups.py', 'Baseline feature family'),

('ML_PIPELINE', 'ema_3m / ema_6m / ema_12m', NULL,
 'ema_3m / ema_6m / ema_12m', NULL, NULL,
 'Exponentially weighted moving averages with respective spans.',
 NULL, 'build_training_groups.py', 'Baseline feature family'),

('ML_PIPELINE', 'growth_3m_yoy / growth_6m_yoy / growth_9m_yoy', NULL,
 'growth_3m_yoy / growth_6m_yoy / growth_9m_yoy', NULL, NULL,
 'Year-over-year % growth for 3, 6, 9 month windows. Capped at ±300%.',
 NULL, 'build_training_groups.py', 'Growth feature family'),

('ML_PIPELINE', 'trend_slope', NULL,
 'trend_slope', NULL, NULL,
 'OLS slope over last 6 complete months, normalised by mean demand.',
 NULL, 'build_training_groups.py', 'Trend feature'),

('ML_PIPELINE', 'trajectory_class', NULL,
 'trajectory → dfc.trajectory_enum', NULL, NULL,
 'CAGR-based classification: Fast Growing (CAGR>20%), Growing (5-20%), '
 'Stable (−5% to 5%), Declining (−5% to −20%), Fast Declining (<−20%).',
 NULL, 'build_training_groups.py', 'Growth trajectory feature'),

-- train_and_predict.py outputs (All_Predictions sheet)
('ML_PIPELINE', 'M0_actual through M5_actual', NULL,
 NULL, 'actual (months[].actual)', 'actual_count',
 'Actual demand count for months M0-M5 (from test set ground truth). '
 'NULL for future months not yet observed.',
 NULL, 'train_and_predict.py', 'All_Predictions sheet columns'),

('ML_PIPELINE', 'M0_predicted through M5_predicted', NULL,
 NULL, 'predicted (months[].predicted)', 'predicted_count',
 'AutoGluon ensemble prediction for months M0-M5.',
 NULL, 'train_and_predict.py', 'All_Predictions sheet columns'),

('ML_PIPELINE', 'accuracy_pct', NULL,
 NULL, 'accuracy_pct', 'accuracy_pct',
 'MAPE-based accuracy = 100 - MAPE(%). Stored per group and aggregated.',
 NULL, 'train_and_predict.py', 'Group_Metrics sheet'),

('ML_PIPELINE', 'Model_Name', NULL,
 NULL, 'model_name', 'model_name',
 'AutoGluon model identifier, e.g. "Gluon::LightGBM_BAG_L1_FULL", '
 '"Gluon::NeuralNetTorch_BAG_L1", "Gluon::CatBoost_BAG_L1".',
 NULL, 'train_and_predict.py', NULL),

('ML_PIPELINE', 'Prediction_Source', NULL,
 NULL, 'prediction_source → dfc.prediction_source_enum', 'prediction_source',
 'AutoML | Override | Guardrail_Corrected.',
 NULL, 'ssd_guardrail.py', NULL),

-- ssd_guardrail.py additions
('ML_PIPELINE', 'SSD_Floor', NULL,
 NULL, 'ssd_floor', 'ssd_floor',
 'Count of already-confirmed SOs (SO Submission Date < train_cutoff) '
 'with Requirement Start Date in the forecast window. '
 'Minimum floor for predictions: predicted_corrected = max(predicted, ssd_floor).',
 NULL, 'ssd_guardrail.py', 'SSD_Floors_Detail sheet'),

('ML_PIPELINE', 'Predicted_Corrected', NULL,
 NULL, 'predicted_corrected', 'predicted_corrected',
 'max(M{n}_predicted, SSD_Floor). This is the value used in API responses '
 'as months[n].predicted_corrected and as system_forecast in FeedbackItem.',
 NULL, 'ssd_guardrail.py', 'All_Predictions_SSD sheet'),

('ML_PIPELINE', 'Correction_Applied', NULL,
 NULL, 'correction_applied', 'correction_applied',
 'TRUE when predicted_corrected > M{n}_predicted (guardrail kicked in).',
 NULL, 'ssd_guardrail.py', NULL),

('ML_PIPELINE', 'Accuracy_Corrected_Pct', NULL,
 NULL, 'accuracy_corrected_pct', 'accuracy_corrected_pct',
 'MAPE-based accuracy recalculated after guardrail correction.',
 NULL, 'ssd_guardrail.py', 'SSD_Accuracy_Summary sheet');

-- Useful query to review the mapping
-- SELECT source_system, source_field_name, fact_column, api_field, frontend_field,
--        transformation
-- FROM dfc.field_mapping_catalog
-- WHERE drop_reason IS NULL
-- ORDER BY source_system, source_field_name;
