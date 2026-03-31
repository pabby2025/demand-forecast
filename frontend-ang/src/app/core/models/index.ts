// =============================================================================
// DEMAND FORECAST PLANNER — TypeScript Models
// Aligned to: OneC_4898_DemandForecasting-code-yaswanth ML pipeline outputs
//             + contracts/ml-api-contract.md
// =============================================================================

// ── Auth ──────────────────────────────────────────────────────────────────────

export interface User {
  email: string;
  role: 'SL_COO' | 'MARKET_COO' | 'CFT_PLANNER';
  name: string;
}

// ── Filters ───────────────────────────────────────────────────────────────────
// Maps to API query params across all /api/v1/forecast/* endpoints.
// practice_area → Practice Area (ADM | Digital Engineering | EPS)
// bu            → BU (Business Unit)
// location      → Market (Americas | EMEA)
// grade         → SO GRADE (SA | A | M | GenC | SM | AD)
// skill_cluster → Skill Cluster (MSC-... names)
// forecast_horizon → "M0"-"M5" | "W1"-"W5" | "Q01"-"Q04" | "All"

export interface FilterState {
  practice_area: string;
  bu: string;
  location: string;
  grade: string;
  skill_cluster: string;
  forecast_horizon: string;
}

// ── Shared dimension types ────────────────────────────────────────────────────

/** A single-month ML prediction row for a cluster/group (M0-M5). */
export interface MonthForecast {
  month_index: number;          // 0-5 (M0=current month, M5=5 months out)
  actual: number;               // Actual demand count (0 if future)
  predicted: number;            // AutoML raw prediction (target_count)
  predicted_corrected?: number; // After SSD guardrail: max(predicted, SSD_Floor)
  accuracy_pct?: number;        // MAPE-based accuracy %
}

/** A named category with a percentage share (used in top-N KPI chips). */
export interface NamedPct {
  name: string;
  pct: number;
}

// ── KPIs (Forecast Overview) ──────────────────────────────────────────────────
// Source: /api/v1/forecast/overview → kpis

export interface DemandGrowthRate {
  qoq: { current: number; last_year: number }; // Quarter-over-quarter %
  mom: { current: number; last_year: number }; // Month-over-month %
  wow: { current: number; last_year: number }; // Week-over-week %
}

export interface ForecastKPIs {
  total_forecast_fte: number;       // Σ Quantity across filtered open SOs
  demand_growth_rate: DemandGrowthRate;
  avg_cancellation_pct: number;     // % SOs with Cancellation Reason ≠ "NA"
  top_practice_areas: NamedPct[];   // Top 3 Practice Areas by demand share
}

// ── Forecast Overview ─────────────────────────────────────────────────────────
// Source: GET /api/v1/forecast/overview

export interface TrendMonthlyPoint {
  month: string;           // "Jan", "Feb", etc.
  fte_demand: number;      // Aggregated Quantity for that month (Requirement Start Date)
  growth_rate_pct: number; // YoY % vs same month prior year
}

export interface TrendWeeklyPoint {
  week: string;            // "W1" – "W5"
  fte_demand: number;
  growth_rate_pct: number;
}

export interface TrendQuarterlyPoint {
  quarter: string;          // "QTR 01" – "QTR 04"
  fte_demand: number;
  growth_rate_pct: number;
}

/** Row in the main forecast grid (all 7 tab views share this base). */
export interface ForecastGridRow {
  practice_area: string;  // Practice Area label
  location: string;       // Market (Americas | EMEA)
  cluster: string;        // Skill Cluster (MSC-... name)
  so_grade: string;       // SO GRADE (SA | A | M | GenC | SM | AD)
  // Monthly demand columns (Requirement Start Date binned to month)
  jan?: number; feb?: number; mar?: number; apr?: number;
  may?: number; jun?: number; jul?: number; aug?: number;
  sep?: number; oct?: number; nov?: number; dec?: number;
  // Quarterly rollups
  q01?: number; q02?: number; q03?: number; q04?: number;
  // Weekly (short-fuse view)
  w1?: number; w2?: number; w3?: number; w4?: number; w5?: number;
  // Extra per-view dimension fields (present depending on the tab)
  demand_type?: string;       // "New Demand" | "Backfill" (Requirement type)
  billability_type?: string;  // "BFD" | "BTB" | "BTM" (Project Billability Type)
  business_unit?: string;     // BU name
  onsite_offshore?: string;   // "Onsite" | "Offshore" (Off/ On field)
  leaf_skill?: string;        // Individual normalized skill name
  stability?: string;         // "Stable" | "Volatile" (XYZ segment: X=Stable)
  [key: string]: string | number | undefined;
}

/** Per-cluster ML forecast data (from train_and_predict.py output). */
export interface ClusterForecast {
  skill_cluster: string;    // Skill Cluster dimension (group_by col)
  months: MonthForecast[];  // M0-M5 predictions + actuals
  model_name: string;       // e.g. "Gluon::LightGBM_BAG_L1_FULL"
}

export interface ForecastOverviewData {
  kpis: ForecastKPIs;
  explainability: string[];           // NL explanations of top demand drivers
  trend_monthly: TrendMonthlyPoint[];
  trend_weekly: TrendWeeklyPoint[];
  trend_quarterly: TrendQuarterlyPoint[];
  grid: ForecastGridRow[];
  forecasts_by_cluster: ClusterForecast[]; // ML M0-M5 per Skill Cluster
}

// ── Demand Type Breakdown ─────────────────────────────────────────────────────
// Source: GET /api/v1/forecast/demand-type-breakdown
// Dimension: Requirement type → "New Demand" | "Backfill"
//            Project Billability Type → "BFD" | "BTB" | "BTM"

export interface DemandTypeKPIs {
  new_vs_backfill: { new_pct: number; backfill_pct: number };
  contract_type_mix: {
    t_and_m_pct: number;        // Time & Material (Project Type filtered)
    fixed_price_pct: number;
    transaction_based_pct: number;
  };
}

export interface DemandTypeData {
  kpis: DemandTypeKPIs;
  trend_new_vs_backfill: Array<{ month: string; new_demand: number; backfill: number }>;
  trend_billability: Array<{ month: string; bfd: number; btb: number; btm: number }>;
  grid_new_vs_backfill: ForecastGridRow[];  // has demand_type field
  grid_billability: ForecastGridRow[];      // has billability_type field
}

// ── Business Unit Performance ─────────────────────────────────────────────────
// Source: GET /api/v1/forecast/business-unit
// Dimension: BU (Business Unit field from source)

export interface BUKPIs {
  top_bus_by_demand: NamedPct[];
  top_bus_by_growth: Array<{ name: string; growth_pct: number; unit: string }>;
}

export interface BUMLForecast {
  bu: string;           // BU dimension (group_by col)
  skill_cluster: string;
  months: MonthForecast[];
  model_name: string;
}

export interface BUPerformanceData {
  kpis: BUKPIs;
  trend_bu_demand: Array<Record<string, string | number>>;  // { month, BU1: n, BU2: n }
  trend_bu_growth: Array<Record<string, string | number>>;  // { month, BU1: %, BU2: % }
  grid_bu_demand: ForecastGridRow[];   // has business_unit field
  grid_bu_growth: ForecastGridRow[];   // has business_unit field
  ml_forecasts: BUMLForecast[];        // BU x Skill Cluster M0-M5
}

// ── Geographic (Location Mix) ─────────────────────────────────────────────────
// Source: GET /api/v1/forecast/geographic
// Dimension: Country, Off/ On → "Onsite" | "Offshore"

export interface GeographicKPIs {
  onsite_offshore: { onsite_pct: number; offshore_pct: number };
  top_countries: Array<{ country: string; pct: number }>;
}

export interface RLCForecast {
  country: string;      // Country dimension
  so_grade: string;     // SO GRADE dimension
  skill_cluster: string;
  months: MonthForecast[];
}

export interface GeographicData {
  kpis: GeographicKPIs;
  trend_mix: Array<{ month: string; onsite_offsite: number; offshore_mix: number }>;
  countrywise_demand: Array<Record<string, string | number>>; // { country, jan: n, feb: n }
  grid: ForecastGridRow[];       // has onsite_offshore field
  rlc_forecasts: RLCForecast[];  // Country + SO GRADE + Skill Cluster M0-M5
}

// ── Skill Distribution ────────────────────────────────────────────────────────
// Source: GET /api/v1/forecast/skill-distribution
// Dimension: Skill Cluster (MSC-...), leaf skills

export interface SkillDistributionKPIs {
  top_clusters_in_practice: NamedPct[];
  top_clusters_in_sl: NamedPct[];       // Top clusters in Service Line
  stable_vs_volatile: { stable_pct: number; volatile_pct: number }; // XYZ: X=stable
  top_drivers: string[];                // NL skill demand driver text
}

export interface ShortFuseHeatmap {
  total: number;
  change_pct: number;
  clusters: Array<{ name: string; monthly: number[] }>;
  legend: { low: string; medium: string; high: string };
}

export interface SkillDistributionData {
  kpis: SkillDistributionKPIs;
  short_fuse_heatmap: ShortFuseHeatmap; // Demand due within 6 weeks
  stability_trend: Array<Record<string, string | number>>; // { month, stable, volatile }
  grid_cluster_demand: ForecastGridRow[];  // has leaf_skill field
  grid_stability: ForecastGridRow[];       // has stability field ("Stable" | "Volatile")
}

// ── Grade Distribution ────────────────────────────────────────────────────────
// Source: GET /api/v1/forecast/grade-distribution
// Dimension: SO GRADE (SA | A | M | GenC | SM | AD)

export interface GradeKPIs {
  top_grade: Array<{ grade: string; count: number; pct: number }>;
  shortfuse_6months: Array<{ grade: string; count: number }>; // Req start ≤ 6 weeks
}

export interface GradeData {
  grade: string;          // SO GRADE code
  label: string;          // Human label (Senior Associate, Analyst, etc.)
  count: number;          // Demand count
  pct: number;            // Share of total demand
  shortfuse: number;      // Count with RSD within 6 weeks
  shortfuse_pct: number;
  billable_pct: number;   // % with Project Billability Type in BFD/BTB/BTM
}

export interface GradeDistributionData {
  kpis: GradeKPIs;
  grade_data: GradeData[];
  donut: Array<{ name: string; value: number; pct: number }>;
  shortfuse_total: number;
  heatmap: Array<Record<string, string | number>>;  // grade x month heatmap
  grade_trend: Array<Record<string, string | number>>; // { month, SA, A, M, GenC, SM, AD }
  grid_monthly: ForecastGridRow[];    // standard grid with monthly demand by grade
  grid_short_fuse: ForecastGridRow[]; // short-fuse view (W1-W5 cols + total)
}

// ── Demand-Supply Gap ─────────────────────────────────────────────────────────
// Source: GET /api/v1/forecast/demand-supply-gap

export interface GapKPIs {
  fulfillment_gap_pct: number;        // (demand - supply) / demand * 100
  critical_skill_shortage: number;    // Count of clusters where gap > threshold
  fulfillment_time_days: number;      // Average days to fill an open SO
}

export interface GapRow {
  cluster: string;  // Skill Cluster
  so_grade: string;
  jan_demand?: number; jan_supply?: number;
  feb_demand?: number; feb_supply?: number;
  mar_demand?: number; mar_supply?: number;
  apr_demand?: number; apr_supply?: number;
  may_demand?: number; may_supply?: number;
  jun_demand?: number; jun_supply?: number;
  gap?: number;
  gap_pct?: number;
  status?: string;  // "Critical" | "Warning" | "OK"
}

export interface DemandSupplyGapData {
  kpis: GapKPIs;
  heatmap: ShortFuseHeatmap;
  demand_supply_trend: Array<{ month: string; demand_fte: number; supply_fte: number }>;
  grid_short_fuse: ForecastGridRow[];
  grid_gap: GapRow[];
}

// ── Task ──────────────────────────────────────────────────────────────────────
// Source: GET /api/tasks

export interface Task {
  task_id: string;
  task_type: string;     // "Update Skill Micro Cluster" | "Conduct Scenario Planning" | "Feedback to Forecast"
  description: string;
  due_date: string;      // ISO date string
  is_overdue: boolean;
  status: string;        // "New" | "In Review" | "Completed"
  view_link: string;     // Navigation path within app
  // Derived / UI-only fields
  assigned_by?: string;
  cluster?: string;      // Skill Cluster reference if applicable
  priority?: string;     // "High" | "Medium" | "Low"
}

// ── Alert ─────────────────────────────────────────────────────────────────────
// Source: GET /api/alerts

export interface Alert {
  alert_id: string;
  alert_type: string;    // e.g. "Demand Spike", "Model Accuracy Drop", "Short Fuse Surge"
  description: string;
  due_date: string;
  is_overdue: boolean;
  status: string;        // "Action Required" | "Pending Review" | "Finalized"
  view_link: string;
  // UI display fields
  severity?: string;     // "High" | "Medium" | "Low"
  cluster?: string;
  category?: string;
}

// ── Scenario Planning ─────────────────────────────────────────────────────────
// Source: POST /api/v1/scenarios/simulate
// Drivers map to business variables that scale the base ML forecast.

export interface ScenarioDrivers {
  bu_level_growth_pct: number;              // % growth applied at BU level
  industry_level_market_spend_pct: number;  // Industry market spend signal
  win_rate_strategic_pct: number;           // Win rate on strategic deals
  growth_strategic_pct: number;             // Strategic growth target %
}

export interface ScenarioKPIs {
  total_base: number;         // Baseline ML forecast total FTE
  scenario_adjusted: number;  // After driver adjustments
  net_change: number;         // scenario_adjusted - total_base
}

export interface ScenarioFilters {
  practice_area?: string;
  bu?: string;
  location?: string;
  grade?: string;
  skill_cluster?: string;
}

export interface ScenarioSimulateRequest {
  filters: ScenarioFilters;
  drivers: ScenarioDrivers;
}

export interface ScenarioComparisonRow {
  metric: string;          // "Scenario Forecast" | "Baseline Forecast" | "Adjustment"
  [month: string]: number | string; // jan_26, feb_26, etc.
}

export interface ScenarioSimulateResponse {
  kpis: ScenarioKPIs;
  comparison_chart: Array<{ month: string; scenario: number; baseline: number }>;
  comparison_table: { rows: ScenarioComparisonRow[] };
  explainability: string[];
}

// Persisted scenario record
export interface Scenario {
  id: string;
  name: string;
  description: string;
  status: string;        // "Draft" | "Submitted" | "Approved"
  created_by: string;
  created_at: string;
  filters: ScenarioFilters;
  drivers: ScenarioDrivers;
  result?: ScenarioSimulateResponse;
  /** Legacy impact summary returned by the backend alongside the scenario record. */
  impact?: {
    demand_delta: number;
    demand_delta_pct: number;
    supply_gap_delta: number;
    accuracy_impact: number;
  };
}

// ── Forecast Feedback ─────────────────────────────────────────────────────────
// Source: POST /api/v1/feedback/submit

export interface FeedbackScenarioInput {
  scenario_id: string;
  variable: string;
  value: number;
  impact_pct: number;
}

export interface FeedbackSummary {
  total_fte: number;
  hc_target: number;
  variance_from_target: number;
  variance_last_cycle_pct: number;
  onsite_pct: number;
  grade_pct: string;         // e.g. "SA:30%, A:25%, M:20%, GenC:15%, SM:7%, AD:3%"
  stable_volatile: string;   // e.g. "Stable:70%, Volatile:30%"
  forecast_accuracy_pct: number;
}

export interface SkillUpdate {
  type: 'Newly Added' | 'Updated' | 'Removed';
  cluster: string;
  old_skills: string;
  new_skills: string;
}

export interface FeedbackSubmitRequest {
  scenario_inputs: FeedbackScenarioInput[];
  summary: FeedbackSummary;
  skill_updates: SkillUpdate[];
  feedback_text: string;
  action: 'submit' | 'audit_report';
}

// Stored feedback item
export interface FeedbackItem {
  id: string;
  month: string;         // e.g. "Jan 2026"
  cluster: string;       // Skill Cluster
  system_forecast: number;    // ML predicted value (Predicted_Corrected after guardrail)
  mgmt_adjustment: number;    // Delta applied by planner
  final_forecast: number;     // system_forecast + mgmt_adjustment
  reason: string;
  status: string;        // "Pending" | "Approved" | "Rejected"
  submitted_by: string;
  submitted_at: string;
}

// ── Skill Taxonomy ────────────────────────────────────────────────────────────
// Source: GET /api/taxonomy/clusters
// Matches: ml-services/reference-data/skill_clusters.json
// Each cluster key: "MSC-{skill1}-{skill2}-..."

export interface LeafSkill {
  skill: string;        // Normalized skill name (from skill_normalization_llm2.json)
  weight: number;       // Jaccard-based co-occurrence weight within cluster (0-1)
}

export interface TaxonomyCluster {
  id: string;           // Cluster key e.g. "MSC-.NET-Angular-Azure-C#-Java"
  cluster: string;      // Display name (same as id)
  leaf_skills: LeafSkill[];
  total_demands: number;  // mapped_demand: Σ Quantity of SOs assigned to this cluster
  cv_score: number;       // CV across all years (stdev/mean of monthly demand)
  cv_2025: number;        // CV for 2025 only (used in XYZ segmentation)
  stability: number;      // 1 - cv_score, capped [0,1] (higher = more stable)
  xyz_segment: 'X' | 'Y' | 'Z'; // X: CV<0.5 (stable), Y: 0.5-1.0, Z: CV>1.0
  last_updated: string;
  practice_area?: string; // PA abbreviation (ADM | DE | EPS)
  market?: string;        // Americas | EMEA
}

export interface TaxonomyData {
  clusters: TaxonomyCluster[];
  skill_growth: Array<{     // From skill_growth_analysis.json
    skill: string;
    demand_2023: number;
    demand_2024: number;
    demand_2025: number;
    rank_change: number;    // Rank position delta from 2023→2025
    trend: 'Rising' | 'Declining' | 'Stable';
    xyz_segment: 'X' | 'Y' | 'Z';
  }>;
}

// ── Executive Summary ─────────────────────────────────────────────────────────
// Source: GET /api/forecast/executive-summary

export interface ExecutiveSummaryData {
  total_fte_demand: number;
  forecast_accuracy: number;
  top_clusters: Array<{ cluster: string; demand: number; growth_pct: number }>;
  top_practice_areas: NamedPct[];
  demand_vs_supply: { demand: number; supply: number; gap: number; gap_pct: number };
  recent_alerts: Alert[];
  pending_tasks: Task[];
}
