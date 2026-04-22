import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import {
  FilterState,
  ForecastOverviewData,
  DemandTypeData,
  BUPerformanceData,
  GeographicData,
  SkillDistributionData,
  GradeDistributionData,
  DemandSupplyGapData,
  ExecutiveSummaryData,
  Task,
  Alert,
  Scenario,
  ScenarioSimulateRequest,
  ScenarioSimulateResponse,
  FeedbackItem,
  FeedbackSubmitRequest,
  TaxonomyData,
} from '../models';

// Runtime config:
//   - Docker / nginx:  window.env is not set → BASE_URL = '' → calls are relative
//                      nginx proxies /api/ → backend:8000
//   - Local dev:       set window.env.API_URL = 'http://localhost:8000' in index.html
//                      or use the Angular proxy.conf.json (npm start uses it automatically)
const BASE_URL =
  (window as Window & { env?: { API_URL?: string } })?.env?.API_URL ?? '';

// API version prefix derived from ml-api-contract.md
const V1 = `${BASE_URL}/api/v1`;
const API = `${BASE_URL}/api`;

function toParams(filters: Partial<FilterState>): HttpParams {
  let params = new HttpParams();
  Object.entries(filters).forEach(([k, v]) => {
    if (v && v !== 'All') params = params.set(k, v);
  });
  return params;
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  constructor(private http: HttpClient) {}

  // ── Auth ────────────────────────────────────────────────────────────────────

  login(email: string, credential: string): Observable<{ user: unknown; token: string; must_change_password: boolean }> {
    return this.http.post<{ user: unknown; token: string; must_change_password: boolean }>(
      `${API}/auth/login`,
      { email, credential }
    );
  }

  changePassword(email: string, current_credential: string, new_password: string): Observable<{ success: boolean; message: string }> {
    return this.http.post<{ success: boolean; message: string }>(
      `${API}/auth/change-password`,
      { email, current_credential, new_password }
    );
  }

  me(): Observable<unknown> {
    return this.http.get(`${API}/auth/me`);
  }

  // ── Forecast (v1 endpoints per ml-api-contract.md) ─────────────────────────

  /**
   * Overview tab: total FTE KPIs, monthly/weekly/quarterly trends,
   * the main forecast grid, and per-cluster M0-M5 ML predictions.
   *
   * Maps to: train_and_predict.py → All_Predictions sheet
   *   Skill Cluster column → skill_cluster
   *   M{n}_actual / M{n}_predicted → months[n].actual / .predicted
   *   Predicted_Corrected → months[n].predicted_corrected (post SSD guardrail)
   *   accuracy_pct (MAPE-based) → months[n].accuracy_pct
   */
  getForecastOverview(filters: Partial<FilterState> = {}): Observable<ForecastOverviewData> {
    return this.http.get<ForecastOverviewData>(
      `${V1}/forecast/overview`,
      { params: toParams(filters) }
    );
  }

  /**
   * Demand-type tab: New Demand vs Backfill, billability mix (BFD/BTB/BTM).
   *
   * Source field mapping:
   *   Requirement type → demand_type ("New Demand" | "Backfill")
   *   Project Billability Type → billability_type ("BFD" | "BTB" | "BTM")
   *   Project Type (EXTN/EXANT) → contract_type_mix
   */
  getDemandType(filters: Partial<FilterState> = {}): Observable<DemandTypeData> {
    return this.http.get<DemandTypeData>(
      `${V1}/forecast/demand-type-breakdown`,
      { params: toParams(filters) }
    );
  }

  /**
   * BU performance tab: per-BU demand and growth trends + per-BU ML forecasts.
   *
   * Source field: BU (Business Unit, corrected via SBU-BU mapping for Americas)
   * Grouping: BU + Skill Cluster (BS grouping in build_training_groups.py)
   */
  getBUPerformance(filters: Partial<FilterState> = {}): Observable<BUPerformanceData> {
    return this.http.get<BUPerformanceData>(
      `${V1}/forecast/business-unit`,
      { params: toParams(filters) }
    );
  }

  /**
   * Geographic tab: onsite/offshore mix, country-wise demand, RLC forecasts.
   *
   * Source field mapping:
   *   Off/ On → onsite_offshore ("Onsite" | "Offshore")
   *   Country → country
   * Grouping: Country + SO GRADE + Skill Cluster (RLC grouping)
   */
  getGeographic(filters: Partial<FilterState> = {}): Observable<GeographicData> {
    return this.http.get<GeographicData>(
      `${V1}/forecast/geographic`,
      { params: toParams(filters) }
    );
  }

  /**
   * Skill distribution tab: cluster stability, short-fuse heatmap, leaf skills.
   *
   * Source field mapping:
   *   Technical Skills Required → Skills Normalized → Skill Groups → Skill Cluster
   *   CV score (stdev/mean of monthly demand) → cv_score / xyz_segment
   *   X: CV<0.5 (Stable), Y: 0.5-1.0 (Variable), Z: CV>1.0 (Sporadic)
   */
  getSkillDistribution(filters: Partial<FilterState> = {}): Observable<SkillDistributionData> {
    return this.http.get<SkillDistributionData>(
      `${V1}/forecast/skill-distribution`,
      { params: toParams(filters) }
    );
  }

  /**
   * Grade distribution tab: per-grade demand, short-fuse heatmap.
   *
   * Source field mapping:
   *   SO GRADE → grade (normalized: PT/PAT/PA/P → GenC; cont/D/VP removed)
   *   Grades: SA | A | M | GenC | SM | AD
   */
  getGradeDistribution(filters: Partial<FilterState> = {}): Observable<GradeDistributionData> {
    return this.http.get<GradeDistributionData>(
      `${V1}/forecast/grade-distribution`,
      { params: toParams(filters) }
    );
  }

  /**
   * Demand-supply gap tab: gap % by cluster, fulfillment time.
   */
  getDemandSupplyGap(filters: Partial<FilterState> = {}): Observable<DemandSupplyGapData> {
    return this.http.get<DemandSupplyGapData>(
      `${V1}/forecast/demand-supply-gap`,
      { params: toParams(filters) }
    );
  }

  /** Executive summary: cross-tab KPIs for SL_COO / MARKET_COO dashboard. */
  getExecutiveSummary(filters: Partial<FilterState> = {}): Observable<ExecutiveSummaryData> {
    return this.http.get<ExecutiveSummaryData>(
      `${V1}/forecast/executive-summary`,
      { params: toParams(filters) }
    );
  }

  // ── Tasks ───────────────────────────────────────────────────────────────────

  getTasks(status?: string): Observable<{ tasks: Task[] }> {
    let params = new HttpParams();
    if (status) params = params.set('status', status);
    return this.http.get<{ tasks: Task[] }>(`${API}/tasks`, { params });
  }

  updateTask(id: string, data: Partial<Pick<Task, 'status'>>): Observable<Task> {
    return this.http.put<Task>(`${API}/tasks/${id}`, data);
  }

  // ── Alerts ──────────────────────────────────────────────────────────────────

  getAlerts(status?: string): Observable<{ alerts: Alert[] }> {
    let params = new HttpParams();
    if (status) params = params.set('status', status);
    return this.http.get<{ alerts: Alert[] }>(`${API}/alerts`, { params });
  }

  acknowledgeAlert(id: string): Observable<Alert> {
    return this.http.put<Alert>(`${API}/alerts/${id}/acknowledge`, {});
  }

  dismissAlert(id: string): Observable<Alert> {
    return this.http.put<Alert>(`${API}/alerts/${id}/dismiss`, {});
  }

  // ── Scenario Planning ────────────────────────────────────────────────────────

  getScenarios(): Observable<{ scenarios: Scenario[] }> {
    return this.http.get<{ scenarios: Scenario[] }>(`${API}/scenarios`);
  }

  /**
   * Run a what-if simulation using business driver adjustments.
   * Drivers scale the base ML forecast (AutoGluon output) at BU / industry level.
   *
   * Driver mapping:
   *   bu_level_growth_pct              → scales BU x Skill Cluster (BS) predictions
   *   industry_level_market_spend_pct  → macro signal applied uniformly
   *   win_rate_strategic_pct           → adjusts new-demand portion
   *   growth_strategic_pct             → strategic growth target applied to base
   */
  simulateScenario(request: ScenarioSimulateRequest): Observable<ScenarioSimulateResponse> {
    return this.http.post<ScenarioSimulateResponse>(
      `${V1}/scenarios/simulate`,
      request
    );
  }

  createScenario(data: Omit<Scenario, 'id' | 'created_at'>): Observable<Scenario> {
    return this.http.post<Scenario>(`${API}/scenarios`, data);
  }

  updateScenario(id: string, data: Partial<Scenario>): Observable<Scenario> {
    return this.http.put<Scenario>(`${API}/scenarios/${id}`, data);
  }

  // ── Forecast Feedback ────────────────────────────────────────────────────────

  getFeedback(): Observable<{ items: FeedbackItem[] }> {
    return this.http.get<{ items: FeedbackItem[] }>(`${API}/feedback`);
  }

  /**
   * Submit management adjustments on top of ML forecasts.
   * final_forecast = system_forecast (Predicted_Corrected) + mgmt_adjustment
   */
  submitFeedback(request: FeedbackSubmitRequest): Observable<{ success: boolean; feedback_id: string }> {
    return this.http.post<{ success: boolean; feedback_id: string }>(
      `${V1}/feedback/submit`,
      request
    );
  }

  updateFeedback(id: string, data: Partial<FeedbackItem>): Observable<FeedbackItem> {
    return this.http.put<FeedbackItem>(`${API}/feedback/${id}`, data);
  }

  deleteFeedback(id: string): Observable<void> {
    return this.http.delete<void>(`${API}/feedback/${id}`);
  }

  // ── Skill Taxonomy ───────────────────────────────────────────────────────────

  /**
   * Returns all Skill Micro Clusters with their leaf skills, CV scores,
   * XYZ segments, and demand counts.
   *
   * Sourced from: ml-services/reference-data/skill_clusters.json
   * Updated by:   skill_clusters_demand.py + apply_clusters.py pipeline runs
   * Skill names are normalized via skills/skill_normalization_llm2.json
   */
  getTaxonomy(): Observable<TaxonomyData> {
    return this.http.get<TaxonomyData>(`${API}/taxonomy/clusters`);
  }
}
