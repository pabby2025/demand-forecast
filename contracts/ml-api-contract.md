# ML Service API Contracts (Based on Real ML Pipeline)

These contracts are derived from the actual Cluster Pipeline output format.
The ML pipeline uses AutoGluon models (LightGBM, NeuralNet, CatBoost) to generate
6-month forecasts at Skill Micro Cluster level, with BU and RLC breakdowns.

---

## Data Dimensions

### Skill Micro Clusters (9 current clusters)
Reference: `src/ml-services/reference-data/skill_clusters.json`
Each cluster has: name, skills[], mapped_demand count, cv (coefficient of variation)

### Grouping Dimensions
- **Skill Cluster** — base level (9 clusters)
- **BU x Skill Cluster** — ~154 combinations (14 BUs x clusters)
- **Country x SO Grade x Skill Cluster (RLC)** — ~188 combinations

### Time Horizon
- M0 through M5 (6 months forward from current month)
- Each month has: Actual, Predicted, and optionally Corrected (guardrail)

### SO Grades
SA, A, M, Gen C, SM, AD

### Countries
India, US, UK, Philippines, Poland, Canada (and others)

### BUs (Sample)
Banking & Capital Markets - NA, Financial Services & FinTech - NA, Insurance-NA,
ML-NA, Provider BU, Retail NA, Technology NA, etc.

---

## Common Query Parameters (All Endpoints)

```
practice_area?: string (default "All")
bu?: string (default "All")
location?: string (default "All")
grade?: string (default "All") — SA | A | M | Gen C | SM | AD
skill_microcluster?: string (default "All")
forecast_horizon?: string (default "All" = 12 months)
```

---

## 1. GET /api/v1/forecast/overview

**Response:**
```json
{
  "kpis": {
    "total_forecast_fte": 2340,
    "demand_growth_rate": {
      "qoq": { "current": 14, "last_year": 10 },
      "mom": { "current": 6, "last_year": 8 },
      "wow": { "current": 3, "last_year": 4 }
    },
    "avg_cancellation_pct": 40,
    "top_practice_areas": [
      { "name": "ADM Central", "pct": 20 },
      { "name": "ADM App dev", "pct": 15 },
      { "name": "ADM AVM", "pct": 10 }
    ]
  },
  "explainability": [
    "Indicates how much growth is from new wins vs replacement"
  ],
  "trend_monthly": [
    { "month": "Jan", "fte_demand": 6000, "growth_rate_pct": 60 }
  ],
  "trend_weekly": [
    { "week": "W1", "fte_demand": 5500, "growth_rate_pct": 55 }
  ],
  "trend_quarterly": [
    { "quarter": "QTR 01", "fte_demand": 7000, "growth_rate_pct": 65 }
  ],
  "grid": [
    {
      "practice_area": "EPS IPM AWS",
      "location": "Americas",
      "cluster": "Microsoft AD",
      "so_grade": "A",
      "jan": 324, "feb": 905, "mar": 105, "q01": 324,
      "apr": 905, "may": 105, "jun": 324, "q02": 324,
      "jul": 905, "aug": 905, "sep": 105, "q03": 105
    }
  ],
  "forecasts_by_cluster": [
    {
      "skill_cluster": "MSC-SQL-JavaScript-MySQL-Spring_Boot-AWS-Angular-Java",
      "months": [
        { "month_index": 0, "actual": 971, "predicted": 571, "predicted_corrected": 929, "accuracy_pct": 95.67 }
      ],
      "model_name": "Gluon::NeuralNetTorch_BAG_L1"
    }
  ]
}
```

---

## 2. GET /api/v1/forecast/demand-type-breakdown

**Response:**
```json
{
  "kpis": {
    "new_vs_backfill": { "new_pct": 65, "backfill_pct": 35 },
    "contract_type_mix": { "t_and_m_pct": 55, "fixed_price_pct": 30, "transaction_based_pct": 15 }
  },
  "trend_new_vs_backfill": [
    { "month": "Jan", "new_demand": 200000, "backfill": 100000 }
  ],
  "trend_billability": [
    { "month": "Jan", "bfd": 180000, "btb": 80000, "btm": 40000 }
  ],
  "grid_new_vs_backfill": [
    { "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "demand_type": "New Demand", "jan": 324, "feb": 905, "mar": 105, "q01": 324 }
  ],
  "grid_billability": [
    { "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "billability_type": "BFD", "jan": 324, "feb": 905, "mar": 105, "q01": 324 }
  ]
}
```

---

## 3. GET /api/v1/forecast/business-unit

**Response:**
```json
{
  "kpis": {
    "top_bus_by_demand": [
      { "name": "Retail NA", "pct": 30 },
      { "name": "Banking & Capital Markets NA", "pct": 25 },
      { "name": "Technology NA", "pct": 15 }
    ],
    "top_bus_by_growth": [
      { "name": "Retail NA", "growth_pct": 10, "unit": "yoy" }
    ]
  },
  "trend_bu_demand": [
    { "month": "Jan", "BU1": 250000, "BU2": 180000, "BU3": 100000 }
  ],
  "trend_bu_growth": [
    { "month": "Jan", "BU1": 3.5, "BU2": 4.2, "BU3": 5.1 }
  ],
  "grid_bu_demand": [
    { "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "business_unit": "BU1", "jan": 324, "feb": 905 }
  ],
  "grid_bu_growth": [
    { "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "business_unit": "BU1", "jan": 324 }
  ],
  "ml_forecasts": [
    { "bu": "Banking & Capital Markets - NA", "skill_cluster": "MSC-...", "months": [{ "month_index": 0, "actual": 558, "predicted": 145 }], "model_name": "Gluon::LightGBM_BAG_L1_FULL" }
  ]
}
```

---

## 4. GET /api/v1/forecast/geographic

**Response:**
```json
{
  "kpis": {
    "onsite_offshore": { "offshore_pct": 68, "onsite_pct": 32 },
    "top_countries": [
      { "country": "US", "pct": 38 },
      { "country": "India", "pct": 30 },
      { "country": "UK", "pct": 12 },
      { "country": "Philippines", "pct": 10 },
      { "country": "Poland", "pct": 10 }
    ]
  },
  "trend_mix": [{ "month": "Jan", "onsite_offsite": 5000, "offshore_mix": 7000 }],
  "countrywise_demand": [{ "country": "India", "jan": 231, "feb": 126, "mar": 810 }],
  "grid": [{ "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "onsite_offshore": "Onsite", "jan": 324 }],
  "rlc_forecasts": [
    { "country": "India", "so_grade": "SA", "skill_cluster": "MSC-...", "months": [{ "month_index": 0, "actual": 461, "predicted": 287 }] }
  ]
}
```

---

## 5. GET /api/v1/forecast/skill-distribution

**Response:**
```json
{
  "kpis": {
    "top_clusters_in_practice": [{ "name": "Java API", "pct": 10 }],
    "top_clusters_in_sl": [{ "name": "Java API", "pct": 10 }],
    "stable_vs_volatile": { "stable_pct": 70, "volatile_pct": 30 },
    "top_drivers": ["Increase Attrition Rate", "New Deals Wins", "SO Growth"]
  },
  "short_fuse_heatmap": { "total": 1347, "change_pct": 2.3, "clusters": [{ "name": "Java API", "monthly": [3,5,4,2,3,5,4,3,2,4,3,5] }], "legend": { "low": "<50", "medium": "50-200", "high": ">200" } },
  "stability_trend": [{ "month": "Jan", "stable": 5000, "volatile": 3000 }],
  "grid_cluster_demand": [{ "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "leaf_skill": "Java", "jan": 324 }],
  "grid_stability": [{ "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "...", "demand_type": "Stable", "jan": 324 }]
}
```

---

## 6. GET /api/v1/forecast/grade-distribution

**Response:**
```json
{
  "kpis": {
    "top_grade": [{ "grade": "SA", "count": 1000, "pct": 50 }],
    "shortfuse_6months": [{ "grade": "SA", "count": 50 }]
  },
  "trend_grade_demand": [{ "month": "Jan", "SA": 280000, "A": 180000, "M": 100000 }],
  "heatmap_short_fuse": { "total": 1347, "change_pct": 2.3, "grades": [] },
  "grid_monthly": [{ "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "A", "jan": 324 }],
  "grid_short_fuse": [{ "practice_area": "...", "location": "...", "cluster": "...", "so_grade": "A", "feb": 2, "mar": 4, "total": 24 }]
}
```

---

## 7. GET /api/v1/forecast/demand-supply-gap

**Response:**
```json
{
  "kpis": { "fulfillment_gap_pct": 6, "critical_skill_shortage": 14, "fulfillment_time_days": 42 },
  "heatmap": { "total": 1347, "change_pct": 2.3, "clusters": [] },
  "demand_supply_trend": [{ "month": "Jan", "demand_fte": 5500, "supply_fte": 4200 }],
  "grid_short_fuse": [],
  "grid_gap": [{ "practice_area": "...", "cluster": "...", "so_grade": "A", "jan_demand": 324, "jan_supply": 324, "feb_demand": 905, "feb_supply": 905 }]
}
```

---

## 8. POST /api/v1/scenario/simulate

**Request:**
```json
{
  "filters": { "practice_area": "All", "bu": "All", "location": "All", "grade": "All", "skill_microcluster": "All" },
  "drivers": { "bu_level_growth_pct": 65, "industry_level_market_spend_pct": 65, "win_rate_strategic_pct": 6.0, "growth_strategic_pct": 65 }
}
```

**Response:**
```json
{
  "kpis": { "total_base": 380, "scenario_adjusted": 456, "net_change": 76 },
  "comparison_chart": [{ "month": "Jan 26", "scenario": 4000, "baseline": 3500 }],
  "comparison_table": {
    "rows": [
      { "metric": "Scenario Forecast", "jan_26": 40, "feb_26": 32, "mar_26": 52 },
      { "metric": "Baseline Forecast", "jan_26": 40, "feb_26": 48, "mar_26": 63 },
      { "metric": "Adjustment", "jan_26": "+50", "feb_26": "+5", "mar_26": "+8" }
    ]
  },
  "explainability": ["Indicates how much growth is from new wins vs replacement"]
}
```

---

## 9. POST /api/v1/feedback/submit

**Request:**
```json
{
  "scenario_inputs": [{ "scenario_id": "255104", "variable": "Growth Rate%", "value": 16, "impact_pct": 6 }],
  "summary": { "total_fte": 2340, "hc_target": 2500, "variance_from_target": 160, "variance_last_cycle_pct": 14, "onsite_pct": 30, "grade_pct": "50% Middle Management", "stable_volatile": "70% Stable", "forecast_accuracy_pct": 70 },
  "skill_updates": [{ "type": "Newly Added", "cluster": "Americas", "old_skills": "Microsoft AD", "new_skills": "324" }],
  "feedback_text": "...",
  "action": "submit | audit_report"
}
```

---

## 10. GET /api/v1/taxonomy/clusters

**Response:** See `src/ml-services/reference-data/skill_clusters.json`

---

## 11. GET /api/v1/tasks & GET /api/v1/alerts

**Tasks Response:**
```json
{ "tasks": [{ "task_id": "54321", "task_type": "Update Skill Micro Cluster | Conduct Scenario Planning | Feedback to Forecast", "description": "...", "due_date": "12 Jan 2025", "is_overdue": true, "status": "New | In Review | Completed", "view_link": "..." }] }
```

**Alerts Response:**
```json
{ "alerts": [{ "alert_id": "54321", "alert_type": "Alert Type 1", "description": "...", "due_date": "12 Jan 2025", "is_overdue": true, "status": "Action Required | Pending Review | Finalized", "view_link": "..." }] }
```

---

## ML Pipeline Integration Notes

### Model Stack (AutoGluon)
- Gluon::LightGBM_BAG_L1 / L1_FULL
- Gluon::NeuralNetTorch_BAG_L1 / L2 / L2_FULL
- Gluon::CatBoost_BAG_L2_FULL

### SSD Guardrail
Prevents under-prediction using seasonal floor corrections.
Adds: SSD_Floor, Predicted_Corrected, Correction_Applied, Accuracy_% columns.

### Cluster Pipeline
See `src/ml-services/cluster-pipeline/` for full source.
Pipeline: Load CSV → Normalize skills (LLM) → Build skill graph → Node2Vec embeddings → KMeans clustering → Triangle microbundles → Map demands.

### Mock Data Ranges
- 9 skill micro clusters
- FTE values: 50-1000 per cluster per month
- Growth rates: 1-15% MoM
- Cancellation: 2-8%
- Onsite: 30-40%, Offshore: 60-70%
- SO Grade distribution: SA 50%, A 25%, M 25%
