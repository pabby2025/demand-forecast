# Claude Code — Ready-to-Use Build Prompts

Copy-paste these into Claude Code sequentially. Each builds on the previous.

---

## Phase 1: Project Scaffolding

```
Read CLAUDE.md thoroughly. Then set up the complete project:

1. Frontend (src/frontend/): React + TypeScript + Vite
   - Tailwind CSS configured
   - TanStack Query + TanStack Table installed
   - Recharts for charting
   - React Router with routes for all 14 screens (see wireframes list in CLAUDE.md)
   - Base layout: dark navy sidebar (5 nav items) + top bar + filter bar + content area

2. Backend (src/backend/): Python FastAPI
   - 5 service modules: demand_mgmt, supply_mgmt, forecasting_ai, agentic, notifications
   - Pydantic models matching contracts/ml-api-contract.md
   - Mock data generators using the 9 real skill clusters from src/ml-services/reference-data/skill_clusters.json
   - Health check endpoints, CORS config

3. Docker: docker-compose.yml with frontend, backend, PostgreSQL

4. Shared types file for API contract types.
```

---

## Phase 2: Executive Summary + Navigation + Auth

```
Read CLAUDE.md, docs/wireframe-reference.md (Screen 01, 13, 14).
Look at wireframes: docs/wireframes/01-executive-summary.png, 13-my-tasks.png, 14-alerts.png

Build:

1. Mock SSO login with RBAC (SL/Market/CFT personas)
2. Sidebar navigation matching wireframe exactly (dark navy, 5 items with icons, active highlight)
3. Executive Summary page (Screen 01) with ALL sections:
   - Forecast Overview KPI row (2340 FTEs, Growth Rate, Cancellation 40%, Top Practices)
   - Business Unit Performance row (Top BUs by demand/growth, drill-down arrows)
   - Geographic Distribution row (Onsite/Offshore 68/32 donut, Top Countries)
   - Skill Distribution row (Top Clusters, Stable/Volatile 70/30 donut)
   - Grade Distribution row (Top grade donut SA/A/M, Shortfuse demand)
   - Demand Type Breakdown row (New/Backfill 65/35, Contract Type Mix T&M/FP/Trans)
   - Bottom CTAs: My Tasks card (green), My Alerts card (red)
4. My Tasks page (Screen 13): Table with Task ID, Type, Description, Due Date, Status badges
5. Alerts page (Screen 14): Table with Alert ID, Type, Description, Due Date, Status badges

Backend: GET /api/v1/home/overview, /api/v1/tasks, /api/v1/alerts with mock data.
Match the color palette: Navy #1B2559, Teal #00BCD4, status badges red/yellow/green.
```

---

## Phase 3: Forecast Dashboard — All 7 Tabs

```
Read docs/wireframe-reference.md (Screens 02-08) and contracts/ml-api-contract.md.
Look at ALL wireframes in docs/wireframes/ numbered 02 through 08.

Build the Forecast Dashboard with horizontal tab navigation and 7 sub-pages:

Tab 1 - Forecast Overview (Screen 02):
- 4 KPI cards: Forecast Demand, Growth Rate (QoQ/MoM/WoW with LY), Cancellation %, Explainability
- 3 side-by-side trend charts: Monthly (bar+line), Weekly (bar+line), Quarterly (bar+line)
- Data grid with Monthly/Weekly tab toggle, pagination
- API: GET /api/v1/forecast/overview

Tab 2 - Demand Type Breakdown (Screen 03):
- 2 KPI donuts: New vs Backfill (65/35), Contract Type Mix (T&M 55/FP 30/Trans 15)
- 2 stacked bar trend charts
- 2 data grids (New vs Backfill + Billability Type)
- API: GET /api/v1/forecast/demand-type-breakdown

Tab 3 - Business Unit Performance (Screen 04):
- 2 KPI cards: Top BUs by demand, Top BUs by growth
- Stacked bar (BU demand) + multi-line chart (BU growth rates)
- 2 data grids with Business Unit column
- API: GET /api/v1/forecast/business-unit

Tab 4 - Geographic Distribution (Screen 05):
- 2 KPI cards: Onsite/Offshore donut (68/32), Top Countries list
- Stacked bar (mix evolution) + mini country table
- Data grid with Onsite/Offshore column
- API: GET /api/v1/forecast/geographic

Tab 5 - Skill Distribution (Screen 06):
- 4 KPI cards: Top Clusters in Practice, Top Clusters in SL, Stable/Volatile donut, Top Drivers
- Heatmap (clusters × months) + Stable/Volatile line chart
- 2 grids: Skill Cluster Demand (with Leaf Skill col) + Demand Stability (with Demand Type col)
- API: GET /api/v1/forecast/skill-distribution

Tab 6 - Grade Distribution (Screen 07):
- 2 KPIs: Top grade donut (SA 50/A 25/M 25), Shortfuse demand requirements
- Stacked bar (grade demand) + heatmap (grades × months)
- 2 grids: Monthly by Grade + Short fuse next 6 months (with Total col)
- API: GET /api/v1/forecast/grade-distribution

Tab 7 - Demand-Supply Gap (Screen 08):
- 3 KPI cards: Fulfillment Gap 6%, Critical Shortage 14 clusters, Fulfillment Time 42 days
- Heatmap + Demand/Supply line chart (2 lines)
- 2 grids: Short Fuse + Demand/Supply Gap (dual columns per month)
- API: GET /api/v1/forecast/demand-supply-gap

Use TanStack Table for all grids (sorting, horizontal scroll, pagination).
Use Recharts for all charts. Match wireframe colors and layout precisely.
```

---

## Phase 4: Scenario Planning + Forecast Feedback

```
Read docs/wireframe-reference.md (Screens 09, 10, 12).
Look at wireframes: 09-scenario-planning-edit.png, 10-scenario-planning-review.png, 12-forecast-feedback.png

Build:

1. Scenario Planning - Edit Mode (Screen 09):
   - Left panel: "Top Drivers - What If Controls" with 4 sliders:
     BU Level Growth (65%), Industry Level Market Spend (65%),
     Win Rate % on Strategic Account (6.0%), Growth % on Strategic Account (65%)
   - "Apply Scenario" green button + "Reset" button
   - 3 KPI cards: Total Base (380), Scenario Adjusted (456), Net Change (76)
   - Scenario Adjustments card with name, delete icon
   - Scenario vs Baseline dual-line chart
   - Comparison table: Scenario Forecast / Baseline Forecast / Adjustment rows
   - Feedback text input
   - Submit (green) + Save buttons
   - Pagination (page 2 of 5)
   - API: POST /api/v1/scenario/simulate

2. Scenario Planning - Review Mode (Screen 10):
   - Same layout but read-only sliders, "Edit" button
   - "In Review" yellow status badge
   - No Submit/Save/Feedback

3. Forecast Feedback (Screen 12):
   - Left: System vs Management overlay stacked bar chart
   - Right: Scenario Planning Inputs table (Scenario ID, Variable, Value, Impact %)
   - Summary Table: Total FTE, HC Target, Variance, Onsite, Grade %, Stability, Accuracy
   - Skill Microcluster Updates CRUD table with "Add New Update" button
   - Free-text feedback textarea
   - Submit + Audit Report buttons
   - API: POST /api/v1/feedback/submit
```

---

## Phase 5: Skill Taxonomy + CFT Features

```
Read docs/wireframe-reference.md (Screen 11) and stories for CFT persona in stories/stories-by-epic.md.
Look at wireframe: 11-skill-taxonomy.png

Build:

1. Skill Micro Cluster Taxonomy (Screen 11):
   - Breadcrumb: Home / Skill Micro Cluster Taxonomy
   - Simple table: Microcluster Name | Leaf Skill 1-5
   - Populate from src/ml-services/reference-data/skill_clusters.json (real data!)
   - Download icon, pagination
   - API: GET /api/v1/taxonomy/clusters

2. Governance Dashboard (CFT only):
   - Governance matrix grid showing SL/Market progress
   - Navigation, filters, download
   - API: GET /api/v1/governance/matrix

3. Task Management (CFT):
   - Modelling status grid with retrain/flag actions
   - Data ingestion: quality tickets, missing data requests

4. Performance (CFT):
   - SL/Market toggle, score weighting, review scheduling

5. Model Improvement (CFT):
   - Skill signal viewer, taxonomy CRUD editor
```

---

## Phase 6: ML Pipeline Integration

```
Read src/ml-services/cluster-pipeline/Cluster_Pipeline_Documentation.md and all .py files.
Read contracts/ml-api-contract.md for the target API shapes.

1. Wrap the Cluster Pipeline in a FastAPI service:
   - POST /api/v1/ml/run-pipeline — trigger full pipeline
   - GET /api/v1/ml/clusters — return current clusters from skill_clusters.json
   - POST /api/v1/ml/retrain — trigger Optuna retraining

2. Create forecast serving endpoints that read the AutoGluon output Excel files:
   - Parse combined_group_results*.xlsx files
   - Serve at Skill Cluster, BU x Skill Cluster, and RLC levels
   - Include guardrail (SSD) corrected predictions

3. Connect Agentic Service:
   - Azure OpenAI integration (GPT 5.1/4.1 mini)
   - Vector DB setup (pgvector)
   - LangChain/AutoGen agent orchestration

4. Replace all mock data with real ML pipeline outputs.

5. Update Docker Compose for the full service mesh.
```

---

## Quick Reference for Each Session

- Start every session: "Read CLAUDE.md first"
- For UI: "Look at wireframe docs/wireframes/[##-name].png and docs/wireframe-reference.md"
- For API shapes: "Follow contracts/ml-api-contract.md"
- For ML: "Read src/ml-services/cluster-pipeline/ modules"
- For skill data: "Use real clusters from src/ml-services/reference-data/skill_clusters.json"
- For stories: "Check stories/stories-by-epic.md for acceptance criteria"
