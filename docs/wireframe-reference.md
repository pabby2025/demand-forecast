# Wireframe Reference — Screen-by-Screen Specification

All wireframes are in `docs/wireframes/`. Each screen maps to specific user stories.

## Global Design System

### Layout
- **Sidebar (left, 250px):** Dark navy (#1B2559), 5 nav items with icons
- **Top bar:** App name "Demand Forecast Planner", help/notifications/settings/profile
- **Filter bar:** 6 dropdowns — Practice Area, BU, Location, Grade, Skill Microcluster, Forecast Horizon (all default "All")
- **Content area:** White background, cards with subtle borders and rounded corners

### Navigation Items (Sidebar)
1. Executive Summary (Home icon)
2. Forecast Dashboard (Dashboard icon)
3. Scenario Planning (Settings/gear icon)
4. Skill Micro Cluster Taxonomy (Network icon)
5. Forecast Feedback (Clipboard icon)

### Forecast Dashboard Sub-Tabs (horizontal)
Forecast Overview | Demand Type Breakdown | Business Unit Performance | Geographic Distribution | Skill Distribution | Grade Distribution

### Component Patterns
- **KPI Cards:** White card, title + subtitle, large number, donut/spark chart, info (i) tooltip
- **Donut Charts:** For ratio KPIs (onsite/offshore, new/backfill, stable/volatile)
- **Stacked Bar Charts:** For trend comparisons across categories over time
- **Line Charts:** For growth rates and trend lines (with multiple series)
- **Heatmaps:** For short-fuse demand (intensity blocks per month, color scale <50/50-200/>200)
- **Data Grids:** Practice Area | Location | Cluster | SO Grade | [dimension] | Jan-Dec + Q1-Q4 columns
  - Quarterly columns (Q01, Q02) highlighted in bold blue
  - Download icon (top-right of each grid)
  - Pagination: "< 1 > of 5" (bottom-right)
  - Horizontal scroll for time columns

---

## Screen 01: Executive Summary (Home Page)
**File:** `01-executive-summary.png`
**Stories:** US-007 through US-033

### Sections (top to bottom):
1. **Forecast Overview row:** Forecast Demand (2340 FTEs), Demand Growth Rate, Average Cancellation (40%), Top Practice Areas
2. **Business Unit Performance row:** Top BUs by demand, Top BUs by growth rate (with → drill-down arrow)
3. **Geographic Distribution row:** Onsite vs Offshore donut, Top Countries for Delivery
4. **Skill Distribution row:** Top Skill Micro Clusters (within practice & in SL), Stable vs Volatile donut
5. **Grade Distribution row:** Top grade donut, Shortfuse demand requirement
6. **Demand Type Breakdown row:** New vs Backfill donut, Contract Type Mix donut
7. **Bottom cards:** "My Tasks" CTA card (green), "My Alerts" CTA card (red)

---

## Screen 02: Forecast Overview
**File:** `02-forecast-overview.png`
**Stories:** US-FD-001 through US-FD-026

### Components:
- **KPI Row:** Forecast Demand (2340 FTEs, 12 months), Demand Growth Rate (QoQ/MoM/WoW with LY comparison), Average Cancellation (40%), Explainability & Recommendations panel
- **3 Trend Charts side-by-side:** FTE Demand Monthly (bar+line), FTE Demand Weekly (bar+line), FTE Demand Quarterly (bar+line) — each shows FTE Demand Forecast (bars) + Growth Rate (line, right axis)
- **Data Grid "FTE Demand":** Monthly/Weekly tab toggle. Columns: Practice Area, Location, Cluster, SO Grade, Jan-Dec + Q01-Q03

---

## Screen 03: Demand Type Breakdown
**File:** `03-demand-type-breakdown.png`
**Stories:** US-DT-001 through US-DT-011

### Components:
- **KPI Row:** New Demand vs Backfill donut (65%/35%), Contract Type Mix donut (T&M 55%, Fixed Price 30%, Transaction 15%)
- **2 Trend Charts:** New Demand vs Backfill stacked bar, Billability Type stacked bar (BFD/BTB/BTM)
- **Grid "New Demand vs Backfill":** + Demand Type column
- **Grid "Billability Type":** + Billability Type column

---

## Screen 04: Business Unit Performance
**File:** `04-business-unit-performance.png`
**Stories:** US-BU-001 through US-BU-013

### Components:
- **KPI Row:** Top BUs by demand (Retail NA 30%, Banking 25%, Technology 15%), Top BUs by growth rate (with yoy %)
- **2 Charts:** BU wise demand stacked bar, BU wise growth rates multi-line chart
- **Grid "BU wise demand":** + Business Unit column
- **Grid "BU wise growth rates":** + Business Unit column

---

## Screen 05: Geographic Distribution
**File:** `05-geographic-distribution.png`
**Stories:** US-LM-001 through US-LM-008

### Components:
- **KPI Row:** Onsite vs Offshore donut (68%/32%), Top Countries for Delivery list
- **2 Charts:** Onsite vs Offsite/Nearshore vs Offshore stacked bar, Countrywise Demand mini-table (Country | Jan | Feb | Mar)
- **Grid "Quarterly Demand by Cluster and Grade":** + Onsite/Offshore column

---

## Screen 06: Skill Distribution
**File:** `06-skill-distribution.png`
**Stories:** US-SM-001 through US-SM-017

### Components:
- **KPI Row (4 cards):** Top Skill Micro Clusters (Within Top Practice Area - ADM), Top Skill Micro Clusters (In SL), Stable vs Volatile donut (70%/30%), Top Drivers list
- **2 Charts:** Short Fuse Demand heatmap (rows=clusters, cols=months, color intensity), % Stable & Volatile line chart
- **Grid "Skill Cluster Demand Overview":** + Leaf Skill column
- **Grid "Demand Stability Analysis":** + Demand Type (Stable/Volatile) column

---

## Screen 07: Grade Distribution
**File:** `07-grade-distribution.png`
**Stories:** Grade-related stories

### Components:
- **KPI Row:** Top grade donut (SA 50%, A 25%, M 25%), Shortfuse demand requirement in 6 months list
- **2 Charts:** Grade wise Demand stacked bar (SA/A/M), Short fuse demand heatmap (rows=grades, cols=months)
- **Grid "Monthly Demand by Grade Level":** Standard grid
- **Grid "Short fuse demand for next 6 months":** Feb-Jul + Total column

---

## Screen 08: Demand-Supply Gap
**File:** `08-demand-supply-gap.png`
**Stories:** Supply-related stories

### Components:
- **KPI Row (3 cards):** Fulfillment Gap (6%), Critical Skill Shortage (14 clusters), Fulfillment Time (42 days)
- **2 Charts:** Short Fuse Demand heatmap, Demand & Supply Gap line chart (2 lines: Demand FTE vs Supply FTE)
- **Grid "Short Fuse Demand":** Standard grid
- **Grid "Demand & Supply Gap":** Dual columns per month (Demand | Supply)

---

## Screen 09: Scenario Planning (Edit Mode)
**File:** `09-scenario-planning-edit.png`
**Stories:** US-SP-001 through US-SP-028

### Components:
- **Left Panel "Top Drivers - What If Controls":** 4 sliders — BU Level Growth (65%), Industry Level Market Spend (65%), Win Rate % on Strategic Account (6.0%), Growth % on Strategic Account (65%). "Apply Scenario" button (green) + "Reset" button
- **KPI Row (3 cards):** Total Base (380), Scenario Adjusted (456), Net Change (76) — each with decorative icon
- **Scenario Adjustments card:** Name "Scenario Adjustments #01", delete icon, Scenario vs Baseline line chart
- **Comparison Table:** Rows = Scenario Forecast, Baseline Forecast, Adjustment (green +values). Cols = Jan 26 through May 26
- **Feedback row:** Text input
- **Action buttons:** Submit (green), Save. Pagination: page 2 of 5

---

## Screen 10: Scenario Planning (Review Mode)
**File:** `10-scenario-planning-review.png`
**Stories:** Same as above, read-only view

### Differences from Edit:
- Sliders are non-interactive, "Edit" button instead of "Apply Scenario"/"Reset"
- No Submit/Save/Feedback row
- "In Review" status badge (yellow) at bottom
- Same KPIs, chart, and comparison table

---

## Screen 11: Skill Micro Cluster Taxonomy
**File:** `11-skill-taxonomy.png`
**Stories:** US-TX-001 through US-TX-013

### Components:
- **Breadcrumb:** Home / Skill Micro Cluster Taxonomy
- **Simple table:** Skill Microcluster Name | Leaf Skill 1 | Leaf Skill 2 | Leaf Skill 3 | Leaf Skill 4 | Leaf Skill 5
- **Download icon** (top-right)
- **Pagination:** 1 of 5

---

## Screen 12: Forecast Feedback
**File:** `12-forecast-feedback.png`
**Stories:** US-FF-001 through US-FF-011, FF-NEW-001 through FF-NEW-003

### Components:
- **Breadcrumb:** Home / Forecast Feedback
- **Left Chart:** "Forecast trend split between System Generated/Scenario Planning Forecast" — stacked bar (System Generated Baseline vs Management Overlay)
- **Right Table "Scenario Planning Inputs":** Scenario ID | Input Variables | Value | Impact by %
- **Summary Table:** Total FTE | HC Target | Variance from Target | Variance from Last Cycle | Onsite | Grade % | Stable Vs Volatile | Forecast Accuracy
- **Skill Microcluster Updates table:** Type of Change | Skill Micro cluster | Old Leaf Skills | Updated Leaf Skills + delete icon per row + "Add New Update" button
- **Give your Feedback:** Textarea
- **Buttons:** Submit, Audit Report (with download icon)

---

## Screen 13: My Tasks
**File:** `13-my-tasks.png`
**Stories:** US-016 through US-022

### Components:
- **Breadcrumb:** Home / My Tasks & Approvals
- **Header bar:** "My Tasks and Approvals" (dark navy)
- **Table:** Task ID (blue link) | Task Type | Description | Due Date (with red triangle for overdue) | Status (color-coded badge) | View (link + arrow)
- **Task Types:** Update Skill Micro Cluster, Conduct Scenario Planning, Feedback to Forecast
- **Statuses:** New (red), In Review (yellow), Completed (green)
- **Pagination:** 1 of 5

---

## Screen 14: Alerts
**File:** `14-alerts.png`
**Stories:** US-024 through US-029

### Components:
- **Breadcrumb:** Home / Alerts
- **Header bar:** "My Alerts" (dark navy)
- **Table:** Alert ID (blue link) | Alert Type | Description | Due Date (with red triangle) | Status (color-coded badge) | View (link + arrow)
- **Statuses:** Action Required (red), Pending Review (yellow), Finalized (green)
- **Download icon** (top-right)
- **Pagination:** 1 of 5
