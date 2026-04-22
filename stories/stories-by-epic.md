# User Stories by Epic

## Persona: Service Line

### Home Page

**US-001: Enterprise Login** [Simple]
- Story: As a Service Line team member, I want to log into the application using my enterprise credentials, so that I don’t need to manage a separate password
- AC: Given I have valid enterprise credentials
When I attempt to log in
Then I should be granted access without entering a separate app password

**US-002: Route to Service Line Homepage** [Simple]
- Story: As a Service Line team member, I want to be redirected to my Service Line homepage after login, so that I can immediately begin work in my scope.
- AC: Given I successfully authenticate
When the login completes
Then I am taken to my Service Line home page

**US-003: Load Context (Service Line + Horizon)** [Simple]
- Story: As a Service Line team member, I want the homepage to automatically load my assigned Service Line and time horizon, so that I see relevant information without manually selecting it.
- AC: Given I have one or more Service Line assignments
When I land on the home page
Then my assigned Service Line and next-12-months view should already be applied

**US-004: View Access Restricted to My Scope** [Simple]
- Story: As a Service Line team member, I want my access to be limited to my Service Line, so that data remains secure and aligned to my role.
- AC: Given I am logged into the system
When I attempt to view any Service Line data
Then I see only data mapped to my assigned Service Line(s)

**US-005: Session Timeout & Re-Authentication** [Simple]
- Story: As a Service Line team member, I want the system to log me out after inactivity and require re-authentication, so that security policies are upheld.
- AC: Given I am logged in
When my session expires due to inactivity
Then I should be logged out and prompted to sign in again

**US-006: Missing Roles Error** [Simple]
- Story: As a user without Service Line mapping, I want a clear error message, so that I know why I cannot proceed and who to contact. 
- AC: Given I log in without a mapped role or Service Line
When the system loads
Then I should see an error informing me that no role is mapped and an admin must assign access

**US-007: View Overview + KPIs** [Simple]
- Story: As a Service Line team member, I want to see key KPIs and demand summary immediately on login, so that I can quickly understand my area’s status.
- AC: Given I access my Service Line homepage
When the page loads
Then I should see KPI tiles and overview data relevant to my Service Line

**US-008: View KPI Tiles at a Glance** [Simple]
- Story: As a Service Line COO, I want to see key KPI tiles summarising demand and fulfillment, so that I can immediately assess operational status.
- AC: Given I am on the home page and have access to a Service Line
When the page loads
Then I should see KPI tiles showing total demand, fulfillment gap %, practice areas, micro clusters, and onsite/offshore ratio

**US-009: View Forecast Trend Line Chart** [Simple]
- Story: As a Service Line COO, I want to view a forecast demand trend line, so that I can understand how demand is shaping over time.
- AC: Given I have forecast data
When the trend graph loads
Then I should see a 12-month line graph showing forecast FTE with hover details for each month

**US-010: View Short-Fuse Heatmap** [Simple]
- Story: As a Service Line COO, I want to see short fuse demand across skill clusters in a heatmap, so that I can spot resourcing risk areas quickly
- AC: Given short fuse demand exists
When I view the heatmap
Then I should see a colored grid grouped into volume buckets showing the intensity by month and 10 skill clusters

**US-011: View % Change in Short Fuse Demand** [Simple]
- Story: As a Service Line COO, I want a metric that compares short fuse demand to a previous period, so that I can see whether urgency is trending up or down.
- AC: Given the current and comparable period demand exist
When the KPI loads
Then I should see a % change trend (e.g., higher/lower than previous period)

**US-012: RBAC-Aligned Data Visibility** [Medium]
- Story: As a Service Line COO, I want to see only data for Service Lines I own, so that confidentiality is preserved.
- AC: Given I am logged in
When dashboards load
Then I should only see data related to my assigned Service Lines

**US-013: No Data State** [Simple]
- Story: As a Service Line COO, I want the system to clearly indicate when no data is available, so that I understand the absence of results is not an error.
- AC: Given there is no recorded short fuse demand for the selected period
When I view the heatmap
Then I should see a message indicating no short fuse demand exists

**US-015: Navigate to Tasks & Approvals** [Simple]
- Story: As a Service Line COO, I want to quickly notice pending tasks, so that I can take necessary action without navigating multiple menus
- AC: Given I have actionable items
When I load the home page
Then I should see a “My Tasks & Approvals” button to redirect to the tasks page

**US-016: View Tasks & Approvals List** [Medium]
- Story: As a Service Line COO, I want to see all pending approval tasks, so that I immediately understand what needs my attention
- AC: Given I am a logged-in COO
When I open the home page
Then I should see a task list showing pending items relevant to my Service Line

**US-017: Understand Task Attributes** [Medium]
- Story: As a Service Line COO, I want each task to display key information, so that I can prioritise what to review
- AC: Given I have tasks assigned
When I view the list
Then each row should show task ID, type, description, due date, and status

**US-018: Identify Status and Overdue Items** [Medium]
- Story: As a Service Line COO, I want clear task status indicators, so that overdue or urgent items stand out.
- AC: Given today's date and task due dates are available
When a task is past its due date and not completed
Then the task should display an overdue status and visual warning

**US-020: Navigate to Task Detail Page** [Medium]
- Story: As a Service Line COO, I want to open a task and view its details, so that I can take the required decision or provide feedback
- AC: Given a task is shown on my list
When I click “view task”
Then I should be taken to the corresponding detail screen with my Service Line context already applied

**US-021: RBAC-Constrained Visibility** [Medium]
- Story: As a Service Line COO, I want to see only tasks relevant to me or my Service Line, so that confidentiality is preserved.
- AC: Given RBAC permissions exist
When the task list loads
Then I should only see tasks for my assigned Service Line(s)

**US-022: Empty State When No Tasks** [Medium]
- Story: As a Service Line COO, I want the system to clearly indicate when there are no pending tasks, so that I know nothing requires action.
- AC: Given no tasks are assigned to me
When I load the task panel
Then I should see a message such as “No pending tasks

**US-024: View Alerts Panel** [Medium]
- Story: As a Service Line COO, I want to see an alert panel on my home page, so that I am instantly aware of important changes
- AC: Given I am logged in and on the home page
When alert conditions exist
Then an alerts section should appear summarising the latest alerts

**US-025: View Alert Types and Context** [Medium]
- Story: As a Service Line COO, I want each alert to show the details of the change, so that I understand at a glance what has shifted.
- AC: Given alerts are visible
When I review an alert row
Then I should see alert type, impacted skill/practice area (if applicable), and the relevant time period

**US-026: See No Alerts Message** [Medium]
- Story: As a Service Line COO, I want the system to tell me when no alerts exist, so that I know everything is stable.
- AC: Given there are no changes meeting alert thresholds
When I view the home page
Then the alert section should display the message “No alerts.”

**US-027: Get Forecast Change Alerts** [Medium]
- Story: As a Service Line COO, I want to be notified when forecast demand materially changes, so that I can plan and respond proactively
- AC: Given a forecast exceeds defined change thresholds
When the change is detected
Then an alert should display showing the % change and impacted area

**US-028: Get Market Signal Alerts** [Medium]
- Story: As a Service Line COO, I want alerts when relevant market indicators change, so that I can align supply decisions with external cues
- AC: Given market signals are updated in source systems
When the update crosses the system’s alert logic
Then I should see a market signal alert in my list

**US-029: Get Skill Taxonomy Change Alerts** [Medium]
- Story: As a Service Line COO, I want alerts when skill clusters or profiles are redefined, so that I understand why data shifts may occur.
- AC: Given taxonomy or skill micro cluster definitions change
When the system detects this change
Then an alert should indicate the impacted areas and what changed

**US-030: Access Application Navigation** [Medium]
- Story: As a Service Line COO, I want a persistent navigation menu, so that I can move across workstreams consistently.
- AC: Given I am logged into the application
When I move between epics or pages
Then I should always see the left side menu with fixed Service Line tabs

**US-031: Highlight Active Page** [Simple]
- Story: As a Service Line COO, I want the active page to be highlighted in the menu, so that I always know where I am in the application.
- AC: Given I am on any application screen
When the navigation menu loads
Then the menu item for the current page should be visually highlighted

**US-033: Retain Context on Browser Refresh** [Simple]
- Story: As a Service Line COO, I want my working context to remain unchanged if I refresh the browser, so that I do not lose my place
- AC: Given I am viewing a planning view with context applied
When I refresh the browser
Then the Service Line and horizon should still be applied without requiring re-selection

---

### Forecast Dashboard (Business Unit)

**US-BU-004: View BU Wise Demand Stack** []
- Story: As a Service Line team member, I want to see a stacked bar chart of demand over time, split by Business Unit, so that I can visualize the changing portfolio mix.
- AC: Given I am viewing the "BU wise demand" widget When the chart loads Then each month's bar should be stacked with segments for "BU1", "BU2", "BU3" And the total height of the bar should represent the aggregate demand of the selected BUs.

**US-BU-005: View BU Wise Growth Curves** []
- Story: As a Service Line team member, I want to compare the growth trajectories of different BUs on a single multi-line chart, so that I can spot if a specific unit is crashing while others are growing.
- AC: Given I am viewing the "BU wise growth rates" widget When the chart loads Then I should see distinct lines for each top BU And the Y-axis should represent growth percentage (allowing for negative values if a BU is shrinking).

**US-BU-008: Delivery Location Context** []
- Story: As a Service Line team member, I want to see how my BU is performing in specific delivery locations (e.g., Bangalore vs. Pune), so that I can balance my offshore supply chain.
- AC: Given I have selected "Retail NA" as the BU When I change the "Delivery Location" filter to "Bangalore" Then the "BU wise demand" chart should update to show only the Retail demand fulfilled from Bangalore.

---

### Forecast Dashboard (Forecast Overview)

**US-FD-014: View Precise Values on Hover** [Simple]
- Story: As a Service Line team member, I want to see the exact FTE count and Growth % when I hover over a month/week, so that I don't have to estimate based on the axis lines.
- AC: Given I am viewing any of the three trend charts When I hover my mouse over a specific bar (e.g., "March" or "QTR 02") Then a tooltip should display: 1. Period Name 2. Exact FTE Count (e.g., 2,340) 3. Exact Growth Rate (e.g., +14%).

**US-FD-017: Synchronize Charts with Global Filters** [Simple]
- Story: As a Service Line team member, I want all three charts (Monthly, Weekly, Quarterly) to update instantly when I change a global filter (e.g., Location), so that the story remains consistent.
- AC: Given I change the "Location" filter from "Bangalore" to "London" When the dashboard refreshes Then ALL three charts should redraw simultaneously to reflect London data only.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**Fo-NEW-GRID: View data grid** [Medium]
- Story: As a Service Line team member, I want to view the data grid by default sort
- AC: Given I am on a dashboard page
When I scroll to the bottom data grid section
Then the data grid should show the same metric values used in the charts for the selected filters
And the default sort order should be Largest to Lowest by Total Demand.

**US-FD-015: Calculate Trend Signal** [Complex]
- Story: As a Service Line team member, I want the "Growth Rate" line to accurately reflect the change against the previous cycle, so that the trend signal is reliable.
- AC: Given the current forecast cycle (Cycle N) and previous cycle (Cycle N-1) When the line chart renders Then the data point for a period should be calculated as: (Current Demand - Previous Demand) / Previous Demand.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-FD-018: Chart Empty State** [Simple]
- Story: As a Service Line team member, I want to know if there is no data for a specific view (e.g., Weekly data missing), rather than seeing a broken chart.
- AC: Given the selected "Skill Micro Cluster" has no demand forecasted for "Week 1" When the Weekly chart loads Then it should display a "No Forecast Data Available for this selection" message in the chart area.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-FD-026: Global API Failure** [Medium]
- Story: As a Service Line team member, I want the system to handle data load failures gracefully, so that I am not left staring at spinning loaders.
- AC: Given the backend API returns a 500 error When the page attempts to load Then the specific widget failing should show a "Retry" button And a global toast message should appear: "Some data failed to load. Please try again."

**Fo-NEW-DL: Download dashboard data** [Simple]
- Story: As a Service Line team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids.

**US-FD-003: Filter by Forecast Horizon** [Simple]
- Story: As a Service Line team member, I want to change the time horizon (e.g., 6 months vs 12 months), so that the KPIs and charts reflect the specific planning period I am working on.
- AC: Given the default view is "12 Months" When I select "6 Months" from the dropdown Then the "Forecast Demand" KPI value should decrease to reflect only that period And all trend charts below should restrict their X-axis to 6 months.

**US-FD-004: Cascading Filter Logic** [Medium]
- Story: As a Service Line team member, I want the "Skill Micro Cluster" options to filter based on my selected "Practice Area", so that I don't see irrelevant skills.
- AC: Given I have selected "Cloud" as the Practice Area When I click the "Skill Micro Cluster" dropdown Then I should only see skills related to Cloud (e.g., Azure, AWS) And I should NOT see skills related to other areas (e.g., Clinical Research).

**US-FD-001: Default Dashboard Load** [Medium]
- Story: As a Service Line team member, I want the "Forecast Overview" tab to load by default with my primary Practice Area selected, so that I see my relevant data immediately.
- AC: Given I am a user mapped to the "Cloud" practice When I navigate to the "Forecast Dashboard" module Then the "Forecast Overview" tab should be active And the "Practice Area" filter should auto-select "Cloud".

**US-FD-010: View Explainability Summary** []
- Story: As a Service Line team member, I want to see a text summary of the "Explainability and Recommendations", so that I understand the "Why" behind the numbers without deep analysis.
- AC: Given the AI engine has analyzed the data When I look at the fourth panel Then I should see a natural language sentence explaining the primary driver (e.g., "Growth is from new wins vs replacement").

**US-FD-006: View KPI Definition Tooltip** []
- Story: As a Service Line team member, I want to see the definition of "Forecast Demand", so that I understand exactly what data sources are included (e.g., does it include soft bookings?).
- AC: Given I am unsure about a metric When I hover over the "i" (Information) icon on the KPI card Then a tooltip should appear defining the calculation logic.

**US-FD-005: View Total Forecast Demand** []
- Story: As a Service Line team member, I want to see the total "Forecast Demand" in FTEs for the selected horizon, so that I have a high-level volume anchor.
- AC: Given forecast data exists When I view the first KPI card Then I should see the total aggregated FTE count (e.g., 2340).

**US-FD-007: View Demand Growth Rates** []
- Story: As a Service Line team member, I want to see growth rates across different timeframes (QoQ, MoM, WoW), so that I can detect acceleration or deceleration in demand.
- AC: Given historical data exists When I view the "Demand Growth Rate" card Then I should see three distinct metrics: Quarter-over-Quarter, Month-over-Month, and Week-over-Week.

**US-FD-009: View Cancellation Rate** []
- Story: As a Service Line team member, I want to track the "Average Cancellation" rate, so that I can assess the reliability of the demand signal.
- AC: Given cancellation data is recorded When I view the third KPI card Then I should see the percentage of demand cancelled (e.g., 40%) And a visual indicator showing if this is high or low risk.

**US-FD-011: View Monthly Demand vs Growth** []
- Story: As a Service Line team member, I want to view "FTE Demand" and "Growth Rate" on a monthly basis, so that I can identify mid-term seasonality or demand spikes.
- AC:  When I view the "FTE demand- Monthly" widget Then I should see a dual-axis chart And "FTE Demand" should be represented as Cyan bars (Left Axis) And "Growth Rate" should be represented as a Grey line (Right Axis).

**US-FD-013: View Quarterly Strategic View** []
- Story: As a Service Line team member, I want to view demand aggregated by Quarter (Q1-Q4), so that I can align the forecast with fiscal reporting cycles.
- AC: Given I am viewing the "FTE demand- Quarterly" widget When the chart loads Then the bars should represent the sum of FTEs for all months in that quarter And the line chart should show the QoQ growth percentage.

**Fo-NEW-NAV: View and use dashboard navigation tabs** []
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

**US-FD-002: Switch Dashboard Tabs** []
- Story: As a Service Line team member, I want to switch between different analytical views (e.g., Location Mix, Skill Mix), so that I can investigate demand from different angles.
- AC: Given I am on the Forecast Overview When I click the "Location Mix" tab Then the main content area should refresh with location-specific charts

**US-FD-012: View Weekly Demand Granularity** []
- Story: As a Service Line team member, I want to view the forecast breakdown by Week (W1-W6), so that I can manage short-term "boots on the ground" deployment.
- AC: Given I am viewing the "FTE demand- Weekly" widget When the chart loads Then the X-axis should display standard work weeks (W1, W2, etc.) And the bars should show volume for that specific week only.

---

### Demand Type Breakdown

**US-DT-007: View Demand Source Table** [Complex]
- Story: As a Service Line team member, I want to see the detailed demand breakdown by attributes (Location, Cluster, Grade) in a table, so that I can operationalize the specific open roles.
- AC: Given I scroll to the "New Demand vs Backfill" table When the grid loads Then I should see columns for: Practice Area, Location, Cluster, SO Grade, and Demand Type.
And the default sort order should be Largest to Lowest by Total Demand.

**US-DT-011: Sort Data Grid** [Simple]
- Story: As a Service Line team member, I want the default sort of the table by totald emand
- AC: Given the table is loaded When I click the "Location" column header, the default sort order should be Largest to Lowest by Total Demand.

**US-DT-008: Quarterly Aggregation** [Medium]
- Story: As a Service Line team member, I want to see Quarterly totals (Q1, Q2) automatically calculated within the table, so that I can see the aggregate volume without manual math.
- AC: Given the table displays monthly data (Jan, Feb, Mar) When I look at the "Q 01" column Then the value should be the SUM of Jan + Feb + Mar for that row.

**US-DT-009: View Billability Detail** [Complex]
- Story: As a Service Line team member, I want to see a dedicated table for "Billability Type", so that I can verify the specific accounting codes (BFD/BTB) assigned to each forecasting line.
- AC: Given I scroll to the "Billability Type" table When the grid loads, "Billability Type" column should be visible And rows should be deafult sorted by total demand highest to lowest

**US-DT-010: Horizontal Scroll** [Complex]
- Story: As a Service Line team member, I want to scroll horizontally if the forecast horizon extends beyond the screen width, so that I can view future quarters (e.g., Q3, Q4).
- AC: Given the forecast covers 12 months When the columns exceed the screen width Then a horizontal scrollbar should appear at the bottom of the table container And the "Dimension" columns (Practice Area, Location) should remain frozen (sticky) while scrolling.

**De-NEW-DL: Download dashboard data** [Simple]
- Story: As a Service Line team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids.

**US-DT-002: View Contract Type Mix** [Medium]
- Story: As a Service Line team member, I want to see the mix of Contract Types (T&M, Fixed Price, Transaction Based), so that I can assess the commercial risk profile of the forecast.
- AC: Given opportunities are tagged with contract types When I view the "Contract Type Mix" KPI card Then I should see a Pie Chart split by type And a legend indicating the percentage for each category (e.g., T&M 55%).
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-DT-001: View New vs Backfill Ratio** [Medium]
- Story: As a Service Line team member, I want to see the percentage split between "New Demand" and "Backfill", so that I know how much of our hiring is for growth versus replacing attrition.
- AC: Given forecast data exists When I view the "New Demand vs Backfill" KPI card Then I should see a Pie Chart visualizing the split And the exact percentages (e.g., New 65%, Backfill 35%) should be displayed next to the legend.

**De-NEW-NAV: View and use dashboard navigation tabs** [Simple]
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

**US-DT-003: View Demand Source Trend** [Medium]
- Story: As a Service Line team member, I want to view the "New vs Backfill" trend over time, so that I can identify seasonal spikes in attrition-based demand.
- AC: Given I am viewing the "New Demand vs Backfill" chart widget When the chart loads Then I should see a Stacked Bar Chart with months on the X-axis And each bar should be split into "Wins" (Cyan) and "Replacement" (Purple) segments proportional to volume.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-DT-004: View Billability Trend** [Medium]
- Story: As a Service Line team member, I want to see the "Billability Type" trend (BFD, BTB, BTM), so that I can forecast revenue versus non-billable overhead.
- AC: Given I am viewing the "Billability Type" chart widget When the chart loads Then I should see a Stacked Bar Chart And the legend should clearly distinguish between Billability codes (e.g., BFD - Billable For Delivery).
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-DT-005: Hover for Composition** [Medium]
- Story: As a Service Line team member, I want to hover over a stacked bar to see the specific volume of each segment, so that I don't have to guess the numbers.
- AC: Given I am hovering over the "June" bar in the Demand Source chart When I pause my mouse Then a tooltip should appear showing: Total: 250 Wins: 150 Replacement: 100.

---

### Forecast Dashboard (Location Mix)

**US-LM-004: Track Mix Evolution** []
- Story: As a Service Line team member, I want to see how the Onsite/Offshore mix changes month-over-month, so that I can verify if our "Shift Left" (move to offshore) initiatives are working.
- AC: Given I am viewing the "Onsite vs Offsite..." bar chart When the chart loads Then I should see grouped bars for each month And distinct colors for "Onsite vs Offsite" and "Offshore mix" to allow visual comparison of volume trends.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**Fo-NEW-GRID: View data grid matching chart data** []
- Story: As a Service Line team member, I want to view the data grid 
- AC: Given I am on a dashboard page
When I scroll to the bottom data grid section
Then the data grid should be visible
And the default sort order should be Largest to Lowest by Total Demand.

**US-LM-005: View Countrywise Demand** []
- Story: As a Service Line team member, I want to see the exact demand numbers for every country in a table, so that I can allocate specific hiring targets to regional recruitment teams.
- AC: Given I view the "Countrywise Demand" widget When the table loads Then I should see rows for every country with demand And columns for each month in the selected horizon (Jan, Feb, Mar).

**US-LM-008: Handle "Nearshore" Logic** []
- Story: As a Service Line team member, I want "Nearshore" locations (e.g., Poland for UK clients) to be tracked separately or grouped logically, so that they aren't incorrectly labeled as "Onsite".
- AC: Given a location is tagged as "Nearshore" When the "Onsite vs Offshore" chart renders Then Nearshore should either have its own segment OR be grouped into "Offshore/Remote" based on system configuration And not be counted as "Onsite".

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Service Line team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids.

**US-LM-001: View Onsite vs Offshore Ratio** []
- Story: As a Service Line team member, I want to see the "Onsite vs Offshore" percentage split, so that I can ensure we are meeting our margin targets by maximizing offshore leverage.
- AC: Given forecast data is tagged with location types When I view the "Onsite vs Offshore" KPI card Then I should see a Pie Chart visualizing the split And the exact percentages (e.g., Offshore 68%) should be displayed.

**US-LM-002: View Top Delivery Countries** []
- Story: As a Service Line team member, I want to see the top contributing countries for delivery, so that I know where our biggest talent hubs are located.
- AC: Given data is aggregated by country When I view the "Top Countries for Delivery" card Then I should see a list of countries sorted by volume share (e.g., US - 38%) And it should display at least the top 4 locations.

**Fo-NEW-NAV: View and use dashboard navigation tabs** []
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

---

### Forecast Dashboard (Skill Mix)

**US-SM-006: View Short Fuse Heatmap** []
- Story: As a Service Line team member, I want to visualize urgent "Short Fuse" demand in a heatmap, so that I can instantly spot the months and skills with the highest pressure.
- AC: Given demand is flagged as "Short Fuse" (Urgent) When I view the "Short Fuse Demand" widget Then I should see a grid of Skills (Rows) vs Months (Columns) And cells should be colored Dark Blue for high volume (>200) and Light Blue for low volume (<50).
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-SM-005: Track Volatility Over Time** []
- Story: As a Service Line team member, I want to track how "Stable" vs "Volatile" demand changes over months, so that I can predict when we might need to surge our contractor workforce.
- AC: Given I am viewing the "% Stable & Volatile Demand Clusters" chart When the line chart renders Then I should see two distinct lines (Cyan for Stable, Blue for Volatile) And the Y-axis should represent the total FTE count or percentage share.

**US-SM-008: View SkillTable** [xxx]
- Story: As a Service Line team member, I want to see a detailed table of skills (implied below the fold), so that I can Download the list for training curriculum planning.
- AC: Given I scroll to the bottom of the page When the "Skill Details" table loads Then I should see columns for: leaf skills, Total Demand, Stability, and Short Fuse Count.
And the default sort order should be Largest to Lowest by Total Demand.

**Fo-NEW-GRID: View data grid in default sort** []
- Story: As a Service Line team member, I want to view the data grid with deafult sort
- AC: Given I am on a dashboard page
When I scroll to the bottom data grid section
Then the data grid should show the data
And the default sort order should be Largest to Lowest by Total Demand.

**US-SM-014: View Grade-Level Demand** []
- Story: As a Service Line team member, I want to see the "SO Grade" (e.g., A, SA, M) for each demand line, so that I can estimate the budget required based on seniority bands.
- AC: Given the data grid is loaded When I analyze the "SO Grade" column Then I should see the standardized grade code (e.g., 'SA' for Senior Associate) And be able to group/sort by this column to see total demand for specific grades.

**US-SM-017: Handle Multiple Leaf Skills** [xxx]
- Story: As a Service Line team member, I want the system to handle cases where one Cluster has multiple Leaf Skills, so that the data isn't duplicated incorrectly.
- AC: Given "Microsoft AD" cluster has both "Java" and ".Net" leaf skills When the table loads Then these should appear as separate rows And the "Cluster" name should be repeated (or grouped) for clarity.

**US-SM-011: View Leaf Skill Granularity** [xxx]
- Story: As a Service Line team member, I want to see the specific "Leaf Skill" (e.g., Java, Spring Boot) associated with a cluster, so that I can create accurate job requisitions.
- AC: Given I am viewing the "Demand for Skill Micro Cluster" table When I review the rows Then I should see a specific "Leaf Skill" column distinct from the "Cluster" column And it should show the precise technology required (e.g., "Java" vs just "Microsoft AD").

**US-SM-012: Quarterly Aggregation Logic** []
- Story: As a Service Line team member, I want the "Q 01" column to automatically sum the demand for Jan, Feb, and Mar, so that I don't have to manually calculate the quarterly total.
- AC: Given the table displays monthly columns (Jan, Feb, Mar) When the "Q 01" column renders Then the value in "Q 01" must equal the sum of [Jan + Feb + Mar] for that row And any updates to a monthly cell should instantly update the Q 01 total.

**US-SM-015: Horizontal Scroll for Future Horizons** []
- Story: As a Service Line team member, I want to scroll horizontally to view months that do not fit on the screen (e.g., Q2, Q3), so that I can see the long-term forecast.
- AC: Given the forecast horizon exceeds the screen width When I use the scroll bar at the bottom of the table Then the date columns (Apr, May, Jun...) should slide into view And the "Key Attribute" columns (Practice Area, Location, Cluster) should remain frozen (sticky) so I don't lose context.

**US-SM-004: Analyze Demand Drivers** []
- Story: As a Service Line team member, I want to expand the "Top Drivers" list, so that I can see why demand is increasing (e.g., is it "Attrition" or "New Deals"?).
- AC: Given I see the "Top Drivers" list (e.g., Increase Attrition Rate) When I click the "+" (plus) icon next to a driver Then the card should expand to show the specific volume or percentage attributed to that driver.

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Service Line team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids.

**US-SM-001: View Top Skills in Practice** []
- Story: As a Service Line team member, I want to see the top 5 "Skill Micro Clusters" driving demand within my practice, so that I know which technologies are generating the most business.
- AC: Given I have selected a "Practice Area" (e.g., Cloud) When I view the first KPI card Then I should see the top skills sorted by volume (e.g., Java API, Azure) And a percentage indicating their share of the practice total.

**US-SM-002: View Top Skills in Service Line** []
- Story: As a Service Line team member, I want to see top skills across the entire Service Line (SL), so that I can identify cross-practice trends.
- AC: Given I am viewing the dashboard When I view the "Top Skill Micro Clusters (In SL)" card Then the list should reflect the broader Service Line demand, distinct from the specific Practice Area filter if applicable.
And the X-axis grain (Year / Quarter / Month) should adjust based on the selected Forecast Horizon.

**US-SM-003: View Stable vs Volatile Ratio** []
- Story: As a Service Line team member, I want to see the "Stable vs Volatile" demand split, so that I can decide the right mix of permanent staff versus contractors.
- AC: Given forecast data is analyzed for variance When I view the "Stable vs Volatile" KPI card Then I should see a Pie Chart showing the percentage split (e.g., 70% Stable) And hovering should explain the threshold (e.g., "Volatile = >15% variance MoM").
And the default sort order should be Largest to Lowest by Total Demand.

**Fo-NEW-NAV: View and use dashboard navigation tabs** []
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

---

### Scenario Planning

**US-SP-001: Load Default Planning Context** []
- Story: As a Service Line team member, I want the Scenario Planning page to default to my primary Practice Area and current forecast cycle, so that I can start simulation without manual setup.
- AC: Given I belong to the "Cloud" Practice AreaWhen I navigate to the Scenario Planning moduleThen the "Practice Area" filter should default to "Cloud"And the "Forecast Horizon" should default to the current active planning cycle.

**US-SP-003: Filter by Skill Micro Cluster** []
- Story: As a Service Line team member, I want to filter the simulation by "Skill Micro Cluster", so that I can model specific talent supply chains (e.g., Java Full Stack).
- AC: Given I have selected a Practice AreaWhen I click the "Skill Micro Cluster" dropdownThen the list should be filtered to show only skills relevant to the selected Practice Area (Cascading Filter logic).

**US-SP-004: Apply Filters to Simulation** []
- Story: As a Service Line team member, I want the simulation data to refresh immediately when I change a filter, so that the "What-If" feedback loop is seamless.
- AC: Given I change the "Grade" filter from "All" to "Senior Associate"When the selection is confirmedThen the "Total Base" and "Scenario Adjusted" KPIs should recalculate and update within 2 seconds.

**US-SP-005: View Active Drivers** []
- Story: As a Service Line team member, I want to see the key business drivers (e.g., Attrition, Win Probability) currently influencing the model, so I know what levers I can pull.
- AC: Given a scenario is loadedWhen I view the "Top Drivers" panelThen I should see the list of enabled drivers with their current percentage values displayed clearly.

**US-SP-006: Adjust Driver Sliders** []
- Story: As a Service Line team member, I want to drag a slider to adjust a driver's percentage, so that I can intuitively simulate "best case" or "worst case" outcomes.
- AC: Given the "Attrition Rate" is currently 15%When I drag the slider to the rightThen the percentage value should increase dynamicallyAnd the "Scenario Adjusted" KPI on the right should update in real-time to reflect the impact.

**US-SP-010: Indication of Modified State** []
- Story: As a Service Line team member, I want to visually distinguish between the "Default" value and my "Modified" value, so I know which drivers I have touched.
- AC: Given the baseline Attrition Rate is 15%When I change it to 20%Then the slider track should show a visual marker (e.g., a tick mark or color change) indicating the original baseline position relative to my new selection.

**SP-NEW-001: Edit scenario levers** []
- Story: As a Service Line team member, I want to edit scenario levers, so that I can simulate the impact on demand volume.
- AC: Given I am viewing a scenario
When I click Edit and change one or more lever values
Then the scenario outputs should recalculate and show updated demand volume without navigating away.

**US-SP-011: View Baseline KPI** []
- Story: As a Service Line team member, I want to see the "Total Base" headcount before any adjustments, so that I have a clear reference point for the current plan.
- AC: Given a baseline forecast exists for the selected period When the page loads Then the "Total Base" tile should display the aggregate FTE count (e.g., 380)

**US-SP-012: View Scenario Impact KPI** []
- Story: As a Service Line team member, I want to see the "Scenario Adjusted" headcount, so that I immediately understand the quantitative result of my driver changes.
- AC: Given drivers have been applied When I view the "Scenario Adjusted" tile Then the value (e.g., 456) should reflect the calculation: Baseline + (Driver Impacts)

**US-SP-013: Calculate Net Change** []
- Story: As a Service Line team member, I want to see the exact "Net Change" difference, so that I don't have to manually calculate the gap between the Baseline and the New Plan.
- AC: Given both Baseline and Scenario values exist When the dashboard renders Then the "Net Change" tile should display the difference (e.g., 76) And the color should be Blue (if positive) or Red (if negative) to indicate growth vs shrinkage.

**US-SP-014: Compare Scenario vs Baseline Trend** []
- Story: As a Service Line team member, I want to see a line chart comparing the "Scenario" vs "Baseline" over time, so that I can identify when the divergence occurs (e.g., Q3 vs Q4).
- AC: When I look at the main chart area Then I should see two distinct lines: Cyan for "Baseline" and Blue for "Scenario" And the Y-axis should scale automatically to fit the data range.

**US-SP-015: Hover for Specific Data Points** []
- Story: As a Service Line team member, I want to hover over specific months on the chart, so that I can read the precise FTE values without looking at the table.
- AC: Given the chart is visible When I hover my mouse over a data point (e.g., June) Then a tooltip should appear showing the exact values for both Baseline and Scenario (e.g., Scenario: 8k, Baseline: 4.5k).

**US-SP-018: View Detailed Forecast Table** []
- Story: As a Service Line team member, I want to see the underlying data in a table format below the chart, so that I can validate the exact numbers for specific dates.
- AC: Given the chart is displayed When I scroll to the data grid Then I should see rows for "Scenario Forecast", "Baseline Forecast", and "Adjustment" And columns corresponding to the dates in the chart (e.g., 08/11/2023).

**US-SP-019: Auto-Calculate Adjustment Row** []
- Story: As a Service Line team member, I want the "Adjustment" row to automatically show the delta, so that I can see the monthly impact of my drivers.
- AC: Given the table is loaded When the system renders the "Adjustment" row Then each cell should calculate as: Scenario Value - Baseline Value And if the value is positive, it should include a "+" sign (e.g., +50).

**US-SP-020: Visual Delta Indicators** []
- Story: As a Service Line team member, I want the adjustment numbers to be color-coded, so that positive growth is distinguishable from negative attrition at a glance.
- AC: Given the adjustment value is calculated When the value is positive (Growth) Then the text should be Blue When the value is negative (Reduction) Then the text should be Red (or distinct neutral color depending on UI standards).

**US-SP-023: Scenario Pagination** []
- Story: As a Service Line team member, I want to page through different saved scenarios (e.g., 1 of 5), so that I can quickly compare different versions of the plan.
- AC: Given multiple scenarios exist for this context When I click the "Next" (>) arrow Then the page should reload with the data for "Scenario #2" And the pagination counter should update to "2 of 5".

**SP-NEW-002: Save scenario as new version** []
- Story: As a Service Line team member, I want to save my changes as a new scenario version, so that I can compare versions later.
- AC: Given I have changed one or more levers
When I click Save
Then a new version should be created with a version number and timestamp
And the previous version should remain unchanged.

**US-SP-021: Send scenario as input to Central Forecast Team** []
- Story: As a Service Line team member, I want to send my selected scenario as an input to the Central Forecast Team, so that it is considered in the next forecasting cycle.
- AC: Given I have a scenario version saved for the selected filters
When I click "Send as input"
Then the scenario status should change to "Sent"
And I should not be able to send another scenario for the same filter combination unless the sent scenario is withdrawn (out of scope for MVP).

**US-SP-017: Delete Scenario** []
- Story: As a Service Line team member, I want to delete obsolete scenarios, so that the planning workspace remains uncluttered.
- AC: Given I am the creator of the scenario OR have "Admin" rights When I click the "Trash" icon Then I should receive a "Confirm Deletion" prompt And confirming it should remove the scenario and revert to the default view.

---

### Forecast Feedback

**US-FF-011: Download Audit Report** []
- Story: As a Service Line team member, I want to download a full "Audit Report" of the feedback session, so that we have a record of who approved the forecast and what changes were made.
- AC: Given the session is complete When I click the "Audit Report" button Then a PDF/Excel file should download containing the Comparison Chart data, Scenario Inputs, Summary Table, and any User Feedback submitted.

**US-FF-010: Submit Qualitative Feedback** []
- Story: As a Service Line team member, I want to submit text-based feedback explaining the forecast context, so that the AI model can learn from these insights (RLHF).
- AC: Given I have analyzed the forecast When I type into the "Give your Feedback" text area and click "Submit" Then the comment should be saved and associated with this specific Forecast Cycle ID.

**US-FF-005: View Variance from Target** []
- Story: As a Service Line team member, I want to see the "Variance from Target" in the summary table, so that I can immediately spot if the current plan misses our headcount goals.
- AC: Given a Headcount (HC) Target exists (e.g., 2500) When I view the "Summary Table" Then the "Variance" column should calculate Forecast - Target And highlight the value in Red if the gap is negative/critical.

**US-FF-006: Monitor Forecast Accuracy** []
- Story: As a Service Line team member, I want to track the "Forecast Accuracy" score (e.g., 70%), so that I can assess the reliability of the underlying AI model.
- AC: Given the system compares past predictions to actuals When I view the "Forecast Accuracy" column Then I should see a percentage score representing the model's historical precision for this segment.

**US-FF-007: Track Cycle-over-Cycle Change** []
- Story: As a Service Line team member, I want to see "Variance from Last Cycle", so that I know if the forecast is stabilizing or becoming more volatile week-over-week.
- AC: Given the previous planning cycle data is stored When the table loads Then the "Variance from Last Cycle" column should display the percentage change (e.g., 14%).

**FF-NEW-002: Edit or delete skill microcluster update** []
- Story: As a Service Line team member, I want to edit or delete a draft skill microcluster update entry, so that I can correct mistakes before submitting.
- AC: Given I have one or more rows in the Skill Microcluster Updates list
When I choose Edit or Delete on a row
Then the row should be updated or removed from the list before submission.

**FF-NEW-001: Add new skill microcluster update** []
- Story: As a Service Line team member, I want to add a new skill microcluster update, so that I can capture changes needed in the taxonomy.
- AC: Given I am on the Forecast Feedback page
When I click "Add New Update"
Then an input row should appear to enter Type of Change (Add/Modify), Skill Micro Cluster, Old Leaf Skills, and Updated Leaf Skills
And when I press Enter the row should be added to the Skill Microcluster Updates list.

**FF-NEW-003: Submit forecast feedback** []
- Story: As a Service Line team member, I want to submit my feedback and updates, so that the Central Forecast Team can review and incorporate them.
- AC: Given I have entered feedback text and/or skill microcluster update rows
When I click Submit
Then the submission should be saved with my selected filters and timestamp
And I should see a confirmation message.

**US-FF-008: View Taxonomy Change Log** []
- Story: As a Service Line team member, I want to see a log of "Skill Microcluster Updates" used in this forecast, so that I can verify if recent taxonomy changes (e.g., adding "Generative AI") are reflected.
- AC: Given changes were made to the taxonomy When I view the "Skill Microcluster Updates" table Then I should see rows for "Newly Added" or "Modified" clusters And the "Updated Leaf Skills" column should show the specific changes.

**US-FF-009: Propose Taxonomy Update** []
- Story: As a Service Line team member, I want to propose a new Skill Microcluster directly from the feedback page, so that the forecast can be corrected for missing skills in the next cycle.
- AC: Given I identify a missing skill gap When I click "Add New Update" Then a modal should open allowing me to define the "Cluster Name" and "Leaf Skills" And submitting it should queue a request for the Master Data team.

**US-FF-003: Trace Forecast to Scenario ID** []
- Story: As a Service Line team member, I want to see which specific "Scenario ID" drove the management overlay, so that adjustments are traceable to a specific planning session.
- AC: Given adjustments have been made When I view the "Scenario Planning Inputs" table Then I should see the "Scenario ID" (e.g., 255104) listed for each input variable.

**US-FF-004: View Input Variable Impact** []
- Story: As a Service Line team member, I want to see the "% Impact" of each variable (e.g., Client Spend), so that I know which specific assumption is driving the forecast change.
- AC: Given I am analyzing Scenario 255104 When I look at the "Impact by %" column Then I should see the weighted contribution of that driver (e.g., Win Probability contributed 6% to the increase).

**US-FF-001: Compare System vs. Management Forecast** []
- Story: As a Service Line team member, I want to visualize the difference between the "System Generated Baseline" and the "Management Overlay," so that I can see how much we are overriding the AI's prediction.
- AC: Given a forecast horizon is selected When I view the "Forecast trend split" chart Then I should see a stacked bar chart And the "System Generated" portion (Cyan) should be distinct from the "Management Overlay" portion (Purple).

**US-FF-002: View Monthly Overlay Delta** []
- Story: As a Service Line team member, I want to hover over a bar to see the exact numeric delta, so that I can quantify the human adjustment.
- AC: Given the chart is visible When I hover over the "May" bar Then a tooltip should display: System: 150k Overlay: +50k Total: 200k.

---

### Skill Taxonomy

**US-TX-001: View Taxonomy Mapping** []
- Story: As a Service Line team member, I want to see which specific "Leaf Skills" make up a "Skill Microcluster", so that I can accurately explain the technical requirements to a candidate.
- AC: Given I am on the Taxonomy page When I look at the "Developer" row Then I should see columns listing all mapped skills (Core Java, Microservices, etc.) And empty columns should be blank if a cluster has fewer than 5 skills.
And Skill Micro Cluster Name values should be combinations such as "java-javascript-sql" or "java-javascript-springboot" (not role labels).

**US-TX-003: Sort by Microcluster Name** []
- Story: As a Service Line team member, I want to sort the table alphabetically by "Microcluster Name", so that I can quickly find a specific role definition.
- AC: Given the table is loaded When I click the "Skill Microcluster Name" header Then the rows should sort A-Z (Admin -> Analyst -> Consultant...).
And Skill Micro Cluster Name values should be combinations such as "java-javascript-sql" or "java-javascript-springboot" (not role labels).

**US-TX-013: Empty State for Leaf Skills** []
- Story: As a Service Line team member, I want to clearly see which columns are empty (e.g., Leaf Skill 4 & 5 are blank), so that I know where we have room to add more skills.
- AC: Given a cluster has only 2 skills defined When I view the row Then the columns for "Leaf Skill 3, 4, and 5" should render with a distinct "Empty" visual (e.g., a dash "-") rather than just whitespace.
And Skill Micro Cluster Name values should be combinations such as "java-javascript-sql" or "java-javascript-springboot" (not role labels).

**US-TX-007: Download Taxonomy Reference** []
- Story: As a Service Line team member, I want to Download this list to Excel, so that I can map our training curriculum to these standard definitions.
- AC: Given I need to share definitions offline When I click the global "Download" icon Then an Excel file should be generated mirroring the table structure exactly.


**US-TX-011: Pagination Controls** []
- Story: As a Service Line team member, I want to page through the list (e.g., Rows 1-50, 51-100), so that the page loads quickly even if we have thousands of microclusters.
- AC: Given there are more records than the default view limit (e.g., 20) When I scroll to the bottom Then I should see standard pagination controls (Next, Previous, Page Numbers).


**US-TX-004: Read-Only Access for Planners** []
- Story: As a Service Line team member, I want read-only access to this table, so that I can reference definitions without accidentally changing the corporate standard.
- AC: I view this page Then I should see the data grid But I should NOT see any "Edit", "Add New", or "Delete" buttons.

---

## Persona: Markets

### Forecast Dashboard (Business Unit)

**US-BU-009: View BU Detail Table** []
- Story: As a Service Line team member, I want to see a detailed table below the charts showing specific demand metrics for each Business Unit, so that I can validate the chart data.
- AC: Given I scroll down the page When the table loads Then I should see columns for: Business Unit, Month (Jan, Feb...), Total Demand, and YoY Growth %.
And the default sort order should be Largest to Lowest by Total Demand.

**Fo-NEW-GRID: Default sort in data grid** []
- Story: As a Service Line team member, I want to view the data grid sor by total demand as default
- AC: Given I am on a dashboard page
When I scroll to the bottom data grid section, the default sort order should be Largest to Lowest by Total Demand.

**US-BU-013: Calculate Market Share %** []
- Story: As a Service Line team member, I want to see the "Market Share" column in the grid, so that I know what percentage of the total Practice demand each BU represents.
- AC: Given the table loads When I view the "Share %" column Then the value should be calculated as: (BU Total Demand / Total Practice Demand) * 100.

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Service Line team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids.

**US-BU-001: Filter by Business Unit** []
- Story: As a Service Line team member, I want to filter the dashboard by specific "Business Units" (e.g., Retail NA), so that I can isolate my specific portfolio from the wider Practice data.
- AC: Given the "Business Unit" dropdown is available When I select "Retail NA" Then the dashboard should refresh to show data only for that BU 

**US-BU-002: View Top BUs by Demand** []
- Story: As a Service Line team member, I want to see a ranked list of the top 3 Business Units by total demand volume, so that I know which verticals are my biggest consumers of talent.
- AC: Given multiple BUs have forecast demand When I view the "Top BUs by demand" card Then I should see the top 3 BUs sorted descending by volume And the percentage share of total demand (e.g., "Retail NA - 30%") should be displayed.

**US-BU-003: View Top BUs by Growth** []
- Story: As a Service Line team member, I want to see which Business Units have the highest Year-over-Year (YoY) growth, so that I can prioritize investment in high-velocity sectors.
- AC: Given historical data exists for comparison When I view the "Top BUs by growth rate" card Then I should see the top 3 BUs sorted by % growth And the YoY indicator (e.g., "10% yoy") should be clearly visible next to the BU name.

**Fo-NEW-NAV: View and use dashboard navigation tabs** []
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

**US-BU-004: View BU Wise Demand Stack** []
- Story: As a Service Line team member, I want to see a stacked bar chart of demand over time, split by Business Unit, so that I can visualize the changing portfolio mix.
- AC: Given I am viewing the "BU wise demand" widget When the chart loads Then each month's bar should be stacked with segments for "BU1", "BU2", "BU3" And the total height of the bar should represent the aggregate demand of the selected BUs.

**US-BU-005: View BU Wise Growth Curves** []
- Story: As a Service Line team member, I want to compare the growth trajectories of different BUs on a single multi-line chart, so that I can spot if a specific unit is crashing while others are growing.
- AC: Given I am viewing the "BU wise growth rates" widget When the chart loads Then I should see distinct lines for each top BU And the Y-axis should represent growth percentage (allowing for negative values if a BU is shrinking).

**US-BU-008: Delivery Location Context** []
- Story: As a Service Line team member, I want to see how my BU is performing in specific delivery locations (e.g., Bangalore vs. Pune), so that I can balance my offshore supply chain.
- AC: Given I have selected "Retail NA" as the BU When I change the "Delivery Location" filter to "Bangalore" Then the "BU wise demand" chart should update to show only the Retail demand fulfilled from Bangalore.

**US-BU-009: View BU Detail Table** []
- Story: As a Service Line team member, I want to see a detailed table below the charts showing specific demand metrics for each Business Unit, so that I can validate the chart data.
- AC: Given I scroll down the page When the table loads Then I should see columns for: Business Unit, Month (Jan, Feb...), Total Demand, and YoY Growth %. And the "Total" row should sum all visible BUs.
And the default sort order should be Largest to Lowest by Total Demand.

**Fo-NEW-GRID: View data grid by default sort** []
- Story: As a Service Line team member, I want to view the data in he default sort
- AC: Given I am on a dashboard page
When I scroll to the bottom data grid section, the data should be in default sort order should be Largest to Lowest by Total Demand.

**US-BU-013: Calculate Market Share %** []
- Story: As a Service Line team member, I want to see the "Market Share" column in the grid, so that I know what percentage of the total Practice demand each BU represents.
- AC: Given the table loads When I view the "Share %" column Then the value should be calculated as: (BU Total Demand / Total Practice Demand) * 100.

**US-BU-007: Aggregation of "Others"** []
- Story: As a Service Line team member, I want smaller Business Units to be grouped into an "Others" category if there are too many to display, so that the chart remains readable.
- AC: Given there are more than 5 active Business Units When the charts render Then the top 4 BUs should have distinct colors And the remaining BUs should be summed into a single grey category labeled "Others".

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Markets team member, I want to download the dashboard data.
- AC: Given I am on a dashboard page
When I click the Download icon
Then a file should be downloaded containing the data grids for the current filters.

**US-BU-001: Filter by Business Unit** []
- Story: As a Service Line team member, I want to filter the dashboard by specific "Business Units" (e.g., Retail NA), so that I can isolate my specific portfolio from the wider Practice data.
- AC: Given the "Business Unit" dropdown is available When I select "Retail NA" Then the dashboard should refresh to show data only for that BU And the "Top BUs" widgets should likely switch to showing sub-units or remain static context (depending on hierarchy).

**US-BU-002: View Top BUs by Demand** []
- Story: As a Service Line team member, I want to see a ranked list of the top 3 Business Units by total demand volume, so that I know which verticals are my biggest consumers of talent.
- AC: Given multiple BUs have forecast demand When I view the "Top BUs by demand" card Then I should see the top 3 BUs sorted descending by volume And the percentage share of total demand (e.g., "Retail NA - 30%") should be displayed.

**US-BU-003: View Top BUs by Growth** []
- Story: As a Service Line team member, I want to see which Business Units have the highest Year-over-Year (YoY) growth, so that I can prioritize investment in high-velocity sectors.
- AC: Given historical data exists for comparison When I view the "Top BUs by growth rate" card Then I should see the top 3 BUs sorted by % growth And the YoY indicator (e.g., "10% yoy") should be clearly visible next to the BU name.

**Fo-NEW-NAV: View and use dashboard navigation tabs** []
- Story: As a Service Line team member, I want to use the dashboard navigation tabs, so that I can switch between Forecast Overview, Demand Type Breakdown, Business Unit, Location Mix, and Skill Mix.
- AC: Given I am on the Forecast Dashboard module
When I click a dashboard tab
Then the selected dashboard page should load.

---

### Home Page

**US-001: Enterprise Login** []
- Story: As a Market Team member, I want to log into the application using my enterprise credentials, so that I don’t need to manage a separate password.
- AC: Given I have valid enterprise credentials; When I attempt to log in; Then I should be granted access without entering a separate app password.

**US-002: Route to Market Homepage** []
- Story: As a Market Team member, I want to be redirected to my Market Team homepage after login, so that I can immediately begin work in my scope.
- AC: Given I successfully authenticate; When the login completes; Then I am taken to my Market Team home page.

**US-003: Load Context (BU + Practice)** []
- Story: As a Market Team member, I want the homepage to automatically load my assigned BU, Practice Area, and time horizon, so that I see relevant info immediately.
- AC: Given I have assigned roles; When I land on the home page; Then my assigned BU, Practice Area, and next-12-months view should already be applied.

**US-004: View Access Restricted to My Scope** []
- Story: As a Market Team member, I want my access to be limited to my BU and Practice, so that data remains secure and aligned to my role.
- AC: Given I am logged into the system; When I attempt to view data; Then I see only data mapped to my assigned Business Unit(s) and Practice Areas.

**US-005: Session Timeout & Re-Authentication** []
- Story: As a Market Team member, I want the system to log me out after inactivity and require re-authentication, so that security policies are upheld.
- AC: Given I am logged in; When my session expires; Then I should be logged out and prompted to sign in again.

**US-006: Missing Roles Error** []
- Story: As a user without Market mapping, I want a clear error message, so that I know why I cannot proceed.
- AC: Given I log in without a mapped BU or Practice; When the system loads; Then I should see an error informing me that an admin must assign access.

**US-007: View Overview + KPIs** []
- Story: As a Market Team member, I want to see key KPIs and demand summary immediately on login, so that I can quickly understand my area’s status.
- AC: Given I access my Market homepage; When the page loads; Then I should see KPI tiles and overview data relevant to my BU/Practice.

---

### Home Overview Panel

**US-008: View KPI Tiles at a Glance** []
- Story: As a Market Team Lead, I want to see key KPI tiles summarizing demand and fulfillment, so that I can immediately assess operational status.
- AC: Given I am on the home page; When the page loads; Then I should see KPI tiles showing total demand, gap %, practice areas, clusters, and onsite ratio.

**US-009: View Forecast Trend Line Chart** []
- Story: As a Market Team Lead, I want to view a forecast demand trend line, so that I can understand how demand is shaping over time.
- AC: Given I have forecast data; When the trend graph loads; Then I should see a 12-month line graph showing forecast FTE for the selected BU/Practice.

**US-010: View Short-Fuse Heatmap** []
- Story: As a Market Team Lead, I want to see short fuse demand across skill clusters in a heatmap, so that I can spot resourcing risk areas quickly.
- AC: Given short fuse demand exists; When I view the heatmap; Then I should see intensity by month and skill cluster for the active BU/Practice.

**US-011: View % Change in Short Fuse Demand** []
- Story: As a Market Team Lead, I want a metric that compares short fuse demand to a previous period.
- AC: Given comparable period demand exists; When the KPI loads; Then I should see a % change trend relative to my BU/Practice.

**US-012: RBAC-Aligned Data Visibility** []
- Story: As a Market Team Lead, I want to see only data for BUs/Practices I own, so that confidentiality is preserved.
- AC: Given I am logged in; When dashboards load; Then I should only see data related to my assigned scope.

**US-013: No Data State** []
- Story: As a Market Team Lead, I want the system to clearly indicate when no data is available for a selected Practice.
- AC: Given no short fuse demand for the selected Practice; When I view the heatmap; Then I should see a "No data" message.

**US-014: Last Refreshed Timestamp** []
- Story: As a Market Team Lead, I want to see when the data was last updated, so that I know how current the insights are.
- AC: Given the data has a refresh timestamp; When I view the overview section; Then I should see the last refresh date and time displayed.

**US-015: Navigate to Tasks & Approvals** []
- Story: As a Market Team Lead, I want to quickly notice pending tasks, so that I can take action without navigating multiple menus.
- AC: Given actionable items; When I load the home page; Then I should see a “My Tasks & Approvals” section with counts for my Practice.

---

### Home- Tasks & Interlocks

**US-016: View Tasks & Approvals List** []
- Story: As a Market Team Lead, I want to see all pending approval tasks for my Practice, so that I immediately understand what needs attention.
- AC: Given I am a logged-in Lead; When I open the home page; Then I should see a task list relevant to my BU/Practice.

**US-017: Understand Task Attributes** []
- Story: As a Market Team Lead, I want each task to display key information (ID, type, description, due date), so that I can prioritize what to review.
- AC: Given I have tasks assigned; When I view the list; Then each row should show task ID, type, description, due date, and status.

**US-018: Identify Status and Overdue Items** []
- Story: As a Market Team Lead, I want clear task status indicators, so that overdue or urgent items stand out.
- AC: Given today's date; When a task is past its due date; Then the task should display an overdue status and visual warning.

**US-019: Sort Tasks** []
- Story: As a Market Team Lead, I want default sort on the task table
- AC: Given tasks are displayed; When I click a header; Then the table should reorder based on that column due date from latest to oldest

**US-020: Navigate to Task Detail Page** []
- Story: As a Market Team Lead, I want to click a task to open its details with my BU/Practice context already applied.
- AC: Given a task is shown; When I click “view task”; Then I should be taken to the detail screen with context preserved.

**US-021: RBAC-Constrained Visibility** []
- Story: As a Market Team Lead, I want to see only tasks relevant to my BU/Practice, so that confidentiality is preserved.
- AC: Given RBAC permissions; When the list loads; Then I should only see tasks for my assigned scope.

**US-022: Empty State When No Tasks** []
- Story: As a Market Team Lead, I want the system to clearly indicate when there are no pending tasks.
- AC: Given no tasks assigned; When I load the panel; Then I should see a message: “No pending tasks”.

---

### Home Alerts

**US-024: View Alerts Panel** []
- Story: As a Market Team Lead, I want to see an alert panel on my home page for important changes in my Practice.
- AC: Given I am on the home page; When alert conditions exist; Then an alerts section should appear.

**US-025: View Alert Types and Context** []
- Story: As a Market Team Lead, I want each alert to show the details (type, impacted area), so that I understand at a glance what shifted.
- AC: Given alerts are visible; When I review a row; Then I should see alert type, impacted area, and relevant time period.

**US-026: See No Alerts Message** []
- Story: As a Market Team Lead, I want the system to tell me when no alerts exist for my Practice.
- AC: Given no changes meet thresholds; When I view the home page; Then the alert section should display “No alerts.”

**US-027: Get Forecast Change Alerts** []
- Story: As a Market Team Lead, I want to be notified when forecast demand for my Practice materially changes.
- AC: Given forecast exceeds thresholds; When detected; Then an alert should display showing % change and area.

**US-028: Get Market Signal Alerts** []
- Story: As a Market Team Lead, I want alerts when relevant market indicators change for my specific BU.
- AC: Given market signals are updated; When update crosses alert logic; Then I should see a market signal alert.

**US-029: Get Skill Taxonomy Change Alerts** []
- Story: As a Market Team Lead, I want alerts when skill clusters in my Practice are redefined.
- AC: Given taxonomy definitions change; When detected; Then an alert should indicate the impacted areas and what changed.

---

### Home Navigation

**US-030: Access Application Navigation** []
- Story: As a Market Team Lead, I want a persistent navigation menu, so that I can move across workstreams consistently.
- AC: Given I am logged in; When I move between pages; Then I should always see the left side menu with fixed tabs.

**US-031: Highlight Active Page** []
- Story: As a Market Team Lead, I want the active page to be highlighted, so that I always know where I am.
- AC: Given I am on any screen; When navigation menu loads; Then the menu item for current page should be highlighted.

**US-032: Preserve Context When Navigating** []
- Story: As a Market Team Lead, I want my selected BU, Practice, and horizon filters to persist when I move between pages.
- AC: Given I have selected a Practice; When I click a navigation link; Then the selected context remains applied.

**US-033: Retain Context on Browser Refresh** []
- Story: As a Market Team Lead, I want my working context (BU/Practice) to remain unchanged if I refresh the browser.
- AC: Given I am viewing a planning view; When I refresh; Then filters should still be applied without re-selection.

---

### Forecast Dashboard

**US-FD-014: View Precise Values on Hover** []
- Story: As a Market Team member, I want to see exact FTE count and Growth % when I hover over a month/week.
- AC: Given trend charts; When I hover over a bar; Then a tooltip should display Period, FTE Count, and Growth Rate.

**US-FD-017: Synchronize Charts with Global Filters** []
- Story: As a Market Team member, I want all charts to update instantly when I change BU, Practice, or Location.
- AC: Given I change a global filter; When dashboard refreshes; Then ALL charts redraw simultaneously for the new context.

**Fo-NEW-GRID: Matching Data Grid** []
- Story: As a Market Team member, I want to view a data grid that includes BU and Practice columns to validate visual numbers.
- AC: Given I am on a dashboard page; When I scroll to the bottom; Then grid shows BU, Practice, and cluster values.

**US-FD-015: Calculate Trend Signal** []
- Story: As a Market Team member, I want the "Growth Rate" line to accurately reflect the change against the previous cycle.
- AC: Given current and previous forecast cycles; When chart renders; Then calculate [(Curr−Prev)/Prev].

**US-FD-018: Chart Empty State** []
- Story: As a Market Team member, I want to know if there is no data for a specific Practice view.
- AC: Given selected filters have no demand; When chart loads; Then display "No Forecast Data Available".

**US-FD-026: Global API Failure** []
- Story: As a Market Team member, I want the system to handle data load failures gracefully.
- AC: Given API returns 500; When page attempts to load; Then failing widget shows "Retry"; And show toast message.

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Market Team member, I want to download the dashboard data with BU/Practice headers for offline use.
- AC: Given I am on a dashboard page; When I click Download; Then a file is downloaded containing grids.

**US-FD-003: Filter by BU & Practice** []
- Story: As a Market Team member, I want to change the BU, Practice, or time horizon to reflect my planning period.
- AC: Given the filter bar; When I select a Practice; Then KPIs and charts redraw to reflect that specific scope.

**US-FD-004: Cascading Practice Filter Logic** []
- Story: As a Market Team member, I want the "Skill Micro Cluster" options to filter based on my selected Practice Area.
- AC: Given I have selected a Practice Area; When I click the Skill dropdown; Then I should only see skills related to that Practice.

**US-FD-001: Default Dashboard Load** []
- Story: As a Market Team member, I want the "Forecast Overview" tab to load by default with my primary BU and Practice selected.
- AC: Given I am a mapped user; When I navigate to Forecast Dashboard; Then Overview tab is active with filters auto-selected.

**US-FD-010: View Explainability Summary** []
- Story: As a Market Team member, I want to see a text summary explaining the "Why" behind the Practice demand numbers.
- AC: Given AI analysis; When I look at the fourth panel; Then show a natural language sentence explaining drivers.

**US-FD-006: View KPI Definition Tooltip** []
- Story: As a Market Team member, I want to see the definition of "Forecast Demand" and its data sources.
- AC: Given I hover over the "i" icon; Then a tooltip appears defining the calculation logic and data sources.

**US-FD-005: View Total Forecast Demand** []
- Story: As a Market Team member, I want to see the total "Forecast Demand" for the selected BU/Practice.
- AC: Given data exists; When I view the first KPI card; Then show total aggregated FTE count for that scope.

**US-FD-007: View Demand Growth Rates** []
- Story: As a Market Team member, I want to see growth rates (QoQ, MoM, WoW) for my Practice.
- AC: Given historical data; When viewing Growth Rate card; Then see three distinct metrics: QoQ, MoM, WoW.

**US-FD-009: View Cancellation Rate** []
- Story: As a Market Team member, I want to track the cancellation rate for my Practice to assess signal reliability.
- AC: Given recorded data; When I view the third KPI card; Then show % demand cancelled with risk indicator.

**US-FD-011: View Monthly Demand vs Growth** []
- Story: As a Market Team member, I want to view Practice demand and growth rate on a monthly basis.
- AC: Given data exists; When I view the Monthly widget; Then show dual-axis chart (Cyan bars, Grey line).

**US-FD-013: View Quarterly Strategic View** []
- Story: As a Market Team member, I want to view demand aggregated by Quarter to align with fiscal cycles.
- AC: Given Quarterly widget; Then bars show sum of FTEs for that quarter; And line shows QoQ growth %.

**Fo-NEW-NAV: Use dashboard navigation tabs** []
- Story: As a Market Team member, I want to switch between Overview, Breakdown, Location, and Skill Mix.
- AC: Given I am in the module; When I click a tab; Then the selected dashboard page should load.

**US-FD-002: Switch Dashboard Tabs** []
- Story: As a Market Team member, I want to investigate demand for my Practice from different angles (e.g. Skill Mix).
- AC: Given Overview page; When I click "Skill Mix"; Then content area refreshes with skill-specific charts.

**US-FD-012: View Weekly Demand Granularity** []
- Story: As a Market Team member, I want to view the forecast breakdown by week (W1-W6) for short-term deployment.
- AC: Given Weekly widget; Then X-axis displays work weeks; And bars show volume for that specific week.

---

### Demand Breakdown

**US-DT-007: View Demand Source Table** []
- Story: As a Market Team member, I want to see detailed demand breakdown by BU, Practice, Location, and Grade.
- AC: Given grid loads; Then show columns for BU, Practice, Location, Cluster, and Demand Type.

**US-DT-011: Sort Data Grid** []
- Story: As a Market Team member, I want to sort the table by "Location" or "Demand Type" within my Practice.
- AC: Given table is loaded; When I click a header; Then rows reorder based on that selection.

**US-DT-008: Quarterly Aggregation** []
- Story: As a Market Team member, I want to see Quarterly totals (Q1, Q2) auto-calculated for my Practice.
- AC: Given monthly data; Then "Q 01" column must be the SUM of Jan + Feb + Mar for that row.

**US-DT-009: View Billability Detail** []
- Story: As a Market Team member, I want to see a table for "Billability Type" (BFD/BTB) for my Practice.
- AC: Given I scroll to the table; When grid loads; Then show Billability Type column; And sort by type.

**US-DT-010: Horizontal Scroll** []
- Story: As a Market Team member, I want to scroll horizontally while keeping BU/Practice columns frozen.
- AC: Given columns exceed width; When using scrollbar; Then dimension columns (BU, Practice) remain sticky.

**US-DT-016: Hide Commercial Data** []
- Story: As a Market Team member, I want to see headcount but NOT "Billability Type" if not authorized.
- AC: Given no "Commercial" permissions; When page loads; Then Pie Chart and Billability table are hidden.

**De-NEW-DL: Download dashboard data** []
- Story: As a Market Team member, I want to download breakdown data
- AC: Given I am on a dashboard page; When I click Download; Then file is downloaded.

**US-DT-012: Download Breakdown Data** []
- Story: As a Market Team member, I want to Download tables to Excel including "Demand Source" and "Billability" sheets.
- AC: Given Demand Type tab; When I click Download; Then file includes specific sheets for breakdown categories.

**De-NEW-FLT: Filter dashboard data** []
- Story: As a Market Team member, I want to filter the breakdown page by BU, Practice, Grade, and Skill.
- AC: Given the Breakdown page; When I select values; Then all tiles, charts, and grids refresh for that scope.

**US-DT-002: View Contract Type Mix** []
- Story: As a Market Team member, I want to see the mix of Contract Types for my BU/Practice.
- AC: Given data exists; When I view KPI card; Then show Pie Chart split by contract type with percentages.

**US-DT-001: View New vs Backfill Ratio** []
- Story: As a Market Team member, I want to see the split between "New Demand" and "Backfill" for my Practice.
- AC: Given forecast data; When viewing KPI; Then show Pie Chart and exact percentages next to legend.

**De-NEW-NAV: Use dashboard navigation tabs** []
- Story: As a Market Team member, I want to use tabs to switch between Overview, Breakdown, BU, and Location.
- AC: Given Forecast module; When I click a tab; Then the selected dashboard page should load.

**US-DT-003: View Demand Source Trend** []
- Story: As a Market Team member, I want to view "New vs Backfill" trend over time for the Practice.
- AC: Given widget loads; Then show Stacked Bar Chart with Wins (Cyan) and Replacement (Purple).

**US-DT-004: View Billability Trend** []
- Story: As a Market Team member, I want to see the "Billability Type" trend (BFD, BTB, BTM).
- AC: Given widget loads; Then show Stacked Bar Chart with legend for billability codes.

**US-DT-005: Hover for Composition** []
- Story: As a Market Team member, I want to hover over a bar to see the specific segment volumes for my Practice.
- AC: Given I hover over a monthly bar; When I pause; Then tooltip shows Total, Wins, and Replacement volumes.

---

### Location Mix

**US-LM-004: Track Mix Evolution** []
- Story: As a Market Team member, I want to see how Onsite/Offshore mix changes month-over-month for my Practice.
- AC: Given bar chart loads; Then show grouped bars for each month with distinct colors for Onsite vs Offshore.

**Fo-NEW-GRID: View data grid matching chart data** []
- Story: As a Market Team member, I want to view the data grid matching the location visuals for my BU/Practice.
- AC: Given dashboard page; When scrolling to bottom; Then show grid with same metric values as charts.

**US-LM-005: View Countrywise Demand** []
- Story: As a Market Team member, I want to see exact demand numbers for every country in the Practice.
- AC: Given widget loads; Then show rows for every country; And columns for each month in the horizon.

**US-LM-008: Handle "Nearshore" Logic** []
- Story: As a Market Team member, I want "Nearshore" locations tracked separately from "Onsite" and "Offshore".
- AC: Given Nearshore tags; When chart renders; Then Nearshore has its own segment OR grouped into "Remote".

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Market Team member, I want to download location dashboard data with BU/Practice headers.
- AC: Given dashboard page; When clicking Download; Then file includes data grids for current filtered scope.

**US-LM-009: Filter by Cost Type** []
- Story: As a Market Team member, I want to filter the location dashboard to show only "High Cost" locations.
- AC: Given filters available; When I select "High Cost"; Then all charts update to exclude Low Cost locations.

**US-LM-003: View Cost Impact Mix** []
- Story: As a Market Team member, I want to see the split between High and Low Cost locations for my Practice.
- AC: Given Cost Tier mapping; When viewing card; Then show Pie Chart of High vs Low Cost ratio.

**US-LM-001: View Onsite vs Offshore Ratio** []
- Story: As a Market Team member, I want to see the "Onsite vs Offshore" percentage split to ensure margin targets.
- AC: Given data tagged with location; When viewing card; Then show Pie Chart with offshore percentages.

**US-LM-002: View Top Delivery Countries** []
- Story: As a Market Team member, I want to see top contributing countries to know talent hub locations for my Practice.
- AC: Given aggregated data; When viewing card; Then show list of countries sorted by volume share.

**Fo-NEW-NAV: Use dashboard navigation tabs** []
- Story: As a Market Team member, I want to use tabs to switch between Overview and Location Mix.
- AC: Given Forecast module; When I click a tab; Then the selected dashboard page should load.

---

### Skill Mix

**US-SM-006: View Short Fuse Heatmap** []
- Story: As a Market Team member, I want to visualize urgent "Short Fuse" demand in a heatmap for the Practice.
- AC: Given Short Fuse data; When viewing heatmap; Then show grid of Skills vs Months with intensity colors.

**US-SM-005: Track Volatility Over Time** []
- Story: As a Market Team member, I want to track how "Stable" vs "Volatile" demand changes for my Practice.
- AC: Given chart renders; Then show Cyan (Stable) and Blue (Volatile) lines; And Y-axis shows FTE count.

**US-SM-008: View Skill Gap Table** []
- Story: As a Market Team member, I want to see a detailed table of skills for the Practice for training planning.
- AC: Given scroll to bottom; When grid loads; Then show Cluster, Demand, Stability, and Short Fuse Count.

**Fo-NEW-GRID: View data grid matching chart data** []
- Story: As a Market Team member, I want to view the data grid matching the skill visuals for the BU/Practice.
- AC: Given dashboard page; When scrolling to bottom; Then show grid with same metric values as charts.

**US-SM-014: View Grade-Level Demand** []
- Story: As a Market Team member, I want to see the seniority bands (SO Grade) for my Practice demand.
- AC: Given grid is loaded; When analyzing Grade column; Then show codes (A, SA, M); And allow grouping.

**US-SM-017: Handle Multiple Leaf Skills** []
- Story: As a Market Team member, I want the system to handle clusters with multiple leaf skills without duplication.
- AC: Given cluster has multiple skills; When table loads; Then show separate rows for each leaf skill.

**US-SM-011: View Leaf Skill Granularity** []
- Story: As a Market Team member, I want to see the precise "Leaf Skill" associated with a cluster for job reqs.
- AC: Given table view; When reviewing rows; Then show "Leaf Skill" column distinct from "Cluster" column.

**US-SM-012: Quarterly Aggregation Logic** []
- Story: As a Market Team member, I want the "Q 01" column to auto-sum Jan, Feb, and Mar for my Practice.
- AC: Given monthly columns; Then "Q 01" must equal SUM of monthly values for that row.

**US-SM-015: Horizontal Scroll for Future** []
- Story: As a Market Team member, I want to scroll horizontally to view long-term forecast months while keeping context.
- AC: Given horizon exceeds width; When scrolling; Then future months slide into view while BU/Practice remain sticky.

**US-SM-013: Identify Volatile Roles** []
- Story: As a Market Team member, I want to identify "Volatile" rows to route to external staffing agencies.
- AC: Given table view; When filtering by "Demand Type"; Then I can isolate "Volatile" labels and Download.

**US-SM-004: Analyze Demand Drivers** []
- Story: As a Market Team member, I want to see the volume vs attrition causes for my Practice demand.
- AC: Given Top Drivers list; When I click "+"; Then card expands to show volume or % share per driver.

**Fo-NEW-DL: Download dashboard data** []
- Story: As a Market Team member, I want to download skill mix data with BU/Practice headers.
- AC: Given dashboard page; When clicking Download; Then file is downloaded containing filtered data grids.

**US-SM-001: View Top Skills in Practice** []
- Story: As a Market Team member, I want to see the top 5 "Skill Micro Clusters" driving demand in my Practice.
- AC: Given Practice selected; When viewing first KPI card; Then show top skills sorted by volume and their share.

**US-SM-002: View Top Skills in Market** []
- Story: As a Market Team member, I want to see top skills across the entire Business Unit (BU).
- AC: Given dashboard view; When viewing "Top Clusters (In BU)" card; Then show broader BU demand.

**US-SM-003: View Stable vs Volatile Ratio** []
- Story: As a Market Team member, I want to see the "Stable vs Volatile" demand split for staff mix decisions.
- AC: Given data variance analyzed; When viewing card; Then show Pie Chart with % split.

**Fo-NEW-NAV: Use dashboard navigation tabs** []
- Story: As a Market Team member, I want to use navigation tabs to switch between Overview and Skill Mix.
- AC: Given Forecast module; When I click a tab; Then the selected dashboard page should load.

---

### Scenario Planning

**US-SP-001: Load Default Planning Context** []
- Story: As a Market Team member, I want Scenario Planning to default to my primary BU and Practice Area.
- AC: Given I am a mapped user; When I navigate to Scenario module; Then BU and Practice filters are auto-selected.

**US-SP-003: Filter by Skill Micro Cluster** []
- Story: As a Market Team member, I want to filter simulation by Skill Cluster within my Practice.
- AC: Given Practice selected; When I click Skill dropdown; Then list shows only skills relevant to that scope.

**US-SP-004: Apply Practice-Level Simulations** []
- Story: As a Market Team member, I want simulation data to refresh immediately when I change the Practice filter.
- AC: Given I change the Practice filter; When confirmed; Then Base and Adjusted KPIs update within 2 seconds.

**US-SP-005: View Active Drivers** []
- Story: As a Market Team member, I want to see the key business drivers influencing my Practice model.
- AC: Given scenario is loaded; When viewing "Top Drivers" panel; Then show list of enabled drivers for the Practice.

**US-SP-006: Adjust Driver Sliders** []
- Story: As a Market Team member, I want to drag a slider to adjust a driver's percentage for my Practice.
- AC: Given Attrition slider; When dragged; Then value increases dynamically; And Adjusted KPI updates.

**US-SP-010: Indication of Modified State** []
- Story: As a Market Team member, I want to visually distinguish between "Default" and "Modified" slider values.
- AC: Given baseline is 15%; When I change to 20%; Then slider track shows a tick mark at the original 15% position.

**SP-NEW-001: Edit scenario levers** []
- Story: As a Market Team member, I want to edit scenario levers and see updated volumes without navigating away.
- AC: Given viewing scenario; When levers are edited; Then outputs recalculate instantly for the BU/Practice.

**US-SP-011: View Baseline KPI** []
- Story: As a Market Team member, I want to see "Total Base" headcount before adjustments for my Practice.
- AC: Given baseline exists; When page loads; Then "Total Base" tile displays aggregate FTE count.

**US-SP-012: View Scenario Impact KPI** []
- Story: As a Market Team member, I want to see the "Scenario Adjusted" headcount for my Practice.
- AC: Given drivers applied; When viewing "Scenario Adjusted" tile; Then value reflects: Baseline + (Driver Impacts).

**US-SP-013: Calculate Net Change** []
- Story: As a Market Team member, I want to see the exact gap between Baseline and New Plan for the Practice.
- AC: Given both values exist; When rendering; Then "Net Change" tile shows difference in Blue or Red.

**US-SP-014: Compare Scenario vs Baseline Trend** []
- Story: As a Market Team member, I want a line chart comparing "Scenario" vs "Baseline" for my Practice.
- AC: When looking at chart; Then show Cyan for Baseline and Blue for Scenario.

**US-SP-015: Hover for Specific Data Points** []
- Story: As a Market Team member, I want to hover over months on the chart to read precise FTE values for my Practice.
- AC: Given chart visible; When I hover over a month; Then tooltip shows exact values for Baseline/Scenario.

**US-SP-018: View Detailed Practice Forecast Table** []
- Story: As a Market Team member, I want to see underlying data in a table including BU/Practice rows.
- AC: Given chart displayed; When scrolling to grid; Then show rows for Scenario, Baseline, and Adjustment.

**US-SP-019: Auto-Calculate Adjustment Row** []
- Story: As a Market Team member, I want the "Adjustment" row to automatically show the delta for my Practice.
- AC: Given table loaded; When rendering; Then each cell calculates: [Scenario−Baseline].

**US-SP-020: Visual Delta Indicators** []
- Story: As a Market Team member, I want adjustment numbers to be color-coded based on growth vs reduction for my Practice.
- AC: Given adjustment calculated; When positive; Then text is Blue; When negative; Then text is Red.

**US-SP-023: Scenario Pagination** []
- Story: As a Market Team member, I want to page through saved scenarios for the current BU/Practice.
- AC: Given multiple scenarios exist; When I click ">"; Then page reloads with data for next version.

**SP-NEW-002: Save scenario as new version** []
- Story: As a Market Team member, I want to save my changes as a new version with a timestamp for the Practice.
- AC: Given lever changes; When clicking Save; Then new version is created while preserving the old.

**US-SP-021: Send to Central Forecast Team** []
- Story: As a Market Team member, I want to send my Practice-specific scenario as an input to the CFT.
- AC: Given scenario saved; When clicking "Send as input"; Then status changes to "Sent" and locked.

**US-SP-017: Delete Scenario** []
- Story: As a Market Team member, I want to delete obsolete scenarios for my Practice.
- AC: Given I am creator or Admin; When clicking "Trash"; Then show confirmation before removal.

**US-SP-016: Rename Scenario** []
- Story: As a Market Team member, I want to rename the scenario (e.g. "Cloud Hiring Plan 2026").
- AC: Given default title; When clicking Pencil; Then text is editable; Save on Enter.

**US-SP-028: Share Scenario Link** []
- Story: As a Market Team member, I want to share a direct link to this specific Practice scenario.
- AC: Given active scenario; When clicking Share icon; Then copy unique URL to clipboard.

---

### Feedback

**US-FF-011: Download Audit Report** []
- Story: As a Market Team member, I want to download an "Audit Report" of the Practice feedback session.
- AC: Given session complete; When clicking Audit Report; Then PDF/Excel downloads containing all inputs/comments.

**US-FF-010: Submit Qualitative Feedback** []
- Story: As a Market Team member, I want to submit text feedback explaining the Practice context for AI learning.
- AC: Given forecast analyzed; When typing in text area; Then comment is saved and tied to the Cycle ID.

**US-FF-005: View Variance from Target** []
- Story: As a Market Team member, I want to see "Variance from Target" in the Practice summary table.
- AC: Given HC Target exists; When viewing summary; Then Variance calculates Forecast - Target for that scope.

**US-FF-006: Monitor Forecast Accuracy** []
- Story: As a Market Team member, I want to track the "Forecast Accuracy" score for my specific Practice Area.
- AC: Given past vs actual comparisons; When viewing scorecard; Then show percentage score for this segment.

**US-FF-007: Track Cycle-over-Cycle Change** []
- Story: As a Market Team member, I want to see "Variance from Last Cycle" for the Practice.
- AC: Given previous cycle data stored; When table loads; Then show percentage change vs last cycle.

**FF-NEW-002: Edit/delete cluster update** []
- Story: As a Market Team member, I want to edit or delete a draft skill cluster update before submitting.
- AC: Given draft rows exist; When choosing Edit or Delete; Then row is updated or removed from the list.

**FF-NEW-001: Add new skill cluster update** []
- Story: As a Market Team member, I want to add a new skill microcluster update during feedback for my Practice.
- AC: Given Feedback page; When clicking "Add New Update"; Then entry row appears for Change Type and Leaf Skills.

**FF-NEW-003: Submit forecast feedback** []
- Story: As a Market Team member, I want to submit feedback and updates for Central Forecast Team review.
- AC: Given text/updates entered; When clicking Submit; Then submission saved with context and confirm message.

**US-FF-008: View Taxonomy Change Log** []
- Story: As a Market Team member, I want to see a log of taxonomy changes used in the Practice forecast.
- AC: Given changes made; When viewing table; Then show "Newly Added" clusters and specific leaf skill changes.

**US-FF-009: Propose Taxonomy Update** []
- Story: As a Market Team member, I want to propose a new Skill Microcluster directly from the feedback page.
- AC: Given missing skill gap; When clicking "Add New Update"; Then modal opens to define Cluster and Leaf Skills.

**US-FF-003: Trace Forecast to Scenario ID** []
- Story: As a Market Team member, I want to see which "Scenario ID" drove the overlay for my Practice.
- AC: Given adjustments made; When viewing inputs table; Then show Scenario ID per variable.

**US-FF-004: View Input Variable Impact** []
- Story: As a Market Team member, I want to see the "% Impact" of each variable on the Practice forecast.
- AC: Given analyzing Scenario; When looking at Impact column; Then show weighted contribution.

**US-FF-001: Compare AI vs Management** []
- Story: As a Market Team member, I want to see the delta between AI baseline and human overlay for the Practice.
- AC: Given horizon selected; When viewing trend split; Then show stacked bars (Cyan=System, Purple=Overlay).

**US-FF-002: View Monthly Overlay Delta** []
- Story: As a Market Team member, I want to hover over a bar to see the numeric overlay delta for my Practice.
- AC: Given chart visible; When I hover over May; Then tooltip displays System, Overlay, and Total.

---

### Skill Taxonomy

**US-TX-009: View Audit Log** []
- Story: As a Market Team member, I want to see history of taxonomy changes (who added what).
- AC: Given history tab; When viewing; Then show log with Timestamp, User, and New Value.

**US-TX-001: View Taxonomy Mapping** []
- Story: As a Market Team member, I want to see which specific "Leaf Skills" make up a "Skill Microcluster".
- AC: Given Taxonomy page; Then show columns listing core skills. Cluster names must be combinations (e.g. java-sql).

**US-TX-003: Sort by Microcluster Name** []
- Story: As a Market Team member, I want to sort the table alphabetically by cluster name.
- AC: Given table loaded; When clicking header; Then sort A-Z. Cluster names must be combinations.

**US-TX-013: Empty State for Leaf Skills** []
- Story: As a Market Team member, I want to clearly see which skill columns are empty in the taxonomy.
- AC: Given cluster defined; When viewing row; Then empty leaf skill columns show a dash "-".

**US-TX-010: Bulk Import Taxonomy** []
- Story: As a Market Team member, I want to upload a CSV to bulk update microclusters for my Practice.
- AC: Given formatted CSV; When clicking Import; Then validate and create rows; And show success report.

**US-TX-012: Prevent Deletion of Used Clusters** []
- Story: As a Market Team member, I want the system to block deletion of clusters used in active Practice forecasts.
- AC: Given cluster has active demand; When attempting delete; Then block action and show error.

**US-TX-007: Download Taxonomy Reference** []
- Story: As a Market Team member, I want to Download cluster definitions to Excel for mapping.
- AC: Given grid view; When clicking Download; Then Excel file is generated mirroring structure.

**US-TX-011: Pagination Controls** []
- Story: As a Market Team member, I want to page through taxonomy rows for fast loading.
- AC: Given large record set; When scrolling; Then show pagination controls (Next, Prev).

**US-TX-004: Read-Only Access** []
- Story: As a Market Team member, I want read-only access to taxonomy to reference corporate standards.
- AC: Given Planner permissions; When viewing; Then show grid but NO Add/Edit/Delete buttons.

**US-TX-002: Search by Leaf Skill** []
- Story: As a Market Team member, I want to search for a skill (e.g. "React") to find its cluster.
- AC: Given search bar; When typing skill; Then filter table to show clusters containing that skill.

**US-TX-008: Prevent Duplicate Clusters** []
- Story: As a Market Team member, I want the system to prevent duplicate Cluster names in the taxonomy.
- AC: Given cluster exists; When trying to create duplicate; Then show error: "Name must be unique."

**?: ** []
- Story: 

---

## Persona: CFT

### Governance

**CFT-GOV-01: Dashboard Navigation** []
- Story: As a CFT member, I want to use the left-hand navigation menu to access the Governance page.
- AC: Given I am on any page; When I click the "Governance" icon in the sidebar; Then the Governance dashboard loads showing Cycle and Progress sections.

**CFT-GOV-02: Primary Governance Filters** []
- Story: As a CFT member, I want to filter the Governance Matrix to view status for specific BUs or periods.
- AC: Given I am on the Governance page; When I select values in Category, Service/Market, or Month filters; Then the "Table Name" grid updates.

**CFT-GOV-03: Governance Matrix Grid** []
- Story: As a CFT member, I want to view a detailed grid of ingestion and modeling status by BU.
- AC: Given I am on the Governance page; When I view the "Table Name" grid; Then the BU column remains sticky on scroll and rows are sorted Largest to Lowest by Total Demand.

**CFT-GOV-04: Download Governance Grid** []
- Story: As a CFT member, I want to Download the current data grid to Excel for offline status reporting.
- AC: Given I am on the Governance dashboard; When I click the "Download" icon; Then a file containing all filtered grid data is generated.

**CFT-GOV-05: View Progress Logic** []
- Story: As a CFT member, I want to see how the cycle progress is calculated for different workstreams.
- AC: Given I am on the Governance page; When I hover over the "i" info icon next to progress bars; Then a tooltip displays the logic for % completion.

**CFT-GOV-01: Dashboard Navigation** []
- Story: As a CFT member, I want to use the left-hand navigation menu to access the Governance page.
- AC: Given I am on any page; When I click the "Governance" icon in the sidebar; Then the Governance dashboard loads showing Cycle and Progress sections.

**CFT-GOV-02: Primary Governance Filters** []
- Story: As a CFT member, I want to filter the Governance Matrix to view status for specific BUs or periods.
- AC: Given I am on the Governance page; When I select values in Category, Service/Market, or Month filters; Then the "Table Name" grid updates.

**CFT-GOV-03: Governance Matrix Grid** []
- Story: As a CFT member, I want to view a detailed grid of ingestion and modeling status by BU.
- AC: Given I am on the Governance page; When I view the "Table Name" grid; Then the BU column remains sticky on scroll and rows are sorted Largest to Lowest by Total Demand.

**CFT-GOV-04: Download Governance Grid** []
- Story: As a CFT member, I want to Download the current data grid to Excel for offline status reporting.
- AC: Given I am on the Governance dashboard; When I click the "Download" icon; Then a file containing all filtered grid data is generated.

**CFT-GOV-05: View Progress Logic** []
- Story: As a CFT member, I want to see how the cycle progress is calculated for different workstreams.
- AC: Given I am on the Governance page; When I hover over the "i" info icon next to progress bars; Then a tooltip displays the logic for % completion.

---

### Task Management

**CFT-TSK-01: View Task Status Navigation** []
- Story: As a CFT member, I want to toggle between Modelling and Data Availability tabs.
- AC: Given I am on the Task Status page; When I click the "Modelling" or "Data Availability & Ingestion" tabs; Then the view switches to the corresponding task set.

**CFT-TSK-02: Modelling Filter & Grid** []
- Story: As a CFT member, I want to filter modeling tasks by run status and view performance metrics.
- AC: Given I am on the Modelling tab; When I select a "Run Status" (e.g., Failed); Then the grid shows Accuracy (%), Bias (%), and Task Status for matching models.

**CFT-TSK-03: Request Model Retrain** []
- Story: As a CFT member, I want to trigger a model retrain for under-performing practices.
- AC: Given a row in the Modelling table shows "Under-performing"; When I click "Request Model Retrain"; Then a system request is sent and status updates to "In Progress."

**CFT-TSK-04: Flag for Design Review** []
- Story: As a CFT member, I want to flag a failing model for a structural design review.
- AC: Given a model run has "Failed"; When I click "Flag for Design Review"; Then the Task Status updates to "Raised Design Review."

**CFT-TSK-05: Raise Data Quality Ticket** []
- Story: As a CFT member, I want to formally report a DQ failure to the data owner.
- AC: Given a row shows "Critical Errors" > 0; When I click "Raise DQ failure ticket"; Then a notification is sent to the Data Owner.

**CFT-TSK-06: Request Missing Data** []
- Story: As a CFT member, I want to nudge owners for data with low completeness.
- AC: Given a completeness score is "<threshold"; When I click "Ask for Complete Data"; Then a data-load request is sent to the Service Line owner.

**CFT-TSK-01: View Task Status Navigation** []
- Story: As a CFT member, I want to toggle between Modelling and Data Availability tabs.
- AC: Given I am on the Task Status page; When I click the "Modelling" or "Data Availability & Ingestion" tabs; Then the view switches to the corresponding task set.

**CFT-TSK-02: Modelling Filter & Grid** []
- Story: As a CFT member, I want to filter modeling tasks by run status and view performance metrics.
- AC: Given I am on the Modelling tab; When I select a "Run Status" (e.g., Failed); Then the grid shows Accuracy (%), Bias (%), and Task Status for matching models.

**CFT-TSK-03: Request Model Retrain** []
- Story: As a CFT member, I want to trigger a model retrain for under-performing practices.
- AC: Given a row in the Modelling table shows "Under-performing"; When I click "Request Model Retrain"; Then a system request is sent and status updates to "In Progress."

**CFT-TSK-04: Flag for Design Review** []
- Story: As a CFT member, I want to flag a failing model for a structural design review.
- AC: Given a model run has "Failed"; When I click "Flag for Design Review"; Then the Task Status updates to "Raised Design Review."

**CFT-TSK-05: Raise Data Quality Ticket** []
- Story: As a CFT member, I want to formally report a DQ failure to the data owner.
- AC: Given a row shows "Critical Errors" > 0; When I click "Raise DQ failure ticket"; Then a notification is sent to the Data Owner.

**CFT-TSK-06: Request Missing Data** []
- Story: As a CFT member, I want to nudge owners for data with low completeness.
- AC: Given a completeness score is "<threshold"; When I click "Ask for Complete Data"; Then a data-load request is sent to the Service Line owner.

---

### Performance

**CFT-PRF-01: Performance View Toggle** []
- Story: As a CFT member, I want to switch between Service Line and Market views.
- AC: Given I am on the Performance page; When I click the "Market Forecast Performance" tab; Then the dashboard refreshes with Market-level KPIs.

**CFT-PRF-02: View Score Weighting** []
- Story: As a CFT member, I want to see the weights of the Performance Score components.
- AC: Given I am viewing the Performance Score; When I hover over the "i" info icon; Then a tooltip displays the % breakdown (Accuracy 50%, Bias 20%, etc.).

**CFT-PRF-03: Schedule Performance Review** []
- Story: As a CFT member, I want to schedule a review for a PA with poor performance.
- AC: Given I click "View Task" in the Performance table; When I select "Schedule SL Performance Review" and set a date/time; Then the review is submitted.

**CFT-PRF-01: Performance View Toggle** []
- Story: As a CFT member, I want to switch between Service Line and Market views.
- AC: Given I am on the Performance page; When I click the "Market Forecast Performance" tab; Then the dashboard refreshes with Market-level KPIs.

**CFT-PRF-02: View Score Weighting** []
- Story: As a CFT member, I want to see the weights of the Performance Score components.
- AC: Given I am viewing the Performance Score; When I hover over the "i" info icon; Then a tooltip displays the % breakdown (Accuracy 50%, Bias 20%, etc.).

**CFT-PRF-03: Schedule Performance Review** []
- Story: As a CFT member, I want to schedule a review for a PA with poor performance.
- AC: Given I click "View Task" in the Performance table; When I select "Schedule SL Performance Review" and set a date/time; Then the review is submitted.

---

### Model Improvement

**CFT-MOD-01: View Skill Signal Details** []
- Story: As a CFT member, I want to see the underlying demand trend for a new skill signal.
- AC: Given the Skill Signals table; When I click "View Details" on a Micro-cluster row; Then a panel expands below the row showing monthly trends.

**CFT-TAX-01: Add New Skill Cluster/Leaf** []
- Story: As a CFT member, I want to add new clusters or leaf skills to the taxonomy.
- AC: Given I am in the Taxonomy Editor; When I click "+ Add Leaf Skill" or "Add New Cluster"; Then a new draft entry is created for editing.

**CFT-TAX-02: Shift or Delete Skill** []
- Story: As a CFT member, I want to move a leaf skill between clusters or remove proposed ones.
- AC: Given a skill row; When I click "Shift" (to re-parent) or "Delete"; Then the taxonomy structure is updated accordingly.

**CFT-MOD-01: View Skill Signal Details** []
- Story: As a CFT member, I want to see the underlying demand trend for a new skill signal.
- AC: Given the Skill Signals table; When I click "View Details" on a Micro-cluster row; Then a panel expands below the row showing monthly trends.

**CFT-TAX-01: Add New Skill Cluster/Leaf** []
- Story: As a CFT member, I want to add new clusters or leaf skills to the taxonomy.
- AC: Given I am in the Taxonomy Editor; When I click "+ Add Leaf Skill" or "Add New Cluster"; Then a new draft entry is created for editing.

**CFT-TAX-02: Shift or Delete Skill** []
- Story: As a CFT member, I want to move a leaf skill between clusters or remove proposed ones.
- AC: Given a skill row; When I click "Shift" (to re-parent) or "Delete"; Then the taxonomy structure is updated accordingly.

---

### Scenario Planning

**CFT-SCE-01: Re-run Scenario Model** []
- Story: As a CFT member, I want to update the forecast after adjusting scenario levers.
- AC: Given I have modified Lever Types or Values in the log; When I click "Run the model with above changes"; Then the adjustments chart and summary table refresh.

**CFT-SCE-02: Finalize Global Overlay** []
- Story: As a CFT member, I want to finalize the global scenario overlay for the current cycle.
- AC: Given I have reviewed adjustments; When I click "Send as input to Central Forecast Team"; Then the Status changes to "Finalized."

**CFT-SCE-03: Toggle Baseline in Chart** []
- Story: As a CFT member, I want to hide the baseline to focus strictly on the scenario trend.
- AC: Given I am viewing the line chart; When I click "Baseline" in the legend; Then the baseline series is hidden from the visual.

**CFT-SCE-01: Re-run Scenario Model** []
- Story: As a CFT member, I want to update the forecast after adjusting scenario levers.
- AC: Given I have modified Lever Types or Values in the log; When I click "Run the model with above changes"; Then the adjustments chart and summary table refresh.

**CFT-SCE-02: Finalize Global Overlay** []
- Story: As a CFT member, I want to finalize the global scenario overlay for the current cycle.
- AC: Given I have reviewed adjustments; When I click "Send as input to Central Forecast Team"; Then the Status changes to "Finalized."

**CFT-SCE-03: Toggle Baseline in Chart** []
- Story: As a CFT member, I want to hide the baseline to focus strictly on the scenario trend.
- AC: Given I am viewing the line chart; When I click "Baseline" in the legend; Then the baseline series is hidden from the visual.

---

