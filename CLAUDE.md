# Demand Forecast Planner — Angular Edition

## What This Is

An enterprise **Demand Forecast Planner** for an IT services organization.
Part of the TMP (Talent Marketplace) Reimagination initiative. This is the
**Angular rewrite** of the original React workspace (`demand-planning-project`).

All features, pages, design system, backend, and ML pipeline are identical —
only the frontend stack has been changed to Angular.

---

## Frontend Stack (Changed)

| Layer | React (original) | Angular (this workspace) |
|-------|-----------------|--------------------------|
| Framework | React 18 + Vite | Angular 17 (standalone) |
| Styling | Tailwind CSS | Tailwind CSS (same config) |
| Data Fetching | TanStack Query | Angular HttpClient + RxJS |
| Data Tables | TanStack Table | Custom DataGridComponent |
| Charts | Recharts | Chart.js + ng2-charts v6 |
| Routing | React Router 6 | Angular Router |
| State | React Context + useState | BehaviorSubject services |
| Icons | lucide-react | Inline SVG icons |

## Frontend location: `frontend-ang/`

```
frontend-ang/
├── angular.json              # Angular CLI config
├── package.json              # Angular 17 deps
├── tsconfig.json             # TypeScript strict mode
├── tailwind.config.js        # Same brand colors
├── src/
│   ├── main.ts               # Bootstrap entry
│   ├── styles.css            # Tailwind + global styles
│   ├── app/
│   │   ├── app.component.ts  # Root component
│   │   ├── app.config.ts     # DI providers
│   │   ├── app.routes.ts     # All routes (lazy-loaded)
│   │   ├── core/
│   │   │   ├── models/       # TypeScript interfaces (same as React)
│   │   │   ├── services/     # AuthService, ApiService, FilterService
│   │   │   ├── guards/       # authGuard (functional)
│   │   │   └── interceptors/ # authInterceptor (Bearer token)
│   │   ├── shared/components/
│   │   │   ├── kpi-card/     # KPI metric cards
│   │   │   ├── data-grid/    # Sortable/paginated data table
│   │   │   ├── filter-bar/   # 6-filter dropdown bar
│   │   │   ├── status-badge/ # Color-coded status pills
│   │   │   ├── loading-skeleton/
│   │   │   └── error-card/
│   │   └── pages/
│   │       ├── login/
│   │       ├── layout/       # Sidebar + filter bar + router-outlet
│   │       ├── executive-summary/
│   │       ├── forecast-dashboard/ (7 tabs)
│   │       ├── my-tasks/
│   │       ├── my-alerts/
│   │       ├── scenario-planning/
│   │       ├── forecast-feedback/
│   │       └── skill-taxonomy/
```

---

## Backend & ML (Unchanged)

- `backend/` — FastAPI Python microservice (identical to original)
- `ml-services/` — Skill micro-clustering pipeline (identical)
- `docs/` — Wireframes, architecture, KPI definitions
- `stories/` — 326 user stories
- `contracts/` — ML API contracts

---

## Design System (Unchanged)

All brand colors, fonts, spacing, and component patterns are identical to
the original React workspace:

- **Sidebar:** Dark navy (#1B2559), white text
- **Primary accent:** Teal (#00BCD4)
- **Secondary:** Purple (#7C3AED)
- **KPI cards:** White with subtle border, large numbers
- **Status badges:** Red/Yellow/Green
- **Charts:** Navy/Teal/Purple palette

---

## Development

### Start locally (Angular dev server)
```bash
cd frontend-ang
npm install
npm start           # http://localhost:3000
```

### Build for production
```bash
cd frontend-ang
npm run build:prod
```

### Docker (full stack)
```bash
docker-compose up --build
```

### Backend only
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

---

## API Endpoints (same as original)

See `contracts/ml-api-contract.md` for all 11 endpoints.

Backend runs on `http://localhost:8000`. Angular dev server proxies API
calls or uses `http://localhost:8000` directly via ApiService.

---

## Key Reference Docs (same as original)
- `docs/wireframe-reference.md` — Component-level wireframe specs
- `docs/architecture-summary.md` — System architecture
- `docs/kpi-definitions.md` — KPI formulas
- `contracts/ml-api-contract.md` — API shapes
