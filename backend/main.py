from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth, forecast, demand, supply, tasks, alerts, scenarios, feedback, taxonomy

app = FastAPI(title="Demand Planning API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Unversioned routes (tasks, alerts, auth, taxonomy stay at /api/) ──────────
app.include_router(auth.router,      prefix="/api/auth",      tags=["auth"])
app.include_router(demand.router,    prefix="/api/demand",    tags=["demand"])
app.include_router(supply.router,    prefix="/api/supply",    tags=["supply"])
app.include_router(tasks.router,     prefix="/api/tasks",     tags=["tasks"])
app.include_router(alerts.router,    prefix="/api/alerts",    tags=["alerts"])
app.include_router(scenarios.router, prefix="/api/scenarios", tags=["scenarios"])
app.include_router(feedback.router,  prefix="/api/feedback",  tags=["feedback"])
app.include_router(taxonomy.router,  prefix="/api/taxonomy",  tags=["taxonomy"])

# ── v1 routes (Angular frontend uses /api/v1/* per ml-api-contract.md) ────────
app.include_router(forecast.router,  prefix="/api/v1/forecast",  tags=["forecast-v1"])
app.include_router(scenarios.router, prefix="/api/v1/scenarios", tags=["scenarios-v1"])
app.include_router(feedback.router,  prefix="/api/v1/feedback",  tags=["feedback-v1"])


@app.get("/health")
def health():
    return {"status": "ok"}
