from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import mock_data
import copy

router = APIRouter()

_scenario_store: list[dict] = copy.deepcopy(mock_data.get_scenarios())


class ScenarioDrivers(BaseModel):
    # Legacy driver names (kept for backward compat)
    revenue_growth: float = 0.0
    market_expansion: float = 0.0
    headcount_change: float = 0.0
    tech_investment: float = 0.0
    # v1 driver names per ml-api-contract.md
    bu_level_growth_pct: float = 0.0
    industry_level_market_spend_pct: float = 0.0
    win_rate_strategic_pct: float = 0.0
    growth_strategic_pct: float = 0.0


class ScenarioFilters(BaseModel):
    practice_area: Optional[str] = None
    bu: Optional[str] = None
    location: Optional[str] = None
    grade: Optional[str] = None
    skill_cluster: Optional[str] = None


class ScenarioSimulateRequest(BaseModel):
    filters: ScenarioFilters = ScenarioFilters()
    drivers: ScenarioDrivers


class ScenarioCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    drivers: ScenarioDrivers


class ScenarioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    drivers: Optional[ScenarioDrivers] = None


@router.get("")
def list_scenarios():
    return {"scenarios": _scenario_store, "total": len(_scenario_store)}


@router.get("/{scenario_id}")
def get_scenario(scenario_id: str):
    scn = next((s for s in _scenario_store if s["id"] == scenario_id), None)
    if not scn:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scn


@router.post("")
def create_scenario(scn: ScenarioCreate):
    new_id = f"SCN-{str(len(_scenario_store) + 1).zfill(3)}"
    drivers = scn.drivers.model_dump()
    impact = mock_data.compute_scenario_impact(drivers)
    new_scn = {
        "id": new_id,
        "name": scn.name,
        "description": scn.description,
        "status": "Draft",
        "created_by": "Sarah Chen",
        "created_at": "2026-03-19",
        "drivers": drivers,
        "impact": impact,
    }
    _scenario_store.append(new_scn)
    return new_scn


@router.put("/{scenario_id}")
def update_scenario(scenario_id: str, update: ScenarioUpdate):
    scn = next((s for s in _scenario_store if s["id"] == scenario_id), None)
    if not scn:
        raise HTTPException(status_code=404, detail="Scenario not found")
    if update.name is not None:
        scn["name"] = update.name
    if update.description is not None:
        scn["description"] = update.description
    if update.status is not None:
        scn["status"] = update.status
    if update.drivers is not None:
        scn["drivers"] = update.drivers.model_dump()
        scn["impact"] = mock_data.compute_scenario_impact(scn["drivers"])
    return scn


@router.post("/simulate")
def simulate_scenario(req: ScenarioSimulateRequest):
    """
    POST /api/v1/scenarios/simulate
    Applies business driver adjustments to the base ML forecast.
    """
    drivers = req.drivers.model_dump()
    # Combine legacy + v1 driver values (v1 names take precedence when non-zero)
    net_growth = (
        drivers.get("bu_level_growth_pct") or drivers.get("revenue_growth", 0)
    ) / 100.0
    market_signal = (
        drivers.get("industry_level_market_spend_pct") or drivers.get("market_expansion", 0)
    ) / 100.0
    win_rate    = drivers.get("win_rate_strategic_pct", 0) / 100.0
    strat_growth = drivers.get("growth_strategic_pct", 0) / 100.0

    combined_factor = 1 + net_growth + (market_signal * 0.5) + (win_rate * 0.3) + (strat_growth * 0.2)

    base_data   = mock_data.get_forecast_overview(req.filters.model_dump(exclude_none=True))
    monthly     = base_data.get("monthly_trend", [])
    total_base  = sum(r.get("actual", r.get("fte_demand", 0)) for r in monthly)
    scenario_total = int(total_base * combined_factor)

    comparison_chart = [
        {
            "month": r.get("month", ""),
            "baseline": r.get("actual", r.get("fte_demand", 0)),
            "scenario": int(r.get("actual", r.get("fte_demand", 0)) * combined_factor),
        }
        for r in monthly
    ]

    month_labels = ["Jan 26", "Feb 26", "Mar 26", "Apr 26", "May 26", "Jun 26",
                    "Jul 26", "Aug 26", "Sep 26", "Oct 26", "Nov 26", "Dec 26"]
    scenario_row  = {"metric": "Scenario Forecast"}
    baseline_row  = {"metric": "Baseline Forecast"}
    adjustment_row = {"metric": "Adjustment"}
    for i, row in enumerate(comparison_chart[:12]):
        key = month_labels[i].replace(" ", "_").lower()
        scenario_row[key]   = row["scenario"]
        baseline_row[key]   = row["baseline"]
        adjustment_row[key] = row["scenario"] - row["baseline"]

    return {
        "kpis": {
            "total_base": total_base,
            "scenario_adjusted": scenario_total,
            "net_change": scenario_total - total_base,
        },
        "comparison_chart": comparison_chart,
        "comparison_table": {"rows": [scenario_row, baseline_row, adjustment_row]},
        "explainability": [
            f"BU-level growth factor: {net_growth * 100:.1f}%",
            f"Industry market spend signal: {market_signal * 100:.1f}%",
            f"Strategic win rate contribution: {win_rate * 100:.1f}%",
            f"Combined demand adjustment: {(combined_factor - 1) * 100:.1f}%",
        ],
    }


@router.get("/{scenario_id}/comparison")
def scenario_comparison(scenario_id: str):
    scn = next((s for s in _scenario_store if s["id"] == scenario_id), None)
    if not scn:
        raise HTTPException(status_code=404, detail="Scenario not found")

    import mock_data as md
    baseline = md.get_forecast_overview()
    monthly = baseline["monthly_trend"]
    impact_pct = scn["impact"]["demand_delta_pct"] / 100

    comparison = [
        {
            "month": row["month"],
            "baseline": row["actual"],
            "scenario": int(row["actual"] * (1 + impact_pct)),
        }
        for row in monthly
    ]
    return {
        "scenario": scn,
        "comparison": comparison,
        "summary": {
            "baseline_total": sum(r["baseline"] for r in comparison),
            "scenario_total": sum(r["scenario"] for r in comparison),
            "delta": scn["impact"]["demand_delta"],
            "delta_pct": scn["impact"]["demand_delta_pct"],
        },
    }
