from fastapi import APIRouter, Query
from typing import Optional
import mock_data

router = APIRouter()


def _build_filters(
    practice_area: Optional[str],
    bu: Optional[str],
    location: Optional[str],
    grade: Optional[str],
    skill_cluster: Optional[str],
    forecast_horizon: Optional[str],
) -> dict:
    return {
        k: v for k, v in {
            "practice_area": practice_area,
            "bu": bu,
            "location": location,
            "grade": grade,
            "skill_cluster": skill_cluster,
            "forecast_horizon": forecast_horizon,
        }.items() if v is not None
    }


def _filters_from_query(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
) -> dict:
    return _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)


# ── /overview ─────────────────────────────────────────────────────────────────
@router.get("/overview")
def forecast_overview(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_forecast_overview(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /demand-type-breakdown (contract name) + legacy /demand-type ──────────────
@router.get("/demand-type-breakdown")
@router.get("/demand-type")
def demand_type(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_demand_type_breakdown(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /business-unit (contract name) + legacy /bu-performance ──────────────────
@router.get("/business-unit")
@router.get("/bu-performance")
def bu_performance(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_bu_performance(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /geographic ───────────────────────────────────────────────────────────────
@router.get("/geographic")
def geographic(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_geographic_distribution(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /skill-distribution ───────────────────────────────────────────────────────
@router.get("/skill-distribution")
def skill_distribution(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_skill_distribution(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /grade-distribution ───────────────────────────────────────────────────────
@router.get("/grade-distribution")
def grade_distribution(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_grade_distribution(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /demand-supply-gap ────────────────────────────────────────────────────────
@router.get("/demand-supply-gap")
def demand_supply_gap(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_demand_supply_gap(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )


# ── /executive-summary ────────────────────────────────────────────────────────
@router.get("/executive-summary")
def executive_summary(
    practice_area: Optional[str] = Query(None),
    bu: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    skill_cluster: Optional[str] = Query(None),
    forecast_horizon: Optional[str] = Query(None),
):
    return mock_data.get_executive_summary(
        _build_filters(practice_area, bu, location, grade, skill_cluster, forecast_horizon)
    )
