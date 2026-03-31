from fastapi import APIRouter
import mock_data
import random

router = APIRouter()

random.seed(99)


@router.get("/summary")
def supply_summary():
    gap_data = mock_data.get_demand_supply_gap()
    return {
        "total_supply": gap_data["kpis"]["supply_utilization"],
        "bench_available": gap_data["kpis"]["bench_available"],
        "utilization_pct": gap_data["kpis"]["supply_utilization"],
        "by_cluster": [
            {
                "cluster": row["cluster"],
                "supply": row["supply"],
                "demand": row["demand"],
                "gap": row["gap"],
                "gap_pct": row["gap_pct"],
            }
            for row in gap_data["gap_grid"]
        ],
        "trend": gap_data["ds_trend"],
    }
