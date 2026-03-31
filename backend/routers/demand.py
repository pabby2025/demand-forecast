from fastapi import APIRouter
import mock_data

router = APIRouter()


@router.get("/summary")
def demand_summary():
    overview = mock_data.get_forecast_overview()
    return {
        "total_demand": overview["kpis"]["total_forecast"],
        "by_cluster": [
            {
                "cluster": row["cluster"],
                "total": sum(row[f"{m}_actual"] for m in mock_data.MONTHS),
            }
            for row in overview["grid"]
        ],
        "by_bu": mock_data.get_bu_performance()["bu_list"][:5],
        "trend": overview["monthly_trend"],
    }
