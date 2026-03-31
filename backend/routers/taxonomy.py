from fastapi import APIRouter
import mock_data

router = APIRouter()


def _build_clusters():
    clusters = []
    cv_values  = [0.251, 0.308, 0.346, 0.395, 0.471, 0.531, 0.533, 0.722, 0.826]
    cv25_values = [0.217, 0.333, 0.303, 0.194, 0.356, 0.174, 0.382, 0.350, 0.240]
    for i, cluster in enumerate(mock_data.SKILL_CLUSTERS):
        cv = cv_values[i] if i < len(cv_values) else round(0.15 + (i * 0.04) % 0.35, 3)
        cv25 = cv25_values[i] if i < len(cv25_values) else cv
        xyz = "X" if cv < 0.5 else ("Y" if cv <= 1.0 else "Z")
        clusters.append({
            "id": f"CLU-{i + 1:03d}",
            "cluster": cluster,
            "leaf_skills": mock_data.CLUSTER_LEAF_SKILLS.get(cluster, []),
            "total_demands": mock_data.CLUSTER_DEMANDS[i],
            "cv_score": cv,
            "cv_2025": cv25,
            "xyz_segment": xyz,
            "stability": round(max(0.0, min(1.0, 1 - cv)), 4),
            "last_updated": "2026-03-01",
        })
    return clusters


# /api/taxonomy  (legacy)
@router.get("")
def get_taxonomy():
    clusters = _build_clusters()
    return {"clusters": clusters, "total": len(clusters)}


# /api/taxonomy/clusters  (Angular frontend uses this path)
@router.get("/clusters")
def get_taxonomy_clusters():
    clusters = _build_clusters()
    return {"clusters": clusters, "total": len(clusters)}
