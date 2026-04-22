"""
Comprehensive mock data generator for Demand Planning Platform.
All data matches ML output dimensions from the cluster pipeline.
"""
import random
from datetime import datetime, date, timedelta
from typing import Any

random.seed(42)

# ── Constants ────────────────────────────────────────────────────────────────

SKILL_CLUSTERS = [
    "MSC-.NET-Angular-Azure-C#-Java",
    "MSC-Agile-Microsoft_365-PPM-Project_Management",
    "MSC-Git-HTML/CSS-Node_JS-React-TypeScript",
    "MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform",
    "MSC-Java-Kafka-Microservices-Python-Spring_Boot",
    "MSC-AWS-Java-MySQL-SQL-Spring_Boot",
    "MSC-Android-React_Native-iOS",
    "MSC-API_Development-Git-Java-Shell_Scripting-Software_Testing",
    "MSC-AWS-Java-JavaScript-MySQL-SQL",
]

CLUSTER_DEMANDS = [7421, 6363, 4884, 1339, 5250, 15618, 2119, 1864, 2562]

CLUSTER_LEAF_SKILLS = {
    "MSC-.NET-Angular-Azure-C#-Java": [
        {"skill": ".NET", "weight": 0.91},
        {"skill": "Angular", "weight": 0.87},
        {"skill": "Azure", "weight": 0.83},
        {"skill": "C#", "weight": 0.95},
        {"skill": "Java", "weight": 0.78},
        {"skill": "ASP.NET Core", "weight": 0.72},
        {"skill": "Entity Framework", "weight": 0.65},
    ],
    "MSC-Agile-Microsoft_365-PPM-Project_Management": [
        {"skill": "Agile", "weight": 0.94},
        {"skill": "Microsoft 365", "weight": 0.88},
        {"skill": "PPM", "weight": 0.79},
        {"skill": "Project Management", "weight": 0.96},
        {"skill": "Scrum", "weight": 0.85},
        {"skill": "JIRA", "weight": 0.71},
        {"skill": "MS Project", "weight": 0.68},
    ],
    "MSC-Git-HTML/CSS-Node_JS-React-TypeScript": [
        {"skill": "Git", "weight": 0.93},
        {"skill": "HTML/CSS", "weight": 0.89},
        {"skill": "Node.js", "weight": 0.86},
        {"skill": "React", "weight": 0.92},
        {"skill": "TypeScript", "weight": 0.88},
        {"skill": "JavaScript", "weight": 0.95},
        {"skill": "REST APIs", "weight": 0.74},
    ],
    "MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform": [
        {"skill": "AWS", "weight": 0.90},
        {"skill": "DevOps", "weight": 0.88},
        {"skill": "Docker", "weight": 0.85},
        {"skill": "Jenkins", "weight": 0.79},
        {"skill": "Terraform", "weight": 0.82},
        {"skill": "Kubernetes", "weight": 0.76},
        {"skill": "CI/CD", "weight": 0.84},
    ],
    "MSC-Java-Kafka-Microservices-Python-Spring_Boot": [
        {"skill": "Java", "weight": 0.95},
        {"skill": "Kafka", "weight": 0.81},
        {"skill": "Microservices", "weight": 0.87},
        {"skill": "Python", "weight": 0.83},
        {"skill": "Spring Boot", "weight": 0.90},
        {"skill": "REST APIs", "weight": 0.78},
        {"skill": "Docker", "weight": 0.72},
    ],
    "MSC-AWS-Java-MySQL-SQL-Spring_Boot": [
        {"skill": "AWS", "weight": 0.88},
        {"skill": "Java", "weight": 0.94},
        {"skill": "MySQL", "weight": 0.86},
        {"skill": "SQL", "weight": 0.92},
        {"skill": "Spring Boot", "weight": 0.89},
        {"skill": "Hibernate", "weight": 0.75},
        {"skill": "JPA", "weight": 0.72},
    ],
    "MSC-Android-React_Native-iOS": [
        {"skill": "Android", "weight": 0.91},
        {"skill": "React Native", "weight": 0.87},
        {"skill": "iOS", "weight": 0.89},
        {"skill": "Swift", "weight": 0.82},
        {"skill": "Kotlin", "weight": 0.84},
        {"skill": "Flutter", "weight": 0.71},
        {"skill": "Mobile UX", "weight": 0.68},
    ],
    "MSC-API_Development-Git-Java-Shell_Scripting-Software_Testing": [
        {"skill": "API Development", "weight": 0.90},
        {"skill": "Git", "weight": 0.88},
        {"skill": "Java", "weight": 0.85},
        {"skill": "Shell Scripting", "weight": 0.79},
        {"skill": "Software Testing", "weight": 0.87},
        {"skill": "Selenium", "weight": 0.74},
        {"skill": "JUnit", "weight": 0.71},
    ],
    "MSC-AWS-Java-JavaScript-MySQL-SQL": [
        {"skill": "AWS", "weight": 0.87},
        {"skill": "Java", "weight": 0.91},
        {"skill": "JavaScript", "weight": 0.85},
        {"skill": "MySQL", "weight": 0.88},
        {"skill": "SQL", "weight": 0.93},
        {"skill": "Node.js", "weight": 0.76},
        {"skill": "Express.js", "weight": 0.69},
    ],
}

BUSINESS_UNITS = [
    "Consulting", "Financial Services", "Healthcare", "Technology",
    "Retail", "Manufacturing", "Energy", "Telecom",
    "Government", "Media", "Education", "Logistics",
    "Real Estate", "Automotive",
]

GRADES = ["SA", "A", "M", "GenC", "SM", "AD"]
GRADE_LABELS = {
    "SA": "Senior Associate",
    "A": "Analyst",
    "M": "Manager",
    "GenC": "General Consultant",
    "SM": "Senior Manager",
    "AD": "Associate Director",
}

COUNTRIES = ["US", "India", "UK", "Philippines", "Poland", "Canada", "Australia", "Germany", "Singapore", "UAE"]

MONTHS = ["M0", "M1", "M2", "M3", "M4", "M5"]

MONTH_LABELS = ["Jan 2026", "Feb 2026", "Mar 2026", "Apr 2026", "May 2026", "Jun 2026"]

MODELS = ["LightGBM", "CatBoost", "NeuralNet", "AutoGluon_Ensemble"]
PREDICTION_SOURCES = ["AutoML", "Override", "Guardrail_Corrected"]

PRACTICE_AREAS = ["Technology", "Financial Services", "Healthcare", "Consulting", "Energy", "Retail"]

# ── Filter helpers ───────────────────────────────────────────────────────────

# Fractional demand weights — how much of total demand each dimension represents
_PA_WEIGHTS: dict[str, float] = {
    "Technology": 0.30, "Financial Services": 0.25, "Healthcare": 0.20,
    "Consulting": 0.15, "Energy": 0.05, "Retail": 0.05,
}
_GRADE_WEIGHTS: dict[str, float] = {
    "SA": 0.25, "A": 0.35, "M": 0.20, "GenC": 0.10, "SM": 0.05, "AD": 0.05,
}
_LOCATION_WEIGHTS: dict[str, float] = {
    "US": 0.35, "India": 0.40, "UK": 0.08, "Philippines": 0.07, "Poland": 0.04,
    "Canada": 0.02, "Australia": 0.01, "Germany": 0.01, "Singapore": 0.01, "UAE": 0.01,
}
_BU_WEIGHT: float = 1.0 / len(BUSINESS_UNITS)


def _filter_scale(filters: dict | None) -> float:
    """Return a multiplicative scale factor (0 < s <= 1) based on active filters."""
    if not filters:
        return 1.0
    scale = 1.0
    if pa := filters.get("practice_area"):
        scale *= _PA_WEIGHTS.get(pa, 0.17)
    if filters.get("bu"):
        scale *= _BU_WEIGHT
    if grade := filters.get("grade"):
        scale *= _GRADE_WEIGHTS.get(grade, 0.17)
    if loc := filters.get("location"):
        scale *= _LOCATION_WEIGHTS.get(loc, 0.10)
    return max(scale, 0.005)


def _cluster_idx(skill_cluster: str) -> int | None:
    """Return SKILL_CLUSTERS index for a given cluster name, or None."""
    try:
        return SKILL_CLUSTERS.index(skill_cluster)
    except ValueError:
        return None


def _matches_row(row: dict, filters: dict | None) -> bool:
    """Check whether a grid row satisfies the active filters."""
    if not filters:
        return True
    # Map filter keys → row field names
    checks = [
        ("practice_area", "practice_area"),
        ("grade", "so_grade"),
        ("skill_cluster", "cluster"),
        ("location", "location"),
        ("bu", "business_unit"),
    ]
    for fk, rk in checks:
        fv = filters.get(fk)
        if not fv:
            continue
        rv = row.get(rk)
        if rv is None:
            continue
        if rv != fv:
            return False
    return True


# ── Helpers ──────────────────────────────────────────────────────────────────

def _jitter(base: int, pct: float = 0.12) -> int:
    delta = int(base * pct)
    return base + random.randint(-delta, delta)


def _accuracy() -> float:
    return round(random.uniform(82.0, 96.5), 1)


def _predicted(actual: int) -> int:
    variance = random.uniform(0.05, 0.15)
    direction = random.choice([1, -1])
    return max(1, int(actual * (1 + direction * variance)))


def _trend_series(base: int, months: int = 6, growth: float = 0.03) -> list[int]:
    values = []
    current = base
    for _ in range(months):
        current = int(current * (1 + random.uniform(-0.02, growth)))
        values.append(current)
    return values


def _month_label(offset: int) -> str:
    base = date(2026, 1, 1)
    d = base + timedelta(days=30 * offset)
    return d.strftime("%b %Y")


# ── Forecast Overview ─────────────────────────────────────────────────────────

def get_forecast_overview(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    total_demand = int(47420 * scale)
    monthly_totals = _trend_series(max(total_demand // 6, 1), 6, 0.025)
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]

    # Grid rows: dimension columns + monthly demand columns
    all_rows = []
    row_idx = 0
    for i, cluster in enumerate(SKILL_CLUSTERS[:5]):
        for pa in PRACTICE_AREAS:
            for grade in GRADES:
                base = CLUSTER_DEMANDS[i] // (len(PRACTICE_AREAS) * len(GRADES))
                row: dict[str, Any] = {
                    "practice_area": pa,
                    "location": COUNTRIES[row_idx % len(COUNTRIES)],
                    "cluster": cluster,
                    "so_grade": grade,
                }
                for mn in month_names:
                    row[mn.lower()] = _jitter(base)
                row["q01"] = row["jan"] + row["feb"] + row["mar"]
                row["q02"] = row["apr"] + row["may"] + row["jun"]
                all_rows.append(row)
                row_idx += 1

    grid_rows = [r for r in all_rows if _matches_row(r, filters)]

    # Per-cluster M0-M5 ML predictions (filter to selected cluster if set)
    clusters_to_show = SKILL_CLUSTERS
    if sc := (filters or {}).get("skill_cluster"):
        clusters_to_show = [sc] if sc in SKILL_CLUSTERS else []

    forecasts_by_cluster = []
    for i, cluster in enumerate(SKILL_CLUSTERS):
        if cluster not in clusters_to_show:
            continue
        base = max(int(CLUSTER_DEMANDS[i] * scale / max(_filter_scale({k: v for k, v in (filters or {}).items() if k != "skill_cluster"}), 0.005)), 1) if (filters or {}).get("skill_cluster") else max(int(CLUSTER_DEMANDS[i] * scale), 1)
        base = CLUSTER_DEMANDS[i] // 6
        months_data = []
        for j in range(6):
            actual = _jitter(int(base * scale)) if j < 3 else 0
            predicted = _predicted(_jitter(int(base * scale)))
            months_data.append({
                "month_index": j,
                "actual": actual,
                "predicted": predicted,
                "predicted_corrected": max(predicted, int(predicted * 0.97)),
                "accuracy_pct": _accuracy(),
            })
        forecasts_by_cluster.append({
            "skill_cluster": cluster,
            "months": months_data,
            "model_name": random.choice(MODELS),
        })

    mom_current = round(random.uniform(4.0, 8.4), 1)
    return {
        "kpis": {
            "total_forecast_fte": total_demand,
            "demand_growth_rate": {
                "qoq": {"current": round(random.uniform(10, 18), 1), "last_year": round(random.uniform(8, 14), 1)},
                "mom": {"current": mom_current, "last_year": round(mom_current - random.uniform(0.5, 2.0), 1)},
                "wow": {"current": round(random.uniform(1.5, 4.5), 1), "last_year": round(random.uniform(1.0, 3.5), 1)},
            },
            "avg_cancellation_pct": 40.0,
            "top_practice_areas": [
                {"name": "ADM Central", "pct": 20},
                {"name": "Digital Engineering", "pct": 15},
                {"name": "EPS IPM", "pct": 10},
            ],
        },
        "explainability": [
            "Growth driven by Java/Spring Boot and Cloud clusters in Americas BU",
            "Backfill demand up 12% vs prior quarter — indicates higher attrition",
            "Short-fuse SOs (RSD < 6 weeks) represent 18% of open demand",
        ],
        "trend_monthly": [
            {"month": month_names[i], "fte_demand": monthly_totals[i],
             "growth_rate_pct": round(random.uniform(3, 12), 1)}
            for i in range(6)
        ],
        "trend_weekly": [
            {"week": f"W{i+1}", "fte_demand": _jitter(max(total_demand // 24, 1), 0.08),
             "growth_rate_pct": round(random.uniform(1, 6), 1)}
            for i in range(5)
        ],
        "trend_quarterly": [
            {"quarter": "QTR 01", "fte_demand": sum(monthly_totals[:3]),
             "growth_rate_pct": round(random.uniform(8, 16), 1)},
            {"quarter": "QTR 02", "fte_demand": sum(monthly_totals[3:]),
             "growth_rate_pct": round(random.uniform(10, 18), 1)},
        ],
        "grid": grid_rows,
        "grid_weekly": [
            {
                "practice_area": r["practice_area"],
                "location": r["location"],
                "cluster": r["cluster"],
                "so_grade": r["so_grade"],
                "w1": _jitter(max(r["jan"] // 4, 1)),
                "w2": _jitter(max(r["jan"] // 4, 1)),
                "w3": _jitter(max(r["feb"] // 4, 1)),
                "w4": _jitter(max(r["feb"] // 4, 1)),
            }
            for r in grid_rows[:60]
        ],
        "forecasts_by_cluster": forecasts_by_cluster,
    }


# ── Demand Type Breakdown ─────────────────────────────────────────────────────

def get_demand_type_breakdown(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    total = int(47420 * scale)
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    def _grid_rows(extra_key: str, extra_val: str) -> list:
        rows = []
        row_idx = 0
        for i, cluster in enumerate(SKILL_CLUSTERS[:4]):
            for pa in PRACTICE_AREAS:
                for grade in GRADES:
                    base = int(random.randint(200, 800) * scale)
                    row: dict[str, Any] = {
                        "practice_area": pa,
                        "location": COUNTRIES[row_idx % len(COUNTRIES)],
                        "cluster": cluster,
                        "so_grade": grade,
                        extra_key: extra_val,
                    }
                    for mn in month_names:
                        row[mn] = _jitter(max(base // 6, 1))
                    row["q01"] = row["jan"] + row["feb"] + row["mar"]
                    rows.append(row)
                    row_idx += 1
        return [r for r in rows if _matches_row(r, filters)]

    return {
        "kpis": {
            "new_vs_backfill": {"new_pct": 65, "backfill_pct": 35},
            "contract_type_mix": {"t_and_m_pct": 55, "fixed_price_pct": 30, "transaction_based_pct": 15},
        },
        "trend_new_vs_backfill": [
            {"month": MONTH_LABELS[i],
             "new_demand": _jitter(max(int(total * 0.65 / 6), 1)),
             "backfill": _jitter(max(int(total * 0.35 / 6), 1))}
            for i in range(6)
        ],
        "trend_billability": [
            {"month": MONTH_LABELS[i],
             "bfd": _jitter(max(int(total * 0.55 / 6), 1)),
             "btb": _jitter(max(int(total * 0.30 / 6), 1)),
             "btm": _jitter(max(int(total * 0.15 / 6), 1))}
            for i in range(6)
        ],
        "grid_new_vs_backfill": _grid_rows("demand_type", "New Demand") + _grid_rows("demand_type", "Backfill"),
        "grid_billability": _grid_rows("billability_type", "BFD") + _grid_rows("billability_type", "BTB"),
    }


# ── BU Performance ────────────────────────────────────────────────────────────

def get_bu_performance(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    # If a specific BU is filtered, only show that BU; otherwise show top 8
    active_bu = (filters or {}).get("bu")
    bu_pool = [active_bu] if active_bu and active_bu in BUSINESS_UNITS else BUSINESS_UNITS[:8]
    bu_sorted = sorted(bu_pool, key=lambda _: random.randint(1000, 5000), reverse=True)

    trend_bu_demand = []
    trend_bu_growth = []
    for i in range(6):
        d_row: dict[str, Any] = {"month": MONTH_LABELS[i]}
        g_row: dict[str, Any] = {"month": MONTH_LABELS[i]}
        for bu in bu_sorted[:5]:
            d_row[bu] = _jitter(int(random.randint(400, 1200) * scale))
            g_row[bu] = round(random.uniform(-3, 18), 1)
        trend_bu_demand.append(d_row)
        trend_bu_growth.append(g_row)

    def _bu_grid(bu_name: str) -> list:
        rows = []
        row_idx = 0
        for i, cluster in enumerate(SKILL_CLUSTERS[:3]):
            for pa in PRACTICE_AREAS[:3]:
                for grade in GRADES[:4]:
                    base = int(random.randint(150, 600) * scale)
                    row: dict[str, Any] = {
                        "practice_area": pa,
                        "location": COUNTRIES[row_idx % len(COUNTRIES)],
                        "cluster": cluster,
                        "so_grade": grade,
                        "business_unit": bu_name,
                    }
                    for mn in month_names:
                        row[mn] = _jitter(max(base // 6, 1))
                    rows.append(row)
                    row_idx += 1
        return [r for r in rows if _matches_row(r, filters)]

    grid_bu_demand, grid_bu_growth = [], []
    for bu in bu_sorted[:3]:
        grid_bu_demand.extend(_bu_grid(bu))
        grid_bu_growth.extend(_bu_grid(bu))

    # Filter ml_forecasts by skill_cluster if set
    clusters_for_ml = SKILL_CLUSTERS[:2]
    if sc := (filters or {}).get("skill_cluster"):
        clusters_for_ml = [sc] if sc in SKILL_CLUSTERS else []

    ml_forecasts = []
    for bu in bu_sorted[:4]:
        for cluster in clusters_for_ml:
            base = int(random.randint(80, 400) * scale)
            ml_forecasts.append({
                "bu": bu,
                "skill_cluster": cluster,
                "months": [
                    {"month_index": j, "actual": _jitter(base) if j < 3 else 0,
                     "predicted": _jitter(base, 0.10)}
                    for j in range(6)
                ],
                "model_name": random.choice(MODELS),
            })

    top_sorted = sorted(bu_sorted, key=lambda b: random.randint(1000, 5000), reverse=True)
    return {
        "kpis": {
            "top_bus_by_demand": [{"name": b, "pct": round(random.uniform(10, 30), 1)} for b in top_sorted[:3]],
            "top_bus_by_growth": [{"name": b, "growth_pct": round(random.uniform(5, 25), 1), "unit": "yoy"} for b in top_sorted[:3]],
        },
        "trend_bu_demand": trend_bu_demand,
        "trend_bu_growth": trend_bu_growth,
        "grid_bu_demand": grid_bu_demand,
        "grid_bu_growth": grid_bu_growth,
        "ml_forecasts": ml_forecasts,
    }


# ── Geographic Distribution ───────────────────────────────────────────────────

def get_geographic_distribution(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    onsite_pct = 0.38
    offshore_pct = 0.62
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    all_country_bases = {
        "India": 18000, "US": 12000, "Philippines": 5000,
        "UK": 3500, "Poland": 2800, "Canada": 2200,
        "Australia": 1500, "Germany": 900, "Singapore": 800, "UAE": 720,
    }

    # If location filter is set, restrict to that country
    active_loc = (filters or {}).get("location")
    if active_loc and active_loc in all_country_bases:
        country_bases = {active_loc: int(all_country_bases[active_loc] * scale / _LOCATION_WEIGHTS.get(active_loc, 0.10))}
    else:
        country_bases = {c: int(b * scale) for c, b in all_country_bases.items()}

    total = sum(country_bases.values())

    # countrywise_demand
    countrywise_demand = []
    for country, base in country_bases.items():
        row: dict[str, Any] = {"country": country}
        for mn in month_names:
            row[mn] = _jitter(max(base // 6, 1))
        countrywise_demand.append(row)

    trend_mix = [
        {
            "month": MONTH_LABELS[i],
            "onsite_offsite": _jitter(max(int(total * onsite_pct / 6), 1)),
            "offshore_mix": _jitter(max(int(total * offshore_pct / 6), 1)),
        }
        for i in range(6)
    ]

    # grid: filter by skill_cluster if set
    clusters_for_grid = SKILL_CLUSTERS[:2]
    if sc := (filters or {}).get("skill_cluster"):
        clusters_for_grid = [sc] if sc in SKILL_CLUSTERS else []

    grid = []
    row_idx = 0
    for country in list(country_bases.keys())[:4]:
        for delivery in ["Onsite", "Offshore"]:
            for cluster in clusters_for_grid:
                for pa in PRACTICE_AREAS[:3]:
                    for grade in GRADES[:3]:
                        base = max(country_bases[country] // (8 * len(PRACTICE_AREAS[:3]) * len(GRADES[:3])), 1)
                        row: dict[str, Any] = {
                            "practice_area": pa,
                            "location": country,
                            "cluster": cluster,
                            "so_grade": grade,
                            "onsite_offshore": delivery,
                        }
                        for mn in month_names:
                            row[mn] = _jitter(max(base // 2, 1))
                        grid.append(row)
                        row_idx += 1

    grid = [r for r in grid if _matches_row(r, {k: v for k, v in (filters or {}).items() if k != "location"})]

    # rlc_forecasts
    rlc_forecasts = []
    for country in list(country_bases.keys())[:3]:
        for grade in ["SA", "A"]:
            for cluster in clusters_for_grid:
                base = max(country_bases[country] // 12, 1)
                rlc_forecasts.append({
                    "country": country,
                    "so_grade": grade,
                    "skill_cluster": cluster,
                    "months": [
                        {
                            "month_index": j,
                            "actual": _jitter(base) if j < 3 else 0,
                            "predicted": _predicted(_jitter(base)),
                            "predicted_corrected": max(_predicted(_jitter(base)), int(base * 0.97)),
                            "accuracy_pct": _accuracy(),
                        }
                        for j in range(6)
                    ],
                })

    total_by_country = max(sum(country_bases.values()), 1)
    return {
        "kpis": {
            "onsite_offshore": {
                "onsite_pct": round(onsite_pct * 100, 1),
                "offshore_pct": round(offshore_pct * 100, 1),
            },
            "top_countries": [
                {"country": c, "pct": round(b / total_by_country * 100, 1)}
                for c, b in list(country_bases.items())[:5]
            ],
        },
        "trend_mix": trend_mix,
        "countrywise_demand": countrywise_demand,
        "grid": grid,
        "rlc_forecasts": rlc_forecasts,
    }


# ── Skill Distribution ────────────────────────────────────────────────────────

def get_skill_distribution(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]
    active_sc = (filters or {}).get("skill_cluster")
    clusters_to_show = [active_sc] if active_sc and active_sc in SKILL_CLUSTERS else SKILL_CLUSTERS

    # short_fuse_heatmap
    short_fuse_clusters = []
    total_short_fuse = 0
    for i, cluster in enumerate(clusters_to_show):
        monthly = [int(random.randint(80, 600) * scale) for _ in range(6)]
        total_short_fuse += sum(monthly)
        short_fuse_clusters.append({
            "name": cluster.split("-")[1] if "-" in cluster else cluster,
            "monthly": monthly,
        })

    short_fuse_heatmap = {
        "total": total_short_fuse,
        "change_pct": round(random.uniform(-5, 18), 1),
        "clusters": short_fuse_clusters,
        "legend": {"low": "< 100", "medium": "100-300", "high": "> 300"},
    }

    # stability_trend: { month, stable: %, volatile: % }
    stability_trend = [
        {
            "month": MONTH_LABELS[i],
            "stable": round(random.uniform(60, 75), 1),
            "volatile": round(random.uniform(25, 40), 1),
        }
        for i in range(6)
    ]

    # grid_cluster_demand: ForecastGridRow with leaf_skill dimension
    all_cd_rows = []
    row_idx = 0
    clusters_for_cd = clusters_to_show[:4] if len(clusters_to_show) >= 4 else clusters_to_show
    for cluster in clusters_for_cd:
        idx = SKILL_CLUSTERS.index(cluster) if cluster in SKILL_CLUSTERS else 0
        for skill_entry in CLUSTER_LEAF_SKILLS.get(cluster, [])[:3]:
            for pa in PRACTICE_AREAS[:3]:
                for grade in GRADES[:3]:
                    base = int(CLUSTER_DEMANDS[idx] // 18 * scale)
                    row: dict[str, Any] = {
                        "practice_area": pa,
                        "location": COUNTRIES[row_idx % len(COUNTRIES)],
                        "cluster": cluster,
                        "so_grade": grade,
                        "leaf_skill": skill_entry["skill"],
                    }
                    for mn in month_names:
                        row[mn] = _jitter(max(base, 1))
                    all_cd_rows.append(row)
                    row_idx += 1
    grid_cluster_demand = [r for r in all_cd_rows if _matches_row(r, filters)]

    # grid_stability
    all_stab_rows = []
    row_idx = 0
    for cluster in clusters_to_show:
        idx = SKILL_CLUSTERS.index(cluster) if cluster in SKILL_CLUSTERS else 0
        cv = round(random.uniform(0.15, 1.2), 3)
        seg = "Stable" if cv < 0.5 else "Volatile"
        base = int(CLUSTER_DEMANDS[idx] // 6 * scale)
        for pa in PRACTICE_AREAS[:3]:
            for grade in GRADES[:2]:
                row: dict[str, Any] = {
                    "practice_area": pa,
                    "location": COUNTRIES[row_idx % len(COUNTRIES)],
                    "cluster": cluster,
                    "so_grade": grade,
                    "stability": seg,
                }
                for mn in month_names:
                    row[mn] = _jitter(max(base, 1))
                all_stab_rows.append(row)
                row_idx += 1
    grid_stability = [r for r in all_stab_rows if _matches_row(r, filters)]

    stable_count = sum(1 for r in grid_stability if r["stability"] == "Stable")
    total_count = max(len(grid_stability), 1)
    stable_pct = round(stable_count / total_count * 100, 1)

    total_demand = sum(CLUSTER_DEMANDS)
    top_in_practice = [
        {"name": c.split("-")[1] if "-" in c else c,
         "pct": round(CLUSTER_DEMANDS[SKILL_CLUSTERS.index(c)] / total_demand * 100, 1)}
        for c in clusters_to_show[:3]
    ]
    top_in_sl = [
        {"name": c.split("-")[1] if "-" in c else c,
         "pct": round(CLUSTER_DEMANDS[SKILL_CLUSTERS.index(c)] / total_demand * 100, 1)}
        for c in clusters_to_show[3:6]
    ] or top_in_practice

    return {
        "kpis": {
            "top_clusters_in_practice": top_in_practice,
            "top_clusters_in_sl": top_in_sl,
            "stable_vs_volatile": {"stable_pct": stable_pct, "volatile_pct": round(100 - stable_pct, 1)},
            "top_drivers": [
                "AWS/DevOps cluster demand up 18% QoQ driven by cloud migration programs",
                "Java/Spring Boot stable demand with consistent backfill pattern",
                "Mobile cluster (Android/iOS) shows high volatility — project-based spikes",
            ],
        },
        "short_fuse_heatmap": short_fuse_heatmap,
        "stability_trend": stability_trend,
        "grid_cluster_demand": grid_cluster_demand,
        "grid_stability": grid_stability,
    }


# ── Grade Distribution ────────────────────────────────────────────────────────

def get_grade_distribution(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    total = int(47420 * scale)
    grade_shares = {"GenC": 0.35, "A": 0.25, "SA": 0.20, "M": 0.12, "SM": 0.05, "AD": 0.03}
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    # If a specific grade is filtered, only show that grade
    active_grade = (filters or {}).get("grade")
    grades_to_show = [active_grade] if active_grade and active_grade in grade_shares else list(grade_shares.keys())

    grade_data = []
    for grade in grades_to_show:
        share = grade_shares[grade]
        count = max(int(total * share), 1)
        shortfuse = int(count * random.uniform(0.08, 0.18))
        grade_data.append({
            "grade": grade,
            "label": GRADE_LABELS[grade],
            "count": count,
            "pct": round(share * 100, 1),
            "shortfuse": shortfuse,
            "shortfuse_pct": round(shortfuse / count * 100, 1),
            "billable_pct": round(random.uniform(78, 95), 1),
        })

    # Filter to relevant clusters
    active_sc = (filters or {}).get("skill_cluster")
    clusters_for_heatmap = [active_sc] if active_sc and active_sc in SKILL_CLUSTERS else SKILL_CLUSTERS

    heatmap = []
    for cluster in clusters_for_heatmap:
        row: dict[str, Any] = {"cluster": cluster.split("-")[1] if "-" in cluster else cluster}
        for grade in grades_to_show:
            row[grade] = int(random.randint(50, 800) * scale)
        heatmap.append(row)

    grade_trend = [
        {
            "month": MONTH_LABELS[i],
            **{g: _jitter(max(int(total * grade_shares[g] / 6), 1)) for g in grades_to_show}
        }
        for i in range(6)
    ]

    # grid_monthly
    all_monthly = []
    row_idx = 0
    for grade_entry in grade_data:
        for cluster in clusters_for_heatmap[:3]:
            for pa in PRACTICE_AREAS[:3]:
                base = max(grade_entry["count"] // 18, 1)
                row: dict[str, Any] = {
                    "practice_area": pa,
                    "location": COUNTRIES[row_idx % len(COUNTRIES)],
                    "cluster": cluster,
                    "so_grade": grade_entry["grade"],
                }
                for mn in month_names:
                    row[mn] = _jitter(base)
                row["q01"] = row["jan"] + row["feb"] + row["mar"]
                row["q02"] = row["apr"] + row["may"] + row["jun"]
                all_monthly.append(row)
                row_idx += 1
    grid_monthly = [r for r in all_monthly if _matches_row(r, filters)]

    # grid_short_fuse
    all_sf = []
    row_idx = 0
    for grade_entry in grade_data:
        for pa in PRACTICE_AREAS[:3]:
            base_sf = max(grade_entry["shortfuse"] // 5, 1)
            cluster = clusters_for_heatmap[row_idx % len(clusters_for_heatmap)]
            row: dict[str, Any] = {
                "practice_area": pa,
                "location": COUNTRIES[row_idx % len(COUNTRIES)],
                "cluster": cluster,
                "so_grade": grade_entry["grade"],
            }
            for w in range(1, 6):
                row[f"w{w}"] = _jitter(base_sf)
            all_sf.append(row)
            row_idx += 1
    grid_short_fuse = [r for r in all_sf if _matches_row(r, filters)]

    top_grades = sorted(grade_data, key=lambda x: x["count"], reverse=True)
    return {
        "kpis": {
            "top_grade": [{"grade": d["grade"], "count": d["count"], "pct": d["pct"]} for d in top_grades[:3]],
            "shortfuse_6months": [{"grade": d["grade"], "count": d["shortfuse"]} for d in grade_data],
        },
        "grade_data": grade_data,
        "donut": [{"name": d["label"], "value": d["count"], "pct": d["pct"]} for d in grade_data],
        "shortfuse_total": sum(d["shortfuse"] for d in grade_data),
        "heatmap": heatmap,
        "grade_trend": grade_trend,
        "grid_monthly": grid_monthly,
        "grid_short_fuse": grid_short_fuse,
    }


# ── Demand Supply Gap ─────────────────────────────────────────────────────────

def get_demand_supply_gap(filters: dict | None = None) -> dict:
    scale = _filter_scale(filters)
    total_demand = max(int(47420 * scale), 1)
    supply_available = int(total_demand * 0.76)
    gap_total = total_demand - supply_available
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    active_sc = (filters or {}).get("skill_cluster")
    clusters_to_show = [active_sc] if active_sc and active_sc in SKILL_CLUSTERS else SKILL_CLUSTERS

    # short_fuse_heatmap
    sf_clusters = []
    total_sf = 0
    for cluster in clusters_to_show:
        monthly = [int(random.randint(100, 800) * scale) for _ in range(6)]
        total_sf += sum(monthly)
        sf_clusters.append({
            "name": cluster.split("-")[1] if "-" in cluster else cluster,
            "monthly": monthly,
        })

    heatmap: dict[str, Any] = {
        "total": total_sf,
        "change_pct": round(random.uniform(-8, 20), 1),
        "clusters": sf_clusters,
        "legend": {"low": "< 150", "medium": "150-400", "high": "> 400"},
    }

    # demand_supply_trend
    demand_supply_trend = [
        {
            "month": MONTH_LABELS[i],
            "demand_fte": _jitter(max(total_demand // 6, 1)),
            "supply_fte": _jitter(max(supply_available // 6, 1)),
        }
        for i in range(6)
    ]

    # grid_short_fuse
    all_sf_rows = []
    row_idx = 0
    for cluster in clusters_to_show[:4]:
        for pa in PRACTICE_AREAS[:3]:
            for grade in GRADES[:3]:
                row: dict[str, Any] = {
                    "practice_area": pa,
                    "location": COUNTRIES[row_idx % len(COUNTRIES)],
                    "cluster": cluster,
                    "so_grade": grade,
                }
                for w in range(1, 6):
                    row[f"w{w}"] = int(random.randint(40, 300) * scale)
                all_sf_rows.append(row)
                row_idx += 1
    grid_short_fuse = [r for r in all_sf_rows if _matches_row(r, filters)]

    # grid_gap
    all_gap_rows = []
    active_grade = (filters or {}).get("grade")
    grades_for_gap = [active_grade] if active_grade else ["SA", "A", "M"]
    for cluster in clusters_to_show:
        for grade in grades_for_gap:
            base_demand = max(int(random.randint(150, 1000) * scale), 1)
            base_supply = int(base_demand * random.uniform(0.62, 0.92))
            gap_val = base_demand - base_supply
            row: dict[str, Any] = {"cluster": cluster, "so_grade": grade}
            for mn in month_names:
                d = _jitter(base_demand // 6)
                s = int(d * random.uniform(0.62, 0.92))
                row[f"{mn}_demand"] = d
                row[f"{mn}_supply"] = s
            row["gap"] = gap_val
            row["gap_pct"] = round(gap_val / base_demand * 100, 1)
            row["status"] = "Critical" if gap_val / base_demand > 0.25 else ("Warning" if gap_val / base_demand > 0.15 else "OK")
            all_gap_rows.append(row)
    grid_gap = all_gap_rows

    critical_count = sum(1 for r in grid_gap if r["status"] == "Critical")
    return {
        "kpis": {
            "fulfillment_gap_pct": round(gap_total / max(total_demand, 1) * 100, 1),
            "critical_skill_shortage": critical_count,
            "fulfillment_time_days": round(random.uniform(18, 42), 1),
        },
        "heatmap": heatmap,
        "demand_supply_trend": demand_supply_trend,
        "grid_short_fuse": grid_short_fuse,
        "grid_gap": grid_gap,
    }


# ── Tasks ─────────────────────────────────────────────────────────────────────

TASK_TYPES = ["Forecast Review", "Data Validation", "Model Approval", "Scenario Review", "Alert Response", "Report Generation"]
PRIORITIES = ["High", "Medium", "Low"]
TASK_STATUSES = ["Pending", "In Progress", "Completed", "Action Required"]
ASSIGNERS = ["Priya Sharma", "James Rodriguez", "Sarah Chen", "Rahul Mehta", "Lisa Wang"]

_TASK_STATUS_MAP = ["New", "In Review", "Completed", "New"]
_VIEW_LINKS = [
    "/forecast-dashboard", "/skill-taxonomy", "/scenario-planning",
    "/my-alerts", "/skill-taxonomy", "/forecast-dashboard",
    "/forecast-dashboard", "/forecast-dashboard", "/forecast-dashboard",
    "/forecast-dashboard", "/forecast-dashboard", "/forecast-feedback",
    "/forecast-dashboard", "/my-alerts", "/forecast-dashboard",
]

_TASKS = [
    {
        "task_id": f"TASK-{100 + i}",
        "task_type": TASK_TYPES[i % len(TASK_TYPES)],
        "priority": PRIORITIES[i % len(PRIORITIES)],
        "due_date": (date(2026, 3, 19) + timedelta(days=random.randint(1, 21))).isoformat(),
        "assigned_by": random.choice(ASSIGNERS),
        "status": _TASK_STATUS_MAP[i % len(_TASK_STATUS_MAP)],
        "is_overdue": i % 5 == 0,
        "description": f"Review and validate {title.lower()} for Q2 2026 planning cycle.",
        "cluster": random.choice(SKILL_CLUSTERS),
        "view_link": _VIEW_LINKS[i % len(_VIEW_LINKS)],
    }
    for i, title in enumerate([
        "Review Q2 Forecast for Technology BU",
        "Validate AWS Cluster Demand Spike",
        "Approve Scenario Planning Submission",
        "Resolve High Gap Alert — Financial Services",
        "Update Model Weights for Java Clusters",
        "Review Backfill Demand Accuracy",
        "Generate Monthly Forecast Report",
        "Investigate YoY Decline — Media BU",
        "Confirm Grade Distribution for SM Level",
        "Validate Geographic Distribution — India",
        "Review CFT Pipeline Outputs",
        "Approve Forecast Feedback Adjustments",
        "Confirm New Demand — Healthcare BU",
        "Escalation: Supply Gap > 30% in DevOps Cluster",
        "Model Retraining Approval — AutoGluon",
    ])
]


def get_tasks() -> list[dict]:
    return _TASKS


def get_task(task_id: str) -> dict | None:
    return next((t for t in _TASKS if t["id"] == task_id), None)


# ── Alerts ────────────────────────────────────────────────────────────────────

ALERT_CATEGORIES = ["Demand Spike", "Supply Gap", "Model Accuracy", "Forecast Deviation", "Data Quality", "System"]
SEVERITIES = ["High", "Medium", "Low"]
ALERT_STATUSES = ["New", "Acknowledged", "Dismissed"]

_ALERT_STATUS_MAP = ["Action Required", "Pending Review", "Finalized", "Action Required"]
_ALERT_VIEW_LINKS = [
    "/my-alerts", "/my-alerts", "/forecast-dashboard",
    "/forecast-dashboard", "/my-alerts", "/forecast-dashboard",
    "/my-alerts", "/my-alerts", "/scenario-planning",
    "/forecast-dashboard", "/my-alerts", "/forecast-dashboard",
]

_ALERTS = [
    {
        "alert_id": f"ALT-{200 + i}",
        "alert_type": title,
        "category": ALERT_CATEGORIES[i % len(ALERT_CATEGORIES)],
        "severity": SEVERITIES[i % len(SEVERITIES)],
        "due_date": (date(2026, 3, 19) - timedelta(days=random.randint(0, 7))).isoformat(),
        "status": _ALERT_STATUS_MAP[i % len(_ALERT_STATUS_MAP)],
        "is_overdue": i % 4 == 0,
        "description": description,
        "cluster": random.choice(SKILL_CLUSTERS),
        "view_link": _ALERT_VIEW_LINKS[i % len(_ALERT_VIEW_LINKS)],
    }
    for i, (title, description) in enumerate([
        ("Demand Spike Detected — AWS Cluster", "AWS-related demand increased 34% vs baseline in Financial Services BU."),
        ("Supply Gap Critical — DevOps Skills", "Supply gap exceeded 30% threshold for MSC-AWS-DevOps cluster in M2."),
        ("Forecast Accuracy Drop Below 85%", "Model accuracy for MSC-Android-React_Native-iOS fell to 82.1%."),
        ("Large Forecast Deviation — Technology BU", "Management override deviates >20% from system forecast for M3."),
        ("Missing Data — Germany Region", "No demand submissions received from Germany for M1 window."),
        ("Model Retraining Required", "AutoGluon ensemble drift detected; retraining recommended."),
        ("New Demand Surge — Healthcare", "Healthcare BU shows +28% new demand vs prior month."),
        ("Supply Utilization Above 95%", "India headcount utilization exceeded 95% — bench critically low."),
        ("Scenario Approval Overdue", "Scenario 'Q2 Expansion' has been pending approval for 5 days."),
        ("Grade Mismatch Alert — SM Level", "SM-level demand forecast exceeds available supply by 42%."),
        ("Data Quality Issue — Retail BU", "Duplicate demand entries detected in Retail BU submission."),
        ("Forecast Horizon Extended", "CFT has extended forecast horizon to M6 for Technology clusters."),
    ])
]


def get_alerts() -> list[dict]:
    return _ALERTS


# ── Scenarios ─────────────────────────────────────────────────────────────────

_SCENARIOS = [
    {
        "id": "SCN-001",
        "name": "Q2 Revenue Expansion",
        "description": "Optimistic growth scenario with 15% revenue expansion and market entry.",
        "status": "Finalized",
        "created_by": "Sarah Chen",
        "created_at": "2026-03-10",
        "drivers": {"revenue_growth": 15.0, "market_expansion": 10.0, "headcount_change": 8.0, "tech_investment": 12.0},
        "impact": {"demand_delta": 4230, "demand_delta_pct": 8.9, "supply_gap_delta": 1200, "accuracy_impact": -1.2},
    },
    {
        "id": "SCN-002",
        "name": "Conservative H1 Plan",
        "description": "Risk-adjusted conservative scenario with flat growth assumptions.",
        "status": "In Review",
        "created_by": "James Rodriguez",
        "created_at": "2026-03-14",
        "drivers": {"revenue_growth": 2.0, "market_expansion": 0.0, "headcount_change": -2.0, "tech_investment": 3.0},
        "impact": {"demand_delta": -850, "demand_delta_pct": -1.8, "supply_gap_delta": -300, "accuracy_impact": 0.5},
    },
    {
        "id": "SCN-003",
        "name": "Tech Talent Surge",
        "description": "High investment in cloud and AI capabilities driving demand surge.",
        "status": "Draft",
        "created_by": "Priya Sharma",
        "created_at": "2026-03-17",
        "drivers": {"revenue_growth": 10.0, "market_expansion": 5.0, "headcount_change": 15.0, "tech_investment": 25.0},
        "impact": {"demand_delta": 7100, "demand_delta_pct": 15.0, "supply_gap_delta": 3200, "accuracy_impact": -2.1},
    },
]


def get_scenarios() -> list[dict]:
    return _SCENARIOS


def compute_scenario_impact(drivers: dict) -> dict:
    demand_delta_pct = (
        drivers.get("revenue_growth", 0) * 0.4
        + drivers.get("market_expansion", 0) * 0.3
        + drivers.get("headcount_change", 0) * 0.2
        + drivers.get("tech_investment", 0) * 0.1
    )
    base_demand = 47420
    demand_delta = int(base_demand * demand_delta_pct / 100)
    return {
        "demand_delta": demand_delta,
        "demand_delta_pct": round(demand_delta_pct, 2),
        "supply_gap_delta": int(demand_delta * 0.55),
        "accuracy_impact": round(-abs(demand_delta_pct) * 0.08, 2),
    }


# ── Feedback ──────────────────────────────────────────────────────────────────

_FEEDBACK = [
    {
        "id": f"FB-{300 + i}",
        "month": MONTH_LABELS[i % 6],
        "cluster": SKILL_CLUSTERS[i % len(SKILL_CLUSTERS)],
        "system_forecast": base,
        "mgmt_adjustment": int(base * adj),
        "final_forecast": int(base * adj),
        "reason": reason,
        "status": status,
        "submitted_by": random.choice(ASSIGNERS),
        "submitted_at": (date(2026, 3, 19) - timedelta(days=i * 3)).isoformat(),
    }
    for i, (base, adj, reason, status) in enumerate([
        (7800, 1.10, "Market expansion in APAC driving higher cloud demand", "Approved"),
        (6200, 0.95, "Budget freeze in Q1 reducing new headcount", "Approved"),
        (4900, 1.05, "New React projects onboarding in Retail BU", "Pending"),
        (1350, 1.20, "Strategic cloud migration program accelerated", "Pending"),
        (5100, 0.90, "Microservices program paused for architecture review", "Rejected"),
        (15000, 1.08, "Large government contracts confirmed for Q2", "Approved"),
        (2200, 1.15, "Mobile-first initiatives across 3 BUs", "Pending"),
        (1900, 1.00, "No management adjustment — system forecast accepted", "Approved"),
        (2600, 0.85, "SQL modernization projects delayed to Q3", "Rejected"),
    ])
]


def get_feedback() -> list[dict]:
    return _FEEDBACK


# ── Executive Summary ─────────────────────────────────────────────────────────

def get_executive_summary(filters: dict | None = None) -> dict:  # noqa: C901
    scale = _filter_scale(filters)
    active_sc = (filters or {}).get("skill_cluster")

    if active_sc and active_sc in SKILL_CLUSTERS:
        idx = SKILL_CLUSTERS.index(active_sc)
        non_sc_filters = {k: v for k, v in (filters or {}).items() if k != "skill_cluster"}
        non_sc_scale = _filter_scale(non_sc_filters)
        total = max(int(CLUSTER_DEMANDS[idx] * non_sc_scale), 1)
    else:
        total = max(int(47420 * scale), 1)

    supply = int(total * 0.76)

    # ── Section 1: Forecast Overview KPIs ─────────────────────────────────────
    all_clusters = [
        {"cluster": SKILL_CLUSTERS[i], "demand": max(int(CLUSTER_DEMANDS[i] * scale), 1),
         "growth_pct": round(random.uniform(2, 22), 1)}
        for i in range(len(SKILL_CLUSTERS))
    ]
    filtered_clusters = [c for c in all_clusters if c["cluster"] == active_sc] if active_sc else all_clusters
    top_clusters = sorted(filtered_clusters, key=lambda x: x["demand"], reverse=True)[:5]

    active_pa = (filters or {}).get("practice_area")
    if active_pa:
        top_practice_areas = [{"name": active_pa, "pct": 100}]
    else:
        top_practice_areas = [
            {"name": "ADM Central", "pct": 35}, {"name": "Digital Engineering", "pct": 30},
            {"name": "EPS IPM", "pct": 20}, {"name": "Consulting", "pct": 10}, {"name": "Other", "pct": 5},
        ]

    mom_change = round(random.uniform(4.0, 8.4), 1)

    # ── Section 2: BU Performance ─────────────────────────────────────────────
    active_bu = (filters or {}).get("bu")
    bus_pool = [active_bu] if active_bu else BUSINESS_UNITS[:6]
    top_bus_by_demand = [
        {"name": bu, "demand": max(int(random.randint(3000, 12000) * scale), 1),
         "pct": round(random.uniform(8, 28), 1)}
        for bu in bus_pool[:5]
    ]
    top_bus_by_demand.sort(key=lambda x: x["demand"], reverse=True)
    top_bus_by_growth = [
        {"name": bu, "growth_pct": round(random.uniform(3, 22), 1), "unit": "yoy"}
        for bu in bus_pool[:5]
    ]
    top_bus_by_growth.sort(key=lambda x: x["growth_pct"], reverse=True)

    # ── Section 3: Geographic ─────────────────────────────────────────────────
    active_loc = (filters or {}).get("location")
    country_bases = {
        "India": 18000, "US": 12000, "Philippines": 5000,
        "UK": 3500, "Poland": 2800, "Canada": 2200,
        "Australia": 1500, "Germany": 900, "Singapore": 800, "UAE": 720,
    }
    if active_loc and active_loc in country_bases:
        geo_countries = {active_loc: int(country_bases[active_loc] * scale / _LOCATION_WEIGHTS.get(active_loc, 0.1))}
    else:
        geo_countries = {c: max(int(b * scale), 1) for c, b in country_bases.items()}
    total_geo = max(sum(geo_countries.values()), 1)
    top_countries = [
        {"country": c, "demand": d, "pct": round(d / total_geo * 100, 1)}
        for c, d in sorted(geo_countries.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    # ── Section 4: Skill Distribution ────────────────────────────────────────
    stable_pct = round(random.uniform(62, 75), 1)
    top_clusters_in_sl = sorted(filtered_clusters, key=lambda x: x["demand"], reverse=True)[2:5]

    # ── Section 5: Grade Distribution ────────────────────────────────────────
    grade_shares = {"GenC": 0.35, "A": 0.25, "SA": 0.20, "M": 0.12, "SM": 0.05, "AD": 0.03}
    active_grade = (filters or {}).get("grade")
    if active_grade and active_grade in grade_shares:
        grade_donut = [{"grade": active_grade, "label": GRADE_LABELS[active_grade],
                        "count": max(int(total * grade_shares[active_grade]), 1), "pct": 100.0}]
    else:
        grade_donut = [
            {"grade": g, "label": GRADE_LABELS[g],
             "count": max(int(total * s), 1), "pct": round(s * 100, 1)}
            for g, s in grade_shares.items()
        ]
    shortfuse_by_grade = [
        {"grade": d["grade"], "count": max(int(d["count"] * random.uniform(0.08, 0.18)), 1)}
        for d in grade_donut
    ]
    shortfuse_total = sum(d["count"] for d in shortfuse_by_grade)

    # ── Section 6: Demand Type ────────────────────────────────────────────────
    new_demand_pct = 65
    backfill_pct = 35
    contract_type_mix = [
        {"name": "T&M", "pct": 55}, {"name": "Fixed Price", "pct": 30}, {"name": "Transaction", "pct": 15},
    ]

    recent_alerts = _ALERTS[:5]
    pending_tasks = [t for t in _TASKS if t["status"] in ("New", "In Review")][:5]

    return {
        # Section 1 — Forecast Overview
        "total_fte_demand": total,
        "forecast_accuracy": 89.3,
        "avg_cancellation_pct": 40.0,
        "mom_change": mom_change,
        "demand_vs_supply": {"demand": total, "supply": supply, "gap": total - supply,
                             "gap_pct": round((total - supply) / max(total, 1) * 100, 1)},
        "top_practice_areas": top_practice_areas,
        # Section 2 — BU Performance
        "bu_performance": {"top_by_demand": top_bus_by_demand, "top_by_growth": top_bus_by_growth},
        # Section 3 — Geographic
        "geographic": {"onsite_pct": 38.0, "offshore_pct": 62.0, "top_countries": top_countries},
        # Section 4 — Skill Distribution
        "skill_distribution": {"top_clusters": top_clusters, "top_clusters_in_sl": top_clusters_in_sl,
                               "stable_pct": stable_pct, "volatile_pct": round(100 - stable_pct, 1)},
        # Section 5 — Grade Distribution
        "grade_distribution": {"grade_donut": grade_donut, "shortfuse_by_grade": shortfuse_by_grade,
                               "shortfuse_total": shortfuse_total},
        # Section 6 — Demand Type
        "demand_type": {"new_demand_pct": new_demand_pct, "backfill_pct": backfill_pct,
                        "contract_type_mix": contract_type_mix},
        # Section 7 — Tasks & Alerts
        "recent_alerts": recent_alerts,
        "pending_tasks": pending_tasks,
        # Legacy fields (backward compat)
        "top_clusters": top_clusters,
    }
