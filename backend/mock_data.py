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
    total_demand = 47420
    monthly_totals = _trend_series(total_demand // 6, 6, 0.025)
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]

    # Grid rows: dimension columns + monthly demand columns
    grid_rows = []
    for i, cluster in enumerate(SKILL_CLUSTERS[:5]):
        for pa in PRACTICE_AREAS[:2]:
            for grade in ["SA", "A", "M"]:
                base = CLUSTER_DEMANDS[i] // 18
                row: dict[str, Any] = {
                    "practice_area": pa,
                    "location": "Americas",
                    "cluster": cluster,
                    "so_grade": grade,
                }
                for j, mn in enumerate(month_names):
                    row[mn.lower()] = _jitter(base)
                row["q01"] = row["jan"] + row["feb"] + row["mar"]
                row["q02"] = row["apr"] + row["may"] + row["jun"]
                grid_rows.append(row)

    # Per-cluster M0-M5 ML predictions (forecasts_by_cluster)
    forecasts_by_cluster = []
    for i, cluster in enumerate(SKILL_CLUSTERS):
        base = CLUSTER_DEMANDS[i] // 6
        months_data = []
        for j in range(6):
            actual = _jitter(base) if j < 3 else 0
            predicted = _predicted(_jitter(base))
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
            {"week": f"W{i+1}", "fte_demand": _jitter(total_demand // 24, 0.08),
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
        "forecasts_by_cluster": forecasts_by_cluster,
    }


# ── Demand Type Breakdown ─────────────────────────────────────────────────────

def get_demand_type_breakdown(filters: dict | None = None) -> dict:
    total = 47420
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    def _grid_rows(extra_key: str, extra_val: str) -> list:
        rows = []
        for cluster in SKILL_CLUSTERS[:4]:
            for grade in ["SA", "A"]:
                base = random.randint(200, 800)
                row: dict[str, Any] = {
                    "practice_area": random.choice(PRACTICE_AREAS[:3]),
                    "location": "Americas",
                    "cluster": cluster,
                    "so_grade": grade,
                    extra_key: extra_val,
                }
                for mn in month_names:
                    row[mn] = _jitter(base // 6)
                row["q01"] = row["jan"] + row["feb"] + row["mar"]
                rows.append(row)
        return rows

    return {
        "kpis": {
            "new_vs_backfill": {"new_pct": 65, "backfill_pct": 35},
            "contract_type_mix": {"t_and_m_pct": 55, "fixed_price_pct": 30, "transaction_based_pct": 15},
        },
        "trend_new_vs_backfill": [
            {"month": MONTH_LABELS[i],
             "new_demand": _jitter(int(total * 0.65 / 6)),
             "backfill": _jitter(int(total * 0.35 / 6))}
            for i in range(6)
        ],
        "trend_billability": [
            {"month": MONTH_LABELS[i],
             "bfd": _jitter(int(total * 0.55 / 6)),
             "btb": _jitter(int(total * 0.30 / 6)),
             "btm": _jitter(int(total * 0.15 / 6))}
            for i in range(6)
        ],
        "grid_new_vs_backfill": _grid_rows("demand_type", "New Demand") + _grid_rows("demand_type", "Backfill"),
        "grid_billability": _grid_rows("billability_type", "BFD") + _grid_rows("billability_type", "BTB"),
    }


# ── BU Performance ────────────────────────────────────────────────────────────

def get_bu_performance(filters: dict | None = None) -> dict:
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]
    bu_sorted = sorted(BUSINESS_UNITS[:8], key=lambda _: random.randint(1000, 5000), reverse=True)

    trend_bu_demand = []
    trend_bu_growth = []
    for i in range(6):
        d_row: dict[str, Any] = {"month": MONTH_LABELS[i]}
        g_row: dict[str, Any] = {"month": MONTH_LABELS[i]}
        for bu in bu_sorted[:5]:
            d_row[bu] = _jitter(random.randint(400, 1200))
            g_row[bu] = round(random.uniform(-3, 18), 1)
        trend_bu_demand.append(d_row)
        trend_bu_growth.append(g_row)

    def _bu_grid(bu_name: str) -> list:
        rows = []
        for cluster in SKILL_CLUSTERS[:3]:
            for grade in ["SA", "A"]:
                base = random.randint(150, 600)
                row: dict[str, Any] = {
                    "practice_area": random.choice(PRACTICE_AREAS[:2]),
                    "location": "Americas",
                    "cluster": cluster,
                    "so_grade": grade,
                    "business_unit": bu_name,
                }
                for mn in month_names:
                    row[mn] = _jitter(base // 6)
                rows.append(row)
        return rows

    grid_bu_demand, grid_bu_growth = [], []
    for bu in bu_sorted[:3]:
        grid_bu_demand.extend(_bu_grid(bu))
        grid_bu_growth.extend(_bu_grid(bu))

    ml_forecasts = []
    for bu in bu_sorted[:4]:
        for cluster in SKILL_CLUSTERS[:2]:
            base = random.randint(80, 400)
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
    total = 47420
    onsite_pct = 0.38
    offshore_pct = 0.62
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    country_bases = {
        "India": 18000, "US": 12000, "Philippines": 5000,
        "UK": 3500, "Poland": 2800, "Canada": 2200,
        "Australia": 1500, "Germany": 900, "Singapore": 800, "UAE": 720,
    }

    # countrywise_demand: { country, jan, feb, ... }
    countrywise_demand = []
    for country, base in country_bases.items():
        row: dict[str, Any] = {"country": country}
        for mn in month_names:
            row[mn] = _jitter(base // 6)
        countrywise_demand.append(row)

    # trend_mix: { month, onsite_offsite, offshore_mix }
    trend_mix = [
        {
            "month": MONTH_LABELS[i],
            "onsite_offsite": _jitter(int(total * onsite_pct / 6)),
            "offshore_mix": _jitter(int(total * offshore_pct / 6)),
        }
        for i in range(6)
    ]

    # grid: ForecastGridRow[] with onsite_offshore dimension
    grid = []
    for country in list(country_bases.keys())[:4]:
        for delivery in ["Onsite", "Offshore"]:
            for cluster in SKILL_CLUSTERS[:2]:
                base = country_bases[country] // 8
                row: dict[str, Any] = {
                    "practice_area": random.choice(PRACTICE_AREAS[:3]),
                    "location": "Americas" if country in ["US", "Canada"] else "EMEA" if country in ["UK", "Germany", "Poland"] else "APAC",
                    "cluster": cluster,
                    "so_grade": random.choice(["SA", "A", "M"]),
                    "onsite_offshore": delivery,
                }
                for mn in month_names:
                    row[mn] = _jitter(base // 2)
                grid.append(row)

    # rlc_forecasts: country + so_grade + skill_cluster M0-M5
    rlc_forecasts = []
    for country in list(country_bases.keys())[:3]:
        for grade in ["SA", "A"]:
            for cluster in SKILL_CLUSTERS[:2]:
                base = country_bases[country] // 12
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

    total_by_country = sum(country_bases.values())
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
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    # short_fuse_heatmap: clusters with monthly demand counts (short-fuse = RSD < 6 weeks)
    short_fuse_clusters = []
    total_short_fuse = 0
    for i, cluster in enumerate(SKILL_CLUSTERS):
        monthly = [random.randint(80, 600) for _ in range(6)]
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
    grid_cluster_demand = []
    for i, cluster in enumerate(SKILL_CLUSTERS[:4]):
        for skill_entry in CLUSTER_LEAF_SKILLS.get(cluster, [])[:3]:
            base = CLUSTER_DEMANDS[i] // 18
            row: dict[str, Any] = {
                "practice_area": random.choice(PRACTICE_AREAS[:3]),
                "location": "Americas",
                "cluster": cluster,
                "so_grade": random.choice(["SA", "A", "M"]),
                "leaf_skill": skill_entry["skill"],
            }
            for mn in month_names:
                row[mn] = _jitter(base)
            grid_cluster_demand.append(row)

    # grid_stability: ForecastGridRow with stability dimension
    grid_stability = []
    for i, cluster in enumerate(SKILL_CLUSTERS):
        cv = round(random.uniform(0.15, 1.2), 3)
        seg = "Stable" if cv < 0.5 else "Volatile"
        base = CLUSTER_DEMANDS[i] // 6
        row: dict[str, Any] = {
            "practice_area": random.choice(PRACTICE_AREAS[:3]),
            "location": "Americas",
            "cluster": cluster,
            "so_grade": random.choice(["SA", "A"]),
            "stability": seg,
        }
        for mn in month_names:
            row[mn] = _jitter(base)
        grid_stability.append(row)

    stable_count = sum(1 for r in grid_stability if r["stability"] == "Stable")
    total_count = len(grid_stability)
    stable_pct = round(stable_count / total_count * 100, 1)

    return {
        "kpis": {
            "top_clusters_in_practice": [
                {"name": SKILL_CLUSTERS[i].split("-")[1] if "-" in SKILL_CLUSTERS[i] else SKILL_CLUSTERS[i],
                 "pct": round(CLUSTER_DEMANDS[i] / sum(CLUSTER_DEMANDS) * 100, 1)}
                for i in range(3)
            ],
            "top_clusters_in_sl": [
                {"name": SKILL_CLUSTERS[i + 3].split("-")[1] if "-" in SKILL_CLUSTERS[i + 3] else SKILL_CLUSTERS[i + 3],
                 "pct": round(CLUSTER_DEMANDS[i + 3] / sum(CLUSTER_DEMANDS) * 100, 1)}
                for i in range(3)
            ],
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
    total = 47420
    grade_shares = {"GenC": 0.35, "A": 0.25, "SA": 0.20, "M": 0.12, "SM": 0.05, "AD": 0.03}
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    grade_data = []
    for grade, share in grade_shares.items():
        count = int(total * share)
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

    heatmap = []
    for cluster in SKILL_CLUSTERS:
        row: dict[str, Any] = {"cluster": cluster.split("-")[1] if "-" in cluster else cluster}
        for grade in GRADES:
            row[grade] = random.randint(50, 800)
        heatmap.append(row)

    grade_trend = [
        {
            "month": MONTH_LABELS[i],
            **{g: _jitter(int(total * grade_shares[g] / 6)) for g in GRADES}
        }
        for i in range(6)
    ]

    # grid_monthly: standard demand grid with so_grade dimension
    grid_monthly = []
    for grade_entry in grade_data:
        for cluster in SKILL_CLUSTERS[:3]:
            base = grade_entry["count"] // 18
            row: dict[str, Any] = {
                "practice_area": random.choice(PRACTICE_AREAS[:3]),
                "location": "Americas",
                "cluster": cluster,
                "so_grade": grade_entry["grade"],
            }
            for mn in month_names:
                row[mn] = _jitter(base)
            row["q01"] = row["jan"] + row["feb"] + row["mar"]
            row["q02"] = row["apr"] + row["may"] + row["jun"]
            grid_monthly.append(row)

    # grid_short_fuse: weekly view (W1-W5) for short-fuse demand
    grid_short_fuse = []
    for grade_entry in grade_data:
        base_sf = grade_entry["shortfuse"] // 5
        row: dict[str, Any] = {
            "practice_area": random.choice(PRACTICE_AREAS[:3]),
            "location": "Americas",
            "cluster": random.choice(SKILL_CLUSTERS),
            "so_grade": grade_entry["grade"],
        }
        for w in range(1, 6):
            row[f"w{w}"] = _jitter(base_sf)
        grid_short_fuse.append(row)

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
    total_demand = 47420
    supply_available = int(total_demand * 0.76)
    gap_total = total_demand - supply_available
    month_names = ["jan", "feb", "mar", "apr", "may", "jun"]

    # short_fuse_heatmap: ShortFuseHeatmap shape (matches SkillDistributionData)
    sf_clusters = []
    total_sf = 0
    for cluster in SKILL_CLUSTERS:
        monthly = [random.randint(100, 800) for _ in range(6)]
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

    # demand_supply_trend: { month, demand_fte, supply_fte }
    demand_supply_trend = [
        {
            "month": MONTH_LABELS[i],
            "demand_fte": _jitter(total_demand // 6),
            "supply_fte": _jitter(supply_available // 6),
        }
        for i in range(6)
    ]

    # grid_short_fuse: weekly short-fuse view
    grid_short_fuse = []
    for cluster in SKILL_CLUSTERS[:4]:
        for grade in ["SA", "A"]:
            row: dict[str, Any] = {
                "practice_area": random.choice(PRACTICE_AREAS[:3]),
                "location": "Americas",
                "cluster": cluster,
                "so_grade": grade,
            }
            for w in range(1, 6):
                row[f"w{w}"] = random.randint(40, 300)
            grid_short_fuse.append(row)

    # grid_gap: GapRow shape — cluster + so_grade + monthly demand/supply
    grid_gap = []
    for cluster in SKILL_CLUSTERS:
        for grade in ["SA", "A", "M"]:
            base_demand = random.randint(150, 1000)
            base_supply = int(base_demand * random.uniform(0.62, 0.92))
            gap_val = base_demand - base_supply
            row: dict[str, Any] = {
                "cluster": cluster,
                "so_grade": grade,
            }
            for mn in month_names:
                d = _jitter(base_demand // 6)
                s = int(d * random.uniform(0.62, 0.92))
                row[f"{mn}_demand"] = d
                row[f"{mn}_supply"] = s
            row["gap"] = gap_val
            row["gap_pct"] = round(gap_val / base_demand * 100, 1)
            row["status"] = "Critical" if gap_val / base_demand > 0.25 else ("Warning" if gap_val / base_demand > 0.15 else "OK")
            grid_gap.append(row)

    critical_count = sum(1 for r in grid_gap if r["status"] == "Critical")
    return {
        "kpis": {
            "fulfillment_gap_pct": round(gap_total / total_demand * 100, 1),
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

def get_executive_summary() -> dict:
    total = 47420
    supply = int(total * 0.76)

    top_clusters = sorted(
        [
            {"cluster": SKILL_CLUSTERS[i], "demand": CLUSTER_DEMANDS[i],
             "growth_pct": round(random.uniform(2, 22), 1)}
            for i in range(len(SKILL_CLUSTERS))
        ],
        key=lambda x: x["demand"],
        reverse=True,
    )[:5]

    top_practice_areas = [
        {"name": "ADM Central", "pct": 35},
        {"name": "Digital Engineering", "pct": 30},
        {"name": "EPS IPM", "pct": 20},
        {"name": "Consulting", "pct": 10},
        {"name": "Other", "pct": 5},
    ]

    recent_alerts = _ALERTS[:5]
    pending_tasks = [t for t in _TASKS if t["status"] in ("New", "In Review")][:5]

    return {
        "total_fte_demand": total,
        "forecast_accuracy": 89.3,
        "top_clusters": top_clusters,
        "top_practice_areas": top_practice_areas,
        "demand_vs_supply": {
            "demand": total,
            "supply": supply,
            "gap": total - supply,
            "gap_pct": round((total - supply) / total * 100, 1),
        },
        "recent_alerts": recent_alerts,
        "pending_tasks": pending_tasks,
    }
