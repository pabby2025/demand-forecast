"""
Apply skill clusters to the market CSV and write applied metrics + CSV.

Normal mode
-----------
Reads skills/{Market}_{PA}/skill_clusters.json (final clusters, optionally human-edited),
loads the normalized CSV from data/{PA}/, assigns each row to the best-matching cluster
using Jaccard similarity (>= a length-dependent threshold).  Rows that contain a priority
skill (e.g. Android/iOS/SharePoint/SAP) are force-assigned to the cluster that holds that
skill, bypassing the Jaccard check entirely.  A second pass then attempts to map any still-
unmapped rows by computing Jaccard on a restricted set of primary skills only.

Outputs (normal mode, per market):
  - skills/{Market}_{PA}/skill_clusters_applied.json   (metrics + cluster list)
  - data/{PA}/DFC_YTD_{yr}_{PA}_V2_skill_clusters_{Market}.csv   (mapped rows)
  - data/{PA}/DFC_YTD_{yr}_{PA}_V2_unmapped_{Market}.csv         (unmapped rows)

Unmapped mode (--unmapped)
--------------------------
Reads unmapped_clusters/skill_clusters.json together with the v2_unmapped CSV produced
by a prior normal-mode run.  The same Jaccard assignment logic applies, but priority-skill
force-assignment and the primary-skills second pass are both disabled (the unmapped CSV
already had those opportunities in the normal run).  Relaxed Jaccard thresholds are used
to maximise coverage while targeting only ~1-2% loss.

Outputs (unmapped mode, per market):
  - skills/{Market}_{PA}/unmapped_clusters/skill_clusters_applied.json
  - data/{PA}/DFC_YTD_{yr}_{PA}_V2_unmapped_clusters_{Market}.csv   (newly mapped rows)
  - data/{PA}/DFC_YTD_{yr}_{PA}_V2_still_unmapped_{Market}.csv      (remainder)

Inputs (per market):
  - skills/{Market}_{PA}/skill_clusters.json
  - data/{PA}/DFC_YTD_{yr}_{PA}_V2_corrected_normalized_{Market}.csv

Usage:
  python apply_clusters.py [--unmapped] [--practice-area PA] [--year-range YYYY-YYYY]
                           [--priority-skills s1,s2] [--exclude-skills s1,s2]
                           [--primary-skills s1,s2]
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import coloredlogs
import pandas as pd

from skill_clusters_demand import (
    DATA_DIR,
    DEMAND_CLUSTER_COL,
    SKILLS_DIR,
    TIME_COL,
    assign_rows_exclusive,
    build_row_skill_sets,
    compute_cluster_cv,
    compute_cluster_skill_occurrence,
    jaccard,
    load_market_csv,
)

APPLIED_JSON_NAME = "skill_clusters_applied.json"
SKILL_CLUSTERS_JSON = "skill_clusters.json"
UNMAPPED_CLUSTERS_SUBDIR = "unmapped_clusters"

MARKETS = ["Americas", "EMEA"]

# PA-specific skills to exclude from row data and clusters (stripped before Jaccard / assignment).
PA_EXCLUDE_SKILLS: Dict[str, List[str]] = {
    "DE":  ["API Development"],
    "ADM": ["API Development"],
    "EPS": ["API Development"],
}

# PA-specific priority skills (force-assign rows containing these to the cluster that holds them).
# Rows with a priority skill are hard-assigned regardless of Jaccard.
PA_PRIORITY_SKILLS: Dict[str, List[str]] = {
    "DE":  ["SharePoint"],
    "ADM": ["COBOL", "ServiceNow"],
    "EPS": ["SAP", "Pega"],
}

# PA-specific primary skills for second-pass assignment of unmapped rows (Jaccard on these only).
PA_PRIMARY_SKILLS: Dict[str, List[str]] = {
    "DE": [
        "Java", "Spring Boot", "JavaScript", "React", "Angular", ".NET", "C#",
        "AWS", "Azure", "SQL", "Python", "TypeScript", "Microservices", "Kafka", "DevOps",
    ],
    "ADM": [
        "Java", "Spring Boot", ".NET", "COBOL", "Oracle", "SQL", "DB2",
        "ServiceNow", "Azure", "AWS", "Python", "Microservices",
        "Shell Scripting/Linux", "Angular", "Node JS", "JavaScript",
    ],
    "EPS": [
        "SAP", "ABAP", "Oracle", "Pega", "Java", "Spring Boot", "SQL",
        "Azure", "AWS", ".NET", "Python", "Microservices", "JavaScript", "ServiceNow",
    ],
}

# Fallback defaults (DE behaviour) used when PA not found in the dicts above.
_DEFAULT_PA = "DE"

# ---------------------------------------------------------------------------
# Length-wise Jaccard thresholds (edit in-code when needed)
# ---------------------------------------------------------------------------
# Normal-mode thresholds: use a higher bar (0.4) when the union of row skills
# and cluster skills is small (< union_size_threshold), and a slightly relaxed
# bar (0.3) when the union is large enough that even a moderate overlap is
# meaningful.  Larger unions are less likely to match by chance, so a lower
# threshold is safe.
JACCARD_BY_UNION_BY_MARKET = {
    "Americas": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.3, "jaccard_when_union_lt": 0.4},
    "EMEA": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.3, "jaccard_when_union_lt": 0.4},
}
# Relaxed Jaccard for unmapped pass (~1-2% loss target).
# Both branches use the same value (0.20) so the threshold is effectively flat,
# prioritising recall over precision for rows that survived the tighter first pass.
JACCARD_BY_UNION_UNMAPPED = {
    "Americas": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.20, "jaccard_when_union_lt": 0.2},
    "EMEA": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.20, "jaccard_when_union_lt": 0.2},
}

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
coloredlogs.install(level=logging.INFO, logger=LOGGER, isatty=True)


def _min_jaccard_for_pair(
    rset: Set[str],
    cset: Set[str],
    min_jaccard: float,
    jaccard_by_union: Optional[Dict[str, Any]] = None,
) -> float:
    """Return the minimum Jaccard threshold to use for a specific row-cluster pair.

    When ``jaccard_by_union`` is provided the threshold is chosen dynamically based
    on the size of the union of the row skill set and the cluster skill set:

    - If  |rset ∪ cset| >= union_size_threshold  →  use ``jaccard_when_union_ge``
      (the *relaxed* threshold, because a large union makes random overlap unlikely).
    - If  |rset ∪ cset| <  union_size_threshold  →  use ``jaccard_when_union_lt``
      (the *stricter* threshold, because small unions inflate Jaccard artificially).

    When ``jaccard_by_union`` is None the single flat ``min_jaccard`` value is
    returned unchanged (backwards-compatible behaviour).
    """
    if jaccard_by_union is None:
        return min_jaccard
    threshold = int(jaccard_by_union.get("union_size_threshold", 7))
    ge_val = float(jaccard_by_union.get("jaccard_when_union_ge", 0.3))
    lt_val = float(jaccard_by_union.get("jaccard_when_union_lt", 0.4))
    union_size = len(rset | cset)
    return ge_val if union_size >= threshold else lt_val


def assign_unmapped_primary_skills_pass(
    row_sets: List[Set[str]],
    chosen_clusters: List[List[str]],
    row_to_cluster: List[int],
    primary_skills: Set[str],
    min_jaccard: float,
    jaccard_by_union: Optional[Dict[str, Any]] = None,
) -> int:
    """Attempt a second assignment pass for rows that were not mapped in the first pass.

    Why a second pass is needed
    ---------------------------
    The first pass computes Jaccard over the *full* skill set of a row.  Rows
    that contain many niche or rare skills may share only a few skills with any
    cluster, producing a low overall Jaccard even when the row's *core* technology
    stack matches a cluster well.  By restricting the Jaccard computation to the
    curated ``primary_skills`` list (mainstream technologies with broad cluster
    coverage) this pass can recover those rows without loosening the threshold for
    the well-specified rows that were already mapped.

    Algorithm
    ---------
    For each still-unmapped row (``row_to_cluster[i] < 0``):
      1. Intersect the row's skill set with ``primary_skills`` → ``r_primary``.
         Skip the row if it has no primary skills at all (nothing to compare).
      2. For every cluster, intersect the cluster's skill set with ``primary_skills``
         → ``c_primary``, then compute ``jaccard(r_primary, c_primary)``.
      3. Collect all clusters whose Jaccard meets the length-dependent threshold.
      4. Among those candidates, pick the one with the highest Jaccard; break ties
         by preferring the smallest cluster (then lowest index) to keep clusters tight.
      5. Write the winning cluster index back into ``row_to_cluster[i]`` in place.

    Parameters
    ----------
    row_sets : list of sets
        Full skill sets for every row (same order as ``row_to_cluster``).
    chosen_clusters : list of lists
        Skill lists for every cluster.
    row_to_cluster : list of int
        Mutable assignment array; -1 means unmapped.  Updated in place.
    primary_skills : set of str
        The curated set of primary/core skills used to restrict Jaccard.
    min_jaccard : float
        Flat fallback threshold (used when ``jaccard_by_union`` is None).
    jaccard_by_union : dict or None
        Length-dependent threshold config; passed to ``_min_jaccard_for_pair``.

    Returns
    -------
    int
        Number of rows newly assigned by this pass.
    """
    cluster_sets = [set(cl) for cl in chosen_clusters]
    cluster_sizes = [len(cl) for cl in chosen_clusters]
    count = 0
    for i in range(len(row_to_cluster)):
        if row_to_cluster[i] >= 0:
            # Row already mapped in the first pass; skip.
            continue
        rset = row_sets[i]
        r_primary = rset & primary_skills
        if not r_primary:
            # Row has no primary skills; cannot meaningfully compare — skip.
            continue
        candidates = []
        for idx, cset in enumerate(cluster_sets):
            c_primary = cset & primary_skills
            j = jaccard(r_primary, c_primary)
            min_j = _min_jaccard_for_pair(r_primary, c_primary, min_jaccard, jaccard_by_union)
            if j >= min_j:
                candidates.append((idx, j))
        if not candidates:
            continue
        max_j = max(c[1] for c in candidates)
        best_candidates = [c for c in candidates if c[1] == max_j]
        # Tie-break: prefer the smallest cluster (then lowest index) to avoid
        # inflating already-large clusters and to keep the result deterministic.
        best_idx = min(best_candidates, key=lambda c: (cluster_sizes[c[0]], c[0]))[0]
        row_to_cluster[i] = best_idx
        count += 1
    return count


def load_clusters(clusters_path: Path, exclude_skills: Optional[Set[str]] = None) -> List[List[str]]:
    """Load skill_clusters.json and return the cluster skill lists.

    Why exclude_skills are stripped before use
    ------------------------------------------
    Certain skills (e.g. "API Development" for DE/ADM/EPS) are near-universal
    across almost every job posting in the PA and therefore carry very little
    discriminative signal.  If they were left in the cluster definitions they
    would inflate Jaccard scores uniformly across *all* clusters, making it
    harder to distinguish which cluster a row truly belongs to.  Removing them
    from both the loaded clusters (here) *and* the row skill sets (in
    ``build_row_skill_sets``) ensures they are invisible to the Jaccard and
    assignment logic, while the underlying data remains unchanged on disk.

    Parameters
    ----------
    clusters_path : Path
        Path to the ``skill_clusters.json`` file to read.
    exclude_skills : set of str or None
        Skills to strip from every cluster after loading.  If None, no skills
        are removed.

    Returns
    -------
    list of list of str
        One inner list per cluster, each containing the retained skill names.

    Raises
    ------
    FileNotFoundError
        If ``clusters_path`` does not exist.
    """
    if not clusters_path.exists():
        raise FileNotFoundError(f"Clusters file not found: {clusters_path}")
    with clusters_path.open(encoding="utf-8") as f:
        data = json.load(f)
    clusters = [list(cl) for cl in data.get("clusters", [])]
    if exclude_skills:
        # Strip excluded skills from every cluster so they play no role in
        # Jaccard similarity or cluster assignment downstream.
        clusters = [[s for s in cl if s not in exclude_skills] for cl in clusters]
    return clusters


def run_market(
    market: str,
    unmapped_mode: bool = False,
    priority_skills: Optional[List[str]] = None,
    exclude_skills: Optional[Set[str]] = None,
    primary_skills_override: Optional[List[str]] = None,
    pa: str = "DE",
    yr: str = "2023-2025",
) -> None:
    """Apply clusters to the market CSV and write skill_clusters_applied.json + output CSVs.

    This is the main per-market processing function.  It orchestrates all steps
    from path resolution through to writing the final mapped and unmapped CSVs.

    Parameters
    ----------
    market : str
        Market name, e.g. "Americas" or "EMEA".
    unmapped_mode : bool
        When True, reads from the unmapped_clusters sub-directory and the
        v2_unmapped CSV; writes to the unmapped_clusters applied JSON and the
        unmapped_clusters / still_unmapped CSVs.
    priority_skills : list of str or None
        Skills that force-assign matching rows; overrides PA default.
        Ignored in unmapped mode.
    exclude_skills : set of str or None
        Skills removed from both row data and cluster definitions before any
        Jaccard / assignment computation.
    primary_skills_override : list of str or None
        If provided, replaces the PA default list used in the second pass.
    pa : str
        Practice Area abbreviation (e.g. "DE", "EPS").  Controls which
        sub-directories and filenames are used.
    yr : str
        Year-range string used in CSV filenames (e.g. "2023-2025").
    """

    # ------------------------------------------------------------------
    # 1. Path resolution block
    #    Build all input/output paths depending on whether we are in
    #    normal mode or unmapped mode.  The two modes use different source
    #    CSVs, different cluster JSON files, and produce different outputs.
    # ------------------------------------------------------------------
    skills_dir = SKILLS_DIR / f"{market}_{pa}"
    data_subdir = DATA_DIR / pa
    if unmapped_mode:
        # Unmapped mode: cluster JSON lives inside the unmapped_clusters sub-dir;
        # input CSV is the "still unmapped" rows from the normal-mode run.
        clusters_path = skills_dir / UNMAPPED_CLUSTERS_SUBDIR / SKILL_CLUSTERS_JSON
        csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_unmapped_{market}.csv"
        out_dir = skills_dir / UNMAPPED_CLUSTERS_SUBDIR
        out_json_path = out_dir / APPLIED_JSON_NAME
        out_csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_unmapped_clusters_{market}.csv"
        out_unmapped_csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_still_unmapped_{market}.csv"
        jaccard_by_union = JACCARD_BY_UNION_UNMAPPED.get(market)
    else:
        # Normal mode: cluster JSON lives directly in the market skills dir;
        # input CSV is the full corrected/normalized market file.
        clusters_path = skills_dir / SKILL_CLUSTERS_JSON
        csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_corrected_normalized_{market}.csv"
        out_dir = skills_dir
        out_json_path = out_dir / APPLIED_JSON_NAME
        out_csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_skill_clusters_{market}.csv"
        out_unmapped_csv_path = data_subdir / f"DFC_YTD_{yr}_{pa}_V2_unmapped_{market}.csv"
        jaccard_by_union = JACCARD_BY_UNION_BY_MARKET.get(market)

    # ------------------------------------------------------------------
    # 2. Loading and validation block
    #    Guard against missing files or empty cluster lists early so the
    #    rest of the function can assume valid inputs.
    # ------------------------------------------------------------------
    if not clusters_path.exists():
        LOGGER.warning("Market '%s': %s not found, skipping", market, clusters_path.name)
        return
    if not csv_path.exists():
        LOGGER.warning("Market '%s': CSV not found %s, skipping", market, csv_path.name)
        return

    chosen_clusters = load_clusters(clusters_path, exclude_skills=exclude_skills)
    if not chosen_clusters:
        LOGGER.warning("Market '%s': no clusters in %s, skipping", market, clusters_path.name)
        return
    if not jaccard_by_union:
        raise ValueError(
            f"Missing Jaccard config for market '{market}'. Add it at the top of apply_clusters.py."
        )
    # Derive a single scalar min_jaccard (the smaller of the two thresholds)
    # used as a fallback floor value and for logging purposes.
    min_jaccard = min(
        float(jaccard_by_union.get("jaccard_when_union_ge", 0.3)),
        float(jaccard_by_union.get("jaccard_when_union_lt", 0.4)),
    )
    LOGGER.info(
        "Market '%s': applying %d clusters with length-wise jaccard (union>=%s -> %s, else %s)%s",
        market,
        len(chosen_clusters),
        jaccard_by_union.get("union_size_threshold"),
        jaccard_by_union.get("jaccard_when_union_ge"),
        jaccard_by_union.get("jaccard_when_union_lt"),
        " (unmapped)" if unmapped_mode else "",
    )
    df_market = load_market_csv(csv_path)
    total_demand = len(df_market)

    # ------------------------------------------------------------------
    # 3. Row skill set construction + first-pass assignment (priority skills)
    #    Each row's skill column is parsed into a Python set, then
    #    assign_rows_exclusive performs Jaccard matching.  Priority skills
    #    (normal mode only) cause hard assignment before Jaccard is checked.
    # ------------------------------------------------------------------
    row_sets, row_indices = build_row_skill_sets(df_market, exclude_skills=exclude_skills)
    total_rows = len(row_sets)
    pa_priority_default = PA_PRIORITY_SKILLS.get(pa, PA_PRIORITY_SKILLS[_DEFAULT_PA])
    # Priority-skill force-assignment is disabled in unmapped mode because those
    # rows already went through that logic in the normal-mode run.
    effective_priority = (priority_skills or pa_priority_default) if not unmapped_mode else None
    row_to_cluster, per_cluster_demand, ties_broken = assign_rows_exclusive(
        row_sets, chosen_clusters, min_jaccard, jaccard_by_union=jaccard_by_union, priority_skills=effective_priority
    )

    # ------------------------------------------------------------------
    # 4. Second pass (primary skills)
    #    Rows still unmapped after the first pass are retried using Jaccard
    #    computed only on each row's primary/core skills vs. each cluster's
    #    primary skills.  This recovers rows whose niche skills diluted the
    #    full-set Jaccard below the threshold even though their core stack
    #    matched a cluster.  Skipped in unmapped mode (no benefit there).
    # ------------------------------------------------------------------
    # Second pass: assign unmapped rows using Jaccard on primary skills only (row & primary, cluster & primary).
    if unmapped_mode:
        # No second pass in unmapped mode.
        primary_skills_set = None
    elif primary_skills_override is not None:
        primary_skills_set = set(primary_skills_override) if primary_skills_override else None
    else:
        pa_primary_default = PA_PRIMARY_SKILLS.get(pa, PA_PRIMARY_SKILLS[_DEFAULT_PA])
        primary_skills_set = set(pa_primary_default)
    mapped_primary_skills_pass = 0
    if primary_skills_set and any(row_to_cluster[i] < 0 for i in range(len(row_to_cluster))):
        mapped_primary_skills_pass = assign_unmapped_primary_skills_pass(
            row_sets, chosen_clusters, row_to_cluster, primary_skills_set, min_jaccard, jaccard_by_union
        )
        if mapped_primary_skills_pass > 0:
            # Recompute per-cluster demand counts to reflect the second-pass assignments.
            per_cluster_demand = [0] * len(chosen_clusters)
            for cidx in row_to_cluster:
                if cidx >= 0:
                    per_cluster_demand[cidx] += 1
            LOGGER.info(
                "Market '%s': second pass (primary skills only) assigned %d previously unmapped rows",
                market, mapped_primary_skills_pass,
            )

    # ------------------------------------------------------------------
    # 5. Occurrence and CV computation
    #    Count how often each skill appears in the rows mapped to each
    #    cluster (occurrence), and compute coefficient of variation of
    #    monthly demand (CV) both for the full date range and for 2025 only.
    # ------------------------------------------------------------------
    cluster_skill_occurrence = compute_cluster_skill_occurrence(
        row_sets, row_to_cluster, chosen_clusters, len(chosen_clusters)
    )

    total_mapped = sum(per_cluster_demand)
    unmapped = total_rows - total_mapped
    coverage = total_mapped / total_rows if total_rows else 0.0
    LOGGER.info(
        "Market '%s': total_mapped=%d, unmapped=%d, coverage=%.2f%%, ties_broken=%d",
        market, total_mapped, unmapped, coverage * 100, ties_broken,
    )

    unique_skills_in_clusters = sorted(set(s for cl in chosen_clusters for s in cl))
    per_cluster_cv = compute_cluster_cv(
        df_market, row_indices, row_to_cluster, len(chosen_clusters), time_col=TIME_COL
    )
    per_cluster_cv_2025 = compute_cluster_cv(
        df_market, row_indices, row_to_cluster, len(chosen_clusters), time_col=TIME_COL, year_filter=2025
    )

    # ------------------------------------------------------------------
    # 6. Payload construction and JSON write
    #    Assemble the full metrics dictionary and serialise it to the
    #    skill_clusters_applied.json output file.  The payload includes
    #    coverage stats, cluster-level metrics (demand, skill occurrence,
    #    CV), and human-readable notes explaining each computed field.
    # ------------------------------------------------------------------
    cluster_list = [
        {
            "skills": cl,
            "skill_count": len(cl),
            "mapped_demand": per_cluster_demand[idx],
            "skill_occurrence": {s: cluster_skill_occurrence[idx].get(s, 0) for s in cl},
            "cv": round(per_cluster_cv[idx], 4),
            "cv_2025": round(per_cluster_cv_2025[idx], 4),
        }
        for idx, cl in enumerate(chosen_clusters)
    ]
    cluster_list.sort(key=lambda x: x["mapped_demand"], reverse=True)

    assignment_rule = (
        "exclusive: each row assigned to the cluster with highest Jaccard (>= threshold); tie-break by smaller cluster; "
        "threshold from jaccard_by_union when set else single min_jaccard"
    )
    if not unmapped_mode and effective_priority:
        assignment_rule += "; rows containing any of priority_skills are assigned to the cluster that contains that skill (Jaccard ignored)"
    if primary_skills_set:
        assignment_rule += "; second pass on unmapped rows: Jaccard computed only on primary skills (DEFAULT_PRIMARY_SKILLS or --primary-skills), then assign if >= threshold"
    payload = {
        "market": market,
        "source": SKILL_CLUSTERS_JSON,
        "min_jaccard_for_coverage": min_jaccard,
        "assignment_rule": assignment_rule,
        "ties_broken": ties_broken,
        "total_demand": total_demand,
        "total_mapped_demand": total_mapped,
        "unmapped_demand": unmapped,
        "coverage_fraction": round(coverage, 4),
        "mapped_primary_skills_pass": mapped_primary_skills_pass,
        "num_clusters": len(chosen_clusters),
        "unique_skills_in_clusters": unique_skills_in_clusters,
        "unique_skills_count": len(unique_skills_in_clusters),
        "clusters_sorted_by": "mapped_demand descending",
        "cv_note": "coefficient of variation (std/mean) of monthly demand per cluster",
        "cv_2025_note": "CV of monthly demand in 2025 only",
        "skill_occurrence_note": "count of mapped rows in this cluster containing each skill",
        "clusters": cluster_list,
    }
    if jaccard_by_union:
        payload["jaccard_by_union"] = dict(jaccard_by_union)
    if not unmapped_mode and effective_priority:
        payload["priority_skills_force_assign"] = list(effective_priority)
    if unmapped_mode:
        payload["unmapped_pass"] = True
    if primary_skills_set:
        payload["primary_skills_count"] = len(primary_skills_set)
        payload["primary_skills_source"] = "args" if primary_skills_override else "default_list"
        payload["primary_skills_second_pass_note"] = "number of rows assigned in second pass using Jaccard on primary skills only"

    out_dir.mkdir(parents=True, exist_ok=True)
    with out_json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    LOGGER.info("Market '%s': wrote %s", market, out_json_path.name)

    # ------------------------------------------------------------------
    # 7. Column cleanup + cluster label building
    #    Remove intermediate helper columns that are not needed in the
    #    output CSV.  Then build a human-readable cluster label for each
    #    cluster index and populate the DEMAND_CLUSTER_COL column.
    #    Normal mode: labels are "MSC-<skill1>-<skill2>-..." with skills
    #    ordered by occurrence (highest first) so the label encodes the
    #    most dominant skills.
    #    Unmapped mode: labels are "MSC-UM-<N>" (simpler sequential IDs
    #    since occurrence ordering is less meaningful for small clusters).
    # ------------------------------------------------------------------
    columns_to_remove = ["Skills Normalized", "Not found", "_clustering_year"]
    existing_cols_to_remove = [c for c in columns_to_remove if c in df_market.columns]
    if existing_cols_to_remove:
        df_market = df_market.drop(columns=existing_cols_to_remove)

    if unmapped_mode:
        cluster_label_by_idx = {idx: f"MSC-UM-{idx + 1}" for idx in range(len(chosen_clusters))}
    else:
        # Order skills by occurrence (high to low) so MSC-a-b-c means a > b > c in occurrence.
        cluster_label_by_idx = {}
        for idx, cl in enumerate(chosen_clusters):
            occ = cluster_skill_occurrence[idx]
            ordered = sorted(cl, key=lambda s: (-occ.get(s, 0), s))
            cluster_label_by_idx[idx] = "MSC-" + "-".join(ordered)
    demand_cluster_col = [None] * len(df_market)
    for i, iloc in enumerate(row_indices):
        cidx = row_to_cluster[i]
        if cidx >= 0:
            demand_cluster_col[iloc] = cluster_label_by_idx[cidx]
    df_market = df_market.copy()
    df_market[DEMAND_CLUSTER_COL] = demand_cluster_col

    # ------------------------------------------------------------------
    # 8. Mapped vs unmapped CSV split and write
    #    Rows with a non-null DEMAND_CLUSTER_COL are written to the
    #    "skill_clusters" (mapped) CSV; rows with null are written to the
    #    "unmapped" CSV (without the cluster column).  Both files overwrite
    #    any previous run's output for this market.
    # ------------------------------------------------------------------
    df_cleaned = df_market[df_market[DEMAND_CLUSTER_COL].notna()].copy()
    df_cleaned.to_csv(out_csv_path, index=False)
    LOGGER.info("Market '%s': wrote %s (%d rows)", market, out_csv_path.name, len(df_cleaned))

    # Drop the cluster column from unmapped rows (it is entirely null for them).
    df_unmapped = df_market[df_market[DEMAND_CLUSTER_COL].isna()].drop(columns=[DEMAND_CLUSTER_COL]).copy()
    df_unmapped.to_csv(out_unmapped_csv_path, index=False)
    LOGGER.info(
        "Market '%s': wrote %s (%d rows)",
        market,
        out_unmapped_csv_path.name,
        len(df_unmapped),
    )


def main() -> None:
    """Parse CLI arguments and run cluster assignment for every configured market.

    CLI arguments
    -------------
    --unmapped
        Switch to unmapped mode: reads unmapped_clusters/skill_clusters.json
        and the v2_unmapped CSV, applies relaxed Jaccard thresholds, and writes
        the newly mapped and still-unmapped outputs.  Omit for normal mode.
    --priority-skills
        Comma-separated skill names that force-assign any row containing them
        to the cluster that holds the skill, bypassing Jaccard entirely.
        Overrides the PA default defined in PA_PRIORITY_SKILLS.
        Only used in normal (non-unmapped) mode.
    --exclude-skills
        Comma-separated skills stripped from both row data and cluster
        definitions before any Jaccard computation.  Overrides the PA default
        defined in PA_EXCLUDE_SKILLS.
    --primary-skills
        Comma-separated skills used in the second-pass Jaccard comparison.
        Overrides the PA default defined in PA_PRIMARY_SKILLS.
    --practice-area / -PA
        Practice Area abbreviation (e.g. DE, EPS, ADM).  Determines which
        data sub-directory and skills sub-directory are used, and which PA-
        specific default skill lists apply.
    --year-range
        Year range string embedded in CSV filenames (e.g. "2023-2025").

    Per-market loop
    ---------------
    After argument parsing, the function iterates over the MARKETS list
    ("Americas", "EMEA") and calls run_market() for each, passing the
    resolved PA, year range, and skill overrides.  Markets that lack the
    required input files are skipped with a warning.
    """
    parser = argparse.ArgumentParser(description="Apply skill clusters to CSV; --unmapped for second pass on unmapped rows.")
    parser.add_argument("--unmapped", action="store_true", help="Apply clusters from skills/<Market>_<PA>/unmapped_clusters/ to v2_unmapped CSV")
    parser.add_argument(
        "--priority-skills",
        type=str,
        default="",
        help="Comma-separated skills that force-assign rows to the cluster containing them, e.g. 'Android,iOS,SharePoint'. Used in mapped mode only.",
    )
    parser.add_argument(
        "--exclude-skills",
        type=str,
        default="",
        help="Comma-separated skills to exclude from row data and loaded clusters (e.g. 'API Development'); not used in Jaccard or assignment.",
    )
    parser.add_argument(
        "--primary-skills",
        type=str,
        default="",
        help="Comma-separated primary skills for second-pass assignment of unmapped rows (Jaccard on these only). If empty, use DEFAULT_PRIMARY_SKILLS in code.",
    )
    parser.add_argument(
        "--practice-area",
        type=str,
        default="DE",
        metavar="PA",
        help="Practice Area abbreviation (e.g. DE, EPS). Controls data/{PA}/ and skills/{Market}_{PA}/. Default: DE",
    )
    parser.add_argument(
        "--year-range",
        type=str,
        default="2023-2025",
        metavar="YYYY-YYYY",
        help="Year range used in CSV filenames (e.g. 2023-2025). Default: 2023-2025",
    )
    args = parser.parse_args()
    pa = args.practice_area.strip()
    yr = args.year_range.strip()
    priority_skills = [s.strip() for s in args.priority_skills.split(",") if s.strip()] if args.priority_skills else None
    if args.exclude_skills:
        exclude_skills = {s.strip() for s in args.exclude_skills.split(",") if s.strip()}
    else:
        # Fall back to the PA-specific default exclusion list (or DE defaults if PA not found).
        exclude_skills = set(PA_EXCLUDE_SKILLS.get(pa, PA_EXCLUDE_SKILLS.get(_DEFAULT_PA, [])))
    primary_skills_override = [s.strip() for s in args.primary_skills.split(",") if s.strip()] if args.primary_skills else None
    if args.unmapped:
        LOGGER.info("Unmapped mode: applying unmapped_clusters to v2_unmapped CSV per market | PA=%s | yr=%s", pa, yr)
    else:
        LOGGER.info("Applying clusters (skill_clusters_applied.json + v2 CSV per market) | PA=%s | yr=%s", pa, yr)
    # Iterate over every configured market and apply clusters independently.
    # Each market has its own cluster file, input CSV, and output paths.
    for market in MARKETS:
        run_market(
            market,
            unmapped_mode=args.unmapped,
            priority_skills=priority_skills,
            exclude_skills=exclude_skills,
            primary_skills_override=primary_skills_override,
            pa=pa,
            yr=yr,
        )
    LOGGER.info("Done (all markets processed)")


if __name__ == "__main__":
    main()
