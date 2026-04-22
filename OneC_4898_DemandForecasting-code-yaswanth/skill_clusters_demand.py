"""
Demand-covering skill clusters per market.

Flow (per market):
  1. Greedy set-cover: pick up to max_clusters from co_occurrence candidates (skill count per cluster
     Americas 2-6, EMEA 2-5). Row covered iff Jaccard(row_skills, cluster_skills) >= min_jaccard.
     A fill phase appends remaining high-true_demand candidates until max_clusters is reached.
  2. Multi-phase injection: ensure high-demand skills appear in at least one cluster (append -> swap ->
     swap-dedup -> synthesize -> force-append). Re-assign rows after cluster changes.
  3. Drop low-demand clusters: remove clusters with mapped demand < min_mapped_demand_per_cluster
     (set per market in MARKET_PARAMS). Migrate high-demand skills from dropped clusters into kept
     ones; re-inject if needed and re-assign.
  4. Trim and replace poor-fit skills: in each cluster, remove skills with 0 occurrence or
     occurrence below MIN_OCCURRENCE_FRACTION_OF_MAX of the cluster max occurrence. Americas
     only: for each removed (mismatch) skill, add a replacement by simulated demand (best
     mapped_demand). EMEA: trim only, no replacement. Drop clusters that become empty; re-assign rows.
  5. Final drop: remove any cluster with mapped demand < min_mapped_demand_per_cluster (no
     exceptions). Ensures no cluster below the market threshold remains in outputs.
  6. Write outputs: skill_clusters.json (final clusters only, for human review / apply_clusters.py)
     and skill_clusters_demand.json (full demand metrics). CSV is produced by apply_clusters.py.

Assignment: each row is assigned to the cluster with highest Jaccard (>= min_jaccard); ties
broken by smaller cluster. Coverage target ~70-80%; top high-demand skill coverage is prioritised.

Inputs (per market):
  - data/DFC_YTD_2023-2025_v1_corrected_normalized_<Market>.csv
  - skills/<Market>/co_occurrence.json  (from skill_normalized.py)
  - skills/<Market>/high_demand_skills.json

Outputs (per market):
  - skills/<Market>/skill_clusters.json       (final clusters only; no CSV write)
  - skills/<Market>/skill_clusters_demand.json (final clusters + demand metrics, post-trim)

Unmapped mode (--unmapped): clusters only the rows that did not match any main cluster.
  - Input: data/DFC_YTD_2023-2025_v2_unmapped_<Market>.csv (from apply_clusters.py).
  - Co-occurrence: built from unmapped rows only (in memory) and written to
    skills/<Market>/unmapped_clusters/co_occurrence.json (main skills/<Market>/co_occurrence.json is never read or overwritten).
  - Outputs: skills/<Market>/unmapped_clusters/skill_clusters.json, skill_clusters_demand.json, co_occurrence.json.
  - Uses max 5 clusters and relaxed Jaccard to keep ~98% of unmapped demand mapped (~1-2% loss).

Folder division: mapped outputs stay in skills/<Market>/; unmapped outputs stay in skills/<Market>/unmapped_clusters/ only.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import coloredlogs
import pandas as pd
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
SKILLS_DIR = PROJECT_ROOT / "skills"

OUTPUT_JSON_NAME = "skill_clusters_demand.json"
SKILL_CLUSTERS_JSON = "skill_clusters.json"
SKILL_GROUPS_COL = "Skill Groups"
TIME_COL = "Requirement Start Date"
DEMAND_CLUSTER_COL = "Skill Cluster"

# Markets to process (must have ..._normalized_<Market>.csv and skills/<Market>/co_occurrence.json).
MARKETS = ["Americas", "EMEA"]

UNMAPPED_CLUSTERS_SUBDIR = "unmapped_clusters"

# ---------------------------------------------------------------------------
# Length-wise Jaccard thresholds (edit in-code when needed)
# ---------------------------------------------------------------------------
# Rule: threshold depends on |row_skills ∪ cluster_skills| (union size).
# - If union_size >= union_size_threshold -> use jaccard_when_union_ge
# - Else -> use jaccard_when_union_lt
JACCARD_BY_UNION_BY_MARKET: Dict[str, Dict[str, object]] = {
    "Americas": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.3, "jaccard_when_union_lt": 0.4},
    "EMEA": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.3, "jaccard_when_union_lt": 0.4},
}

# ---------------------------------------------------------------------------
# Parameters: one dict per market
# ---------------------------------------------------------------------------
# max_clusters: Greedy set cover picks up to this many; fill phase adds high-demand candidates until this cap.
# min_skills_per_cluster / max_skills_per_cluster: Only co_occurrence combinations with skill count in [min, max] are candidates (avoids singletons and huge combos).
# max_candidate_clusters: Top N combinations from co_occurrence (sorted by true_demand desc) considered as candidates. Limits runtime and focuses on high-demand stacks.
# min_jaccard_for_coverage: Row is "covered" by a cluster only if Jaccard >= this; also used for exclusive assignment; ties broken by smaller cluster.
# min_mapped_demand_per_cluster: (per market) Clusters with mapped demand below this are dropped; high-demand skills in dropped clusters are migrated into kept clusters.
# Universal: skill is a mismatch if its occurrence is below this fraction of the cluster max occurrence (works for 2-skill clusters; median would not).
MIN_OCCURRENCE_FRACTION_OF_MAX = 0.2

MARKET_PARAMS: Dict[str, Dict[str, object]] = {
    "Americas": {
        "max_clusters": 15,
        "min_skills_per_cluster": 2,
        "max_skills_per_cluster": 7,
        "max_candidate_clusters": 250,
        "min_jaccard_for_coverage": 0.50,
        "min_mapped_demand_per_cluster": 500,
    },
    "EMEA": {
        "max_clusters": 10,
        "min_skills_per_cluster": 2,
        "max_skills_per_cluster": 7,
        "max_candidate_clusters": 100,
        "min_jaccard_for_coverage": 0.50,
        "min_mapped_demand_per_cluster": 250,
    },
}

# Per-PA overrides merged on top of MARKET_PARAMS (only keys listed here are overridden).
# ADM has 28/22 high-demand skills → needs more clusters to cover diverse tech stacks.
# EPS is SAP/Oracle/Pega heavy and benefits from slightly more clusters than the default.
PA_MARKET_PARAMS_OVERRIDES: Dict[str, Dict[str, Dict[str, object]]] = {
    "ADM": {
        "Americas": {"max_clusters": 18, "max_skills_per_cluster": 6},
        "EMEA":    {"max_clusters": 12, "max_skills_per_cluster": 6},
    },
    "EPS": {
        "Americas": {"max_clusters": 12, "max_skills_per_cluster": 6},
        "EMEA":    {"max_clusters": 10, "max_skills_per_cluster": 6},
    },
    "DE": {
        "Americas": {"min_skills_per_cluster": 1},
        "EMEA":    {"min_skills_per_cluster": 1},
    },
}

# Relaxed Jaccard for unmapped pass to keep ~98% coverage (~1-2% loss).
JACCARD_BY_UNION_UNMAPPED: Dict[str, Dict[str, object]] = {
    "Americas": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.20, "jaccard_when_union_lt": 0.25},
    "EMEA": {"union_size_threshold": 7, "jaccard_when_union_ge": 0.20, "jaccard_when_union_lt": 0.25},
}
UNMAPPED_PARAMS: Dict[str, object] = {
    "max_clusters": 5,
    "min_skills_per_cluster": 2,
    "max_skills_per_cluster": 5,
    "max_candidate_clusters": 80,
    "min_jaccard_for_coverage": 0.25,
    "min_mapped_demand_per_cluster": 50,
}


def _params(market: str, pa: str = "DE") -> Dict[str, object]:
    """Return params for market with PA-specific overrides applied; fallback to Americas if market not in MARKET_PARAMS.

    Override-merge logic:
      1. Start from the market's base dict in MARKET_PARAMS (copied so mutations don't leak).
      2. Look up PA_MARKET_PARAMS_OVERRIDES[pa][market]; if present, merge (dict.update) on top
         of the base so PA-specific values win.  Any key not in the override dict keeps its
         market-level default — this lets PA overrides be sparse (only change what matters).
      3. If the market key is missing entirely (e.g. a new market not yet in MARKET_PARAMS),
         fall back to Americas defaults to avoid KeyError.
    """
    # Start with a copy so callers cannot accidentally mutate the global MARKET_PARAMS dict.
    base = MARKET_PARAMS.get(market, MARKET_PARAMS["Americas"]).copy()
    # Fetch PA-level overrides; empty dict if PA or market not found, so update is always safe.
    overrides = PA_MARKET_PARAMS_OVERRIDES.get(pa, {}).get(market, {})
    base.update(overrides)
    return base

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
coloredlogs.install(level=logging.INFO, logger=LOGGER, isatty=True)


def split_skills_cell(raw_value) -> List[str]:
    """Parse skill groups cell handling delimiters and parentheses (same logic as skill_clustering).

    Parsing strategy:
      - Recognises three delimiter characters: comma, semicolon, and colon.
      - Tracks parenthesis depth so that delimiters inside parentheses (e.g. "Java (EE, SE)")
        are treated as part of the skill name rather than as separators.
      - Builds each token character-by-character; flushes the buffer when an unquoted
        delimiter is encountered, then strips leading/trailing whitespace.
      - Empty tokens (whitespace-only or doubled delimiters) are silently discarded.
    """
    if raw_value is None:
        return []
    text = str(raw_value).strip()
    if not text:
        return []
    delimiters = {",", ";", ":"}
    parts, current = [], []
    # depth tracks how many open parentheses we are inside; delimiters are ignored when depth > 0.
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            # Guard against malformed input with more closing than opening parens.
            depth = max(0, depth - 1)
        if ch in delimiters and depth == 0:
            # Delimiter at top level: flush current token buffer as one skill name.
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
        else:
            current.append(ch)
    # Flush the last token (no trailing delimiter required).
    token = "".join(current).strip()
    if token:
        parts.append(token)
    return parts


def load_market_csv(csv_path: Path) -> pd.DataFrame:
    """Load per-market normalized CSV (no market filter; file is already market-specific). Returns df.

    The file naming convention (<PA>/<yr>_<PA>_V2_corrected_normalized_<Market>.csv) means the
    market is already baked into the path, so no post-load filtering is required.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    return pd.read_csv(csv_path)


def load_high_demand_skills(skills_dir: Path) -> Dict[str, int]:
    """Load high-demand skill -> demand from skills/<Market>/high_demand_skills.json if present; else return empty dict.

    The file is optional: if it does not exist (e.g. for unmapped pass), an empty dict is returned
    so that the injection step becomes a no-op rather than raising an error.  The isinstance guard
    handles files that were accidentally serialised as a list instead of a dict.
    """
    path = skills_dir / "high_demand_skills.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    # Defensively convert in case the file was written as a list of [skill, demand] pairs.
    return dict(data) if isinstance(data, dict) else {}


def load_candidate_clusters(
    coocc_path: Path,
    market: str,
    exclude_skills: Optional[Set[str]] = None,
    pa: str = "DE",
) -> List[Tuple[List[str], int]]:
    """
    Load co_occurrence JSON; keep entries with skill count in [min_skills, max_skills] for market.
    When min_skills_per_cluster == 1, also loads single-skill entries from single_large_occ.json
    (same directory as coocc_path) so singleton clusters like ["React"] or ["Python"] are candidates.
    Exclude_skills are removed from each entry's skill list so they never appear in clusters.
    Returns list of (skill_list, true_demand) sorted by true_demand desc, capped at max_candidate_clusters for market.

    Filtering logic:
      - Skill count filter [min_skills, max_skills]: avoids singletons (poor Jaccard signal)
        and overly large combos (low co-occurrence confidence, slow assignment).
      - exclude_skills removal happens AFTER splitting the comma-separated skills string so
        that a multi-skill combo becomes a shorter valid combo rather than being dropped entirely
        (unless it falls below min_skills after removal).
      - The cap (max_candidate_clusters) limits greedy set-cover runtime: candidates are
        pre-sorted by true_demand descending so the cap retains the most demand-relevant stacks.
      - When min_skills_per_cluster == 1 (e.g. DE practice area), singleton clusters are loaded
        from a separate file (single_large_occ.json) because co_occurrence.json only stores
        multi-skill combinations.  A seen-set deduplicates in case any singleton already appears.
    """
    p = _params(market, pa=pa)
    min_skills = int(p["min_skills_per_cluster"])
    max_skills = int(p["max_skills_per_cluster"])
    cap = int(p["max_candidate_clusters"])
    exclude = exclude_skills or set()
    with coocc_path.open(encoding="utf-8") as f:
        data = json.load(f)
    candidates = []
    for entry in data:
        skills_str = entry.get("skills", "")
        skills = [s.strip() for s in skills_str.split(",") if s.strip()]
        # Remove any globally-excluded skills before checking length bounds.
        skills = [s for s in skills if s not in exclude]
        # Discard combos that are too small (singleton noise) or too large (sparse co-occurrence).
        if not (min_skills <= len(skills) <= max_skills):
            continue
        true_demand = entry.get("true_demand", 0)
        candidates.append((skills, true_demand))
    # When singletons are allowed, supplement with the dedicated single_large_occ.json file.
    if min_skills == 1:
        single_path = coocc_path.parent / "single_large_occ.json"
        if single_path.exists():
            with single_path.open(encoding="utf-8") as f:
                single_data = json.load(f)
            # Track which skill-sets are already present to avoid duplicate candidates.
            seen = {frozenset(c[0]) for c in candidates}
            for entry in single_data:
                skill = entry.get("skills", "").strip()
                if not skill or skill in exclude:
                    continue
                if frozenset([skill]) in seen:
                    continue
                candidates.append(([skill], entry.get("true_demand", 0)))
    # Sort descending by true_demand so the cap keeps the highest-demand skill stacks.
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:cap]


def build_candidate_clusters_from_row_sets(
    row_sets: List[Set[str]],
    min_skills: int,
    max_skills: int,
    cap: int,
) -> List[Tuple[List[str], int]]:
    """Build co-occurrence candidates from row skill sets (for unmapped pass; same format as load_candidate_clusters).

    In the unmapped pass there is no pre-computed co_occurrence.json to load from disk because
    the unmapped rows form a long-tail population that was not seen during the main co-occurrence
    build.  This function builds that structure in memory instead:

      1. For each row, the entire skill set is treated as one co-occurring combination.
         Skills are sorted and joined so the same combination always maps to the same key
         regardless of the original column order.
      2. The count of identical combinations becomes the true_demand proxy (number of rows
         that share exactly this skill stack).
      3. Skill-count filtering and the cap mirror load_candidate_clusters so the downstream
         greedy set-cover receives the same input format.

    Note: using the full row skill set as a combination is conservative — it treats rows with
    many skills as a single high-specificity cluster candidate.  The greedy cover step then
    decides whether it is worth selecting such a specific cluster.
    """
    combination_counts: Dict[str, int] = {}
    for rset in row_sets:
        if not rset:
            continue
        # Canonical key: sorted skills joined by ", " so {"A","B"} and {"B","A"} map to the same entry.
        combo_key = ", ".join(sorted(rset))
        combination_counts[combo_key] = combination_counts.get(combo_key, 0) + 1
    candidates = []
    for combo, demand in combination_counts.items():
        skills = [s.strip() for s in combo.split(", ") if s.strip()]
        skill_count = len(skills)
        # Apply the same [min_skills, max_skills] filter used by load_candidate_clusters.
        if not (min_skills <= skill_count <= max_skills):
            continue
        candidates.append((skills, demand))
    # Sort descending so the cap retains the most common (highest-demand) combinations.
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:cap]


def build_row_skill_sets(
    df: pd.DataFrame,
    exclude_skills: Optional[Set[str]] = None,
) -> Tuple[List[Set[str]], List[int]]:
    """Return (list of sets of skills per row with at least one skill, list of iloc indices into df). Exclude_skills are stripped so they are not used in Jaccard or clustering.

    Rows that have no skills after parsing and exclusion are dropped entirely (they cannot be
    covered by any cluster, so including them would inflate the "unmapped" count with rows that
    are structurally unmappable).  The returned iloc indices allow callers to map back to the
    original DataFrame position for demand metrics and output labelling.
    """
    exclude = exclude_skills or set()
    row_sets = []
    row_indices = []
    for iloc_idx in range(len(df)):
        # Parse the cell and immediately subtract any globally-excluded skills.
        skills = set(split_skills_cell(df.iloc[iloc_idx].get(SKILL_GROUPS_COL))) - exclude
        # Only keep rows that have at least one usable skill; skill-less rows are unmappable.
        if skills:
            row_sets.append(skills)
            row_indices.append(iloc_idx)
    return row_sets, row_indices


def jaccard(a: Set[str], b: Set[str]) -> float:
    """Jaccard similarity |a ∩ b| / |a ∪ b|; 0 if both empty. Set-based, no demand weighting.

    Jaccard is chosen over cosine or other similarity measures because:
      - It naturally penalises large clusters that cover a row only partially (more skills in
        the union dilute the score even if the intersection is large).
      - It is symmetric: Jaccard(row, cluster) == Jaccard(cluster, row), so the threshold is
        intuitive in both directions.
      - It is parameter-free at the skill level — no TF-IDF weights or embeddings needed.

    Returns 0.0 when both sets are empty to avoid ZeroDivisionError; this edge case should
    not occur in practice because rows with no skills are excluded by build_row_skill_sets.
    """
    inter = len(a & b)
    union = len(a | b)
    # Guard against the degenerate case where both sets are empty.
    return inter / union if union else 0.0


def row_covered_by_cluster(rset: Set[str], cset: Set[str], min_jaccard: float) -> bool:
    """True iff Jaccard(row_skills, cluster_skills) >= min_jaccard.

    Used during the greedy set-cover phase where a single global min_jaccard threshold applies
    (not the length-based variant).  The length-based threshold (_min_jaccard_for_pair) is used
    only during the final exclusive assignment step.
    """
    return jaccard(rset, cset) >= min_jaccard


def compute_cluster_cv(
    df: pd.DataFrame,
    row_indices: List[int],
    row_to_cluster: List[int],
    num_clusters: int,
    time_col: str = TIME_COL,
    year_filter: Optional[int] = None,
) -> List[float]:
    """
    Coefficient of variation (std/mean) of monthly demand per cluster.
    If year_filter is set (e.g. 2025), only months in that year are used; otherwise all months.
    Returns list of CV per cluster index; 0 if cluster has no/constant demand.

    CV is a dimensionless measure of demand seasonality / volatility.  Low CV means the cluster
    sees steady demand across months; high CV suggests spikes (project ramp-ups, seasonal hiring).
    Returning 0.0 when there is fewer than 2 months of data avoids a meaningless single-point CV.
    """
    df = df.copy()
    # Initialise a cluster column; -1 means unassigned (will be filtered out later).
    df["_cluster"] = -1
    for i, cidx in enumerate(row_to_cluster):
        df.iat[row_indices[i], df.columns.get_loc("_cluster")] = cidx
    if time_col not in df.columns:
        # If the time column is missing the CV cannot be computed; return zeros for all clusters.
        return [0.0] * num_clusters
    # Convert date strings to calendar month periods for stable monthly aggregation.
    df["_period"] = pd.to_datetime(df[time_col], errors="coerce").dt.to_period("M")
    df["_year"] = df["_period"].dt.year
    if year_filter is not None:
        # When year_filter is set, restrict to that year to capture recent volatility separately.
        df = df[df["_year"] == year_filter]
    # Drop rows that were not assigned to any cluster (assignment happens before this call).
    df = df[df["_cluster"] >= 0]
    if df.empty:
        return [0.0] * num_clusters
    # Count rows per (month, cluster) to get monthly demand series per cluster.
    monthly = df.groupby(["_period", "_cluster"], dropna=False).size().reset_index(name="demand")
    cvs: List[float] = []
    for cidx in range(num_clusters):
        sub = monthly[monthly["_cluster"] == cidx]["demand"]
        # CV is undefined or 0 if fewer than 2 months of data or if mean is zero (constant demand).
        if len(sub) < 2 or sub.mean() == 0:
            cvs.append(0.0)
        else:
            cvs.append(float(sub.std() / sub.mean()))
    return cvs


def _min_jaccard_for_pair(
    rset: Set[str],
    cset: Set[str],
    min_jaccard: float,
    jaccard_by_union: Optional[Dict[str, object]] = None,
) -> float:
    """Return the min Jaccard threshold for this row-cluster pair; uses length-based rule when jaccard_by_union is set.

    Rationale for the length-based rule:
      - When the union of a row and a cluster is large (>= union_size_threshold), the raw Jaccard
        score is naturally depressed even for a good conceptual match.  For example, a row with
        5 skills and a 6-skill cluster may share 3 skills (Jaccard = 3/8 = 0.375) even though
        that is excellent overlap.  The relaxed threshold (jaccard_when_union_ge) compensates.
      - When the union is small (< threshold), Jaccard is a sharper signal; a stricter threshold
        (jaccard_when_union_lt) avoids assigning rows to weakly-related clusters.
      - If jaccard_by_union is None, the global min_jaccard is returned unchanged (simple mode).
    """
    if jaccard_by_union is None:
        return min_jaccard
    threshold = int(jaccard_by_union.get("union_size_threshold", 7))
    ge_val = float(jaccard_by_union.get("jaccard_when_union_ge", 0.3))
    lt_val = float(jaccard_by_union.get("jaccard_when_union_lt", 0.4))
    union_size = len(rset | cset)
    # Use the relaxed threshold for large unions, stricter threshold for small ones.
    return ge_val if union_size >= threshold else lt_val


def assign_rows_exclusive(
    row_sets: List[Set[str]],
    chosen_clusters: List[List[str]],
    min_jaccard: float,
    jaccard_by_union: Optional[Dict[str, object]] = None,
    priority_skills: Optional[List[str]] = None,
) -> Tuple[List[int], List[int], int]:
    """
    Assign each row to exactly one cluster: highest Jaccard (if >= threshold).
    When priority_skills is set, rows containing any of these skills are assigned to a cluster
    that contains that skill (no Jaccard threshold); tie-break by best Jaccard then smaller cluster.
    Threshold is per pair when jaccard_by_union is set.
    Returns (row_to_cluster, per_cluster_demand, ties_broken_count).

    Assignment logic in detail:
      - Normal path: compute Jaccard between the row and every cluster; keep only clusters
        where Jaccard >= the pair-specific threshold (from _min_jaccard_for_pair).  Among
        qualifying clusters, pick the one with the highest Jaccard.
      - Priority path: if the row contains a priority skill that is present in at least one
        cluster, restrict the candidate set to those clusters and skip the threshold — the
        row must land in a cluster containing its priority skill regardless of Jaccard.
      - Tie-breaking (same Jaccard): prefer the cluster with fewer skills (more specific match
        relative to its size), then by lower index (stable sort determinism).
      - Rows that satisfy no threshold (or have no matching priority cluster) get -1 (unmapped).
    """
    cluster_sets = [set(skills) for skills in chosen_clusters]
    cluster_sizes = [len(skills) for skills in chosen_clusters]
    # Map each priority skill to cluster indices that contain it (for force-assign, mapped case only).
    skill_to_cluster_indices: Dict[str, List[int]] = {}
    if priority_skills:
        priority_set = set(priority_skills)
        for idx, cset in enumerate(cluster_sets):
            for s in cset:
                if s in priority_set:
                    skill_to_cluster_indices.setdefault(s, []).append(idx)
    row_to_cluster: List[int] = []
    ties_broken = 0
    for rset in row_sets:
        # Determine whether this row should be force-assigned via its first priority skill.
        candidate_indices: Optional[List[int]] = None
        if priority_skills:
            for s in priority_skills:
                if s in rset and s in skill_to_cluster_indices:
                    # Use the first matching priority skill; only clusters containing it are considered.
                    candidate_indices = skill_to_cluster_indices[s]
                    break
        candidates = []
        for idx, cset in enumerate(cluster_sets):
            if candidate_indices is not None and idx not in candidate_indices:
                # Skip clusters not containing the priority skill.
                continue
            j = jaccard(rset, cset)
            min_j = _min_jaccard_for_pair(rset, cset, min_jaccard, jaccard_by_union)
            if candidate_indices is not None:
                # Priority path: no threshold, just best Jaccard among clusters containing the skill.
                candidates.append((idx, j))
            elif j >= min_j:
                # Normal path: only clusters that meet the (possibly length-adjusted) threshold.
                candidates.append((idx, j))
        if not candidates:
            # No cluster qualifies: row is unmapped.
            row_to_cluster.append(-1)
            continue
        max_j = max(c[1] for c in candidates)
        best_candidates = [c for c in candidates if c[1] == max_j]
        if len(best_candidates) > 1:
            # Track ties for logging; prefer smaller cluster as tiebreaker for specificity.
            ties_broken += 1
        # Among equally-scored clusters, prefer smallest (then lowest index for determinism).
        best_idx = min(best_candidates, key=lambda c: (cluster_sizes[c[0]], c[0]))[0]
        row_to_cluster.append(best_idx)
    # Aggregate per-cluster demand counts from the assignment vector.
    per_cluster_demand = [0] * len(chosen_clusters)
    for cidx in row_to_cluster:
        if cidx >= 0:
            per_cluster_demand[cidx] += 1
    return row_to_cluster, per_cluster_demand, ties_broken


def compute_cluster_skill_occurrence(
    row_sets: List[Set[str]],
    row_to_cluster: List[int],
    chosen_clusters: List[List[str]],
    num_clusters: int,
) -> List[Dict[str, int]]:
    """Per cluster: count of mapped rows containing each skill.

    Occurrence is used downstream to identify mismatch skills (trim_and_replace step): a skill
    that appears in a cluster but is rarely present in the rows assigned to it is a sign of poor
    fit and should be trimmed or replaced.  Counting over the full row skill set (not just cluster
    skills) means skills that were injected but do not naturally co-occur with the cluster's core
    skills will have low occurrence and be flagged correctly.
    """
    by_cluster: List[Dict[str, int]] = [{} for _ in range(num_clusters)]
    for i, cidx in enumerate(row_to_cluster):
        if cidx < 0:
            # Unassigned rows do not contribute to any cluster's occurrence counts.
            continue
        for skill in row_sets[i]:
            by_cluster[cidx][skill] = by_cluster[cidx].get(skill, 0) + 1
    return by_cluster


def find_best_replacement_by_simulated_demand(
    row_sets: List[Set[str]],
    chosen_clusters: List[List[str]],
    cluster_idx: int,
    anchor_skills: List[str],
    exclude_skills: Set[str],
    skill_rows: Dict[str, Set[int]],
    min_jaccard: float,
    max_skills_per_cluster: int,
    min_occurrence_fraction_of_max: float,
    jaccard_by_union: Optional[Dict[str, object]] = None,
) -> Optional[str]:
    """
    For each candidate skill (co-occurs with anchor in data), simulate assignment with
    cluster = anchor + [candidate]; pick the candidate that yields max mapped_demand for
    this cluster and whose occurrence in that cluster is >= fraction of cluster max (no new mismatch).

    This is the Americas replacement strategy used in the trim-and-replace phase:
      1. Derive a candidate pool from rows that contain ALL anchor skills (intersection of row
         sets).  This ensures the replacement naturally co-occurs with the surviving cluster core.
      2. For each candidate, simulate a full assignment pass with a trial cluster that appends
         the candidate to the anchor.  Simulation is needed because adding a skill changes which
         rows Jaccard-qualify for the cluster, which changes mapped_demand.
      3. A candidate is rejected if its occurrence in the simulated assignment is below the
         min_occurrence_fraction_of_max threshold — that would just introduce a new mismatch.
      4. Among accepted candidates, pick the one that maximises mapped_demand.  If no candidate
         passes the occurrence check, the best by demand is returned anyway (occ_ok=False)
         rather than returning None, to avoid leaving the cluster below min_skills.

    Returns None if: cluster is already at max_skills, no rows contain all anchors, or no
    non-excluded skills co-occur with the anchor.
    """
    # Cannot add another skill if the cluster is already at its maximum size.
    if len(anchor_skills) >= max_skills_per_cluster:
        return None
    anchor_set = set(anchor_skills)
    # Exclude current anchor skills and any globally-excluded or already-tried replacements.
    exclude = anchor_set | exclude_skills
    # Find rows that contain ALL current anchor skills — these define valid co-occurrence context.
    rows_with_anchor = None
    for s in anchor_skills:
        r = skill_rows.get(s, set())
        rows_with_anchor = r if rows_with_anchor is None else rows_with_anchor & r
    if not rows_with_anchor:
        # No rows contain every anchor skill simultaneously; replacement is not meaningful.
        return None
    # Collect all skills that appear alongside every anchor skill in at least one row.
    candidates = set()
    for i in rows_with_anchor:
        for s in row_sets[i]:
            if s not in exclude:
                candidates.add(s)
    if not candidates:
        return None
    num_clusters = len(chosen_clusters)
    best_skill: Optional[str] = None
    best_demand = -1
    best_occ_ok = False
    for candidate in candidates:
        # Build a trial cluster list: replace only the target cluster with anchor + candidate.
        clusters_test = [list(c) for c in chosen_clusters]
        clusters_test[cluster_idx] = anchor_skills + [candidate]
        # Full assignment simulation to get accurate mapped_demand for the trial cluster.
        row_to_cluster, per_cluster_demand, _ = assign_rows_exclusive(
            row_sets, clusters_test, min_jaccard, jaccard_by_union=jaccard_by_union
        )
        occs_test = compute_cluster_skill_occurrence(
            row_sets, row_to_cluster, clusters_test, num_clusters
        )
        demand = per_cluster_demand[cluster_idx]
        occs = [occs_test[cluster_idx].get(s, 0) for s in clusters_test[cluster_idx]]
        max_occ = max(occs) if occs else 0
        min_occ = max(1, int(min_occurrence_fraction_of_max * max_occ)) if max_occ > 0 else 1
        candidate_occ = occs_test[cluster_idx].get(candidate, 0)
        # occ_ok is True only if the candidate itself satisfies the occurrence floor — meaning
        # adding it does not create a new mismatch.  Prefer occ_ok=True candidates; break ties
        # by mapped_demand so the final cluster is as large and demand-rich as possible.
        occ_ok = candidate_occ >= min_occ
        if (occ_ok, demand) > (best_occ_ok, best_demand):
            best_occ_ok = occ_ok
            best_demand = demand
            best_skill = candidate
    return best_skill


def greedy_set_cover(
    row_sets: List[Set[str]],
    candidate_clusters: List[Tuple[List[str], int]],
    total_rows: int,
    max_clusters: int,
    min_jaccard: float,
) -> Tuple[List[List[str]], float]:
    """
    Greedy set cover: repeatedly pick the candidate cluster that covers the most uncovered rows
    (row covered iff Jaccard >= min_jaccard). Stops at max_clusters or when no candidate adds coverage.
    Fill phase: if fewer than max_clusters chosen, add remaining by highest true_demand until max_clusters.
    Returns (chosen cluster skill lists, coverage fraction).

    Algorithm overview:
      Phase 1 — Greedy coverage maximisation:
        At each iteration, scan every unchosen candidate and count how many *currently uncovered*
        rows it would cover.  Pick the candidate with the highest new-coverage count.  Stop early
        if no candidate covers any new rows (saturation) or max_clusters is reached.

      Phase 2 — Fill by true_demand:
        If fewer than max_clusters were selected in Phase 1 (because coverage saturated), pad the
        cluster list with the highest-true_demand unchosen candidates.  This ensures we use the
        full cluster budget with the most economically relevant stacks rather than leaving slots
        empty.  Coverage fraction is updated after each fill addition.

    Why greedy?  The optimal set-cover problem is NP-hard; the greedy approximation gives a
    (1 - 1/e) ≈ 63% coverage guarantee relative to the optimal.  In practice, for skill data
    with high co-occurrence structure, greedy typically achieves 70-80% coverage.
    """
    # Pre-convert candidate skill lists to sets for O(1) intersection/union in Jaccard.
    cluster_sets = [set(skills) for skills, _ in candidate_clusters]
    covered: Set[int] = set()   # Indices of rows covered by at least one chosen cluster.
    chosen: List[List[str]] = []
    chosen_indices: Set[int] = set()  # Track which candidates have been selected to skip them.

    # -----------------------------------------------------------------
    # Phase 1: Greedy coverage maximisation
    # -----------------------------------------------------------------
    for _ in range(max_clusters):
        best_idx = -1
        best_new = 0
        # Scan all unchosen candidates and find the one that covers the most uncovered rows.
        for idx, cset in enumerate(cluster_sets):
            if idx in chosen_indices:
                continue
            new_covered = sum(
                1
                for i, rset in enumerate(row_sets)
                if i not in covered and row_covered_by_cluster(rset, cset, min_jaccard)
            )
            if new_covered > best_new:
                best_new = new_covered
                best_idx = idx
        # Stopping conditions:
        #   best_idx == -1  -> all candidates already chosen (exhausted the pool).
        #   best_new == 0   -> no candidate covers any new row (coverage saturated).
        if best_idx == -1 or best_new == 0:
            break
        chosen.append(list(candidate_clusters[best_idx][0]))
        chosen_indices.add(best_idx)
        # Mark all rows covered by the newly chosen cluster (including previously covered ones).
        cset = cluster_sets[best_idx]
        for i, rset in enumerate(row_sets):
            if row_covered_by_cluster(rset, cset, min_jaccard):
                covered.add(i)
        if len(chosen) >= max_clusters:
            break

    # Record coverage fraction at the end of the greedy phase.
    coverage = len(covered) / total_rows if total_rows else 0.0

    # -----------------------------------------------------------------
    # Phase 2: Fill remaining cluster slots by highest true_demand
    # -----------------------------------------------------------------
    # When the greedy phase saturates before reaching max_clusters, pad with the best
    # remaining candidates by economic demand so no cluster budget slots go to waste.
    while len(chosen) < max_clusters and len(chosen_indices) < len(candidate_clusters):
        best_idx = -1
        best_demand = -1
        for idx, (_, true_demand) in enumerate(candidate_clusters):
            if idx in chosen_indices:
                continue
            if true_demand > best_demand:
                best_demand = true_demand
                best_idx = idx
        if best_idx == -1:
            break
        chosen.append(list(candidate_clusters[best_idx][0]))
        chosen_indices.add(best_idx)
        # Update covered set so the returned coverage fraction accounts for fill candidates too.
        cset = cluster_sets[best_idx]
        for i, rset in enumerate(row_sets):
            if row_covered_by_cluster(rset, cset, min_jaccard):
                covered.add(i)
        coverage = len(covered) / total_rows if total_rows else 0.0

    return chosen, coverage


def inject_missing_top_skills(
    chosen_clusters: List[List[str]],
    row_sets: List[Set[str]],
    per_cluster_demand: List[int],
    high_demand: Dict[str, int],
    max_skills_per_cluster: int,
    max_clusters: int,
    allow_synthetic: bool = True,
    exclude_skills: Optional[Set[str]] = None,
) -> Tuple[List[List[str]], Dict[str, object]]:
    """Guarantee every high-demand skill appears in at least one cluster via five phases. Exclude_skills are not injected.

    The five injection phases are tried in order from least to most disruptive:
      1. Append:        Add the missing skill to a cluster that has a free slot; score by
                        co-occurrence overlap to pick the most compatible host cluster.
      2. Swap:          No free slots exist; replace a non-focus skill (lowest true_demand victim)
                        in any cluster to make room for the missing skill.
      3. Swap-dedup:    A focus skill appears in multiple clusters; replace one of its duplicates
                        in the cluster with the lowest demand to free a slot for the missing skill.
      4. Synthesize:    None of the above worked; create a new cluster containing the remaining
                        missing skills (batch up to max_skills).  Skipped when allow_synthetic=False
                        (e.g. post-drop re-injection to avoid inflating the cluster count).
      5. Force-append:  Last resort; append to any cluster with a free slot regardless of fit.

    After all phases, any skills that are still missing are reported in focus_skills_missed_after
    so the caller can log a warning.
    """
    exclude = exclude_skills or set()
    # Sort focus skills by descending true demand so higher-priority skills are injected first.
    focus_skills = [s for s, _ in sorted(high_demand.items(), key=lambda x: x[1], reverse=True) if s not in exclude]
    focus_set = set(focus_skills)
    # Work on a deep copy so the original chosen_clusters list is not mutated on failure.
    chosen = [list(c) for c in chosen_clusters]

    def _present() -> Set[str]:
        """Return the set of all skills currently in any chosen cluster."""
        return {s for c in chosen for s in c}

    def _missing() -> List[str]:
        """Return focus skills not yet covered by any chosen cluster, in demand order."""
        return [s for s in focus_skills if s not in _present()]

    missing_before = _missing()
    if not missing_before:
        # All focus skills already present; nothing to do.
        return chosen, {
            "focus_skills_missed_before": [],
            "focus_skills_missed_after": [],
            "actions_taken": 0,
            "action_details": [],
        }

    # Pre-compute per-skill row indices for fast co-occurrence scoring (used in Phase 1 and 4).
    skill_rows: Dict[str, Set[int]] = {}
    for i, rset in enumerate(row_sets):
        for s in rset:
            if s not in skill_rows:
                skill_rows[s] = set()
            skill_rows[s].add(i)

    actions: List[Dict[str, object]] = []

    # ---------------------------------------------------------------
    # Phase 1: Append missing skill to a cluster with room (< max_skills)
    # ---------------------------------------------------------------
    # Score host clusters by the size of the overlap between the missing skill's rows and
    # the union of each cluster's skill rows.  Higher overlap means the skill naturally
    # co-occurs with the cluster's existing skills in the data.
    for skill in list(_missing()):
        if skill in _present():
            continue
        s_rows = skill_rows.get(skill, set())
        best_idx, best_score = -1, -1
        for idx, cl in enumerate(chosen):
            if len(cl) >= max_skills_per_cluster or skill in cl:
                continue
            cluster_rows: Set[int] = set()
            for cs in cl:
                cluster_rows |= skill_rows.get(cs, set())
            score = len(s_rows & cluster_rows)
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx >= 0:
            old = sorted(chosen[best_idx])
            chosen[best_idx].append(skill)
            actions.append({"phase": "append", "skill": skill, "cluster": best_idx,
                            "old": old, "new": sorted(chosen[best_idx])})

    # ---------------------------------------------------------------
    # Phase 2: Swap out a non-focus skill (lowest demand victim)
    # ---------------------------------------------------------------
    # If every cluster is full, find a skill that is NOT in the focus (high-demand) set and
    # replace it with the missing skill.  Choose the victim with the lowest true_demand to
    # minimise the impact on overall coverage quality.
    for skill in list(_missing()):
        if skill in _present():
            continue
        best: Optional[Tuple[int, str, int]] = None
        for idx, cl in enumerate(chosen):
            for cs in cl:
                if cs in focus_set or cs == skill:
                    continue
                victim_demand = high_demand.get(cs, 0)
                if best is None or victim_demand < best[2]:
                    best = (idx, cs, victim_demand)
        if best is None:
            continue
        idx, old_skill, _ = best
        old = sorted(chosen[idx])
        chosen[idx] = [skill if s == old_skill else s for s in chosen[idx]]
        if len(set(chosen[idx])) != len(chosen[idx]):
            chosen[idx] = list(old)
            continue
        actions.append({"phase": "swap", "added": skill, "removed": old_skill,
                        "cluster": idx, "old": old, "new": sorted(chosen[idx])})

    # ---------------------------------------------------------------
    # Phase 3: Swap a duplicate focus skill (appears in 2+ clusters)
    # ---------------------------------------------------------------
    # When all clusters are full and every non-focus skill has already been swapped out (or
    # protected), look for a focus skill that appears in more than one cluster.  Remove it from
    # the cluster with the lowest demand (it is still covered by another cluster) and replace it
    # with the missing skill.  This recycles cluster real-estate without losing any focus coverage.
    for skill in list(_missing()):
        if skill in _present():
            continue
        skill_in_clusters: Dict[str, List[int]] = {}
        for idx, cl in enumerate(chosen):
            for cs in cl:
                skill_in_clusters.setdefault(cs, []).append(idx)
        best_dup: Optional[Tuple[int, str, int]] = None
        for victim, indices in skill_in_clusters.items():
            if len(indices) < 2 or victim == skill:
                continue
            for idx in indices:
                if skill in chosen[idx]:
                    continue
                cl_demand = per_cluster_demand[idx] if idx < len(per_cluster_demand) else 0
                if best_dup is None or cl_demand < best_dup[2]:
                    best_dup = (idx, victim, cl_demand)
        if best_dup is None:
            continue
        idx, old_skill, _ = best_dup
        old = sorted(chosen[idx])
        chosen[idx] = [skill if s == old_skill else s for s in chosen[idx]]
        if len(set(chosen[idx])) != len(chosen[idx]):
            chosen[idx] = list(old)
            continue
        actions.append({"phase": "swap_dup", "added": skill, "removed": old_skill,
                        "cluster": idx, "old": old, "new": sorted(chosen[idx])})

    # ---------------------------------------------------------------
    # Phase 4: Synthesize new clusters from remaining missing skills
    # ---------------------------------------------------------------
    # Phases 1-3 exhausted all modification options for existing clusters.  Create a brand-new
    # cluster containing up to max_skills_per_cluster of the still-missing focus skills (batched
    # together since they are all high-demand and likely co-occur in data).  Repeat until no
    # skills remain missing or the max_clusters cap is reached.  allow_synthetic=False is passed
    # during post-drop re-injection to prevent the cluster count from growing again after a drop.
    still_missing = _missing()
    while allow_synthetic and still_missing and len(chosen) < max_clusters:
        batch = still_missing[:max(2, min(max_skills_per_cluster, len(still_missing)))]
        if len(batch) < 2:
            for s in focus_skills:
                if s not in batch:
                    batch.append(s)
                    if len(batch) >= 2:
                        break
        chosen.append(batch)
        actions.append({"phase": "synthetic", "new_cluster": sorted(batch)})
        still_missing = _missing()

    # ---------------------------------------------------------------
    # Phase 5: Force-append to any cluster with room (last resort)
    # ---------------------------------------------------------------
    # All earlier phases failed (synthetic clusters not allowed or cap already reached).
    # Append the missing skill to the first cluster that has a free slot, ignoring co-occurrence
    # fit.  This guarantees the skill appears in at least one cluster even at the cost of
    # reduced Jaccard quality for that cluster.
    for skill in list(_missing()):
        if skill in _present():
            continue
        for idx, cl in enumerate(chosen):
            if len(cl) >= max_skills_per_cluster or skill in cl:
                continue
            old = sorted(chosen[idx])
            chosen[idx].append(skill)
            actions.append({"phase": "force_append", "skill": skill, "cluster": idx,
                            "old": old, "new": sorted(chosen[idx])})
            break

    missing_after = _missing()
    return chosen, {
        "focus_skills_missed_before": missing_before,
        "focus_skills_missed_after": missing_after,
        "actions_taken": len(actions),
        "action_details": actions,
    }


def _skill_rows_from_row_sets(row_sets: List[Set[str]]) -> Dict[str, Set[int]]:
    """Build skill -> set of row indices for co-occurrence scoring.

    This inverted index is used whenever we need to quickly find all rows that contain a
    given skill (e.g. when scoring a candidate host cluster for injection, or when finding
    co-occurrence candidates for replacement).  Building it once per major pipeline phase
    avoids repeated O(n_rows) scans.
    """
    out: Dict[str, Set[int]] = {}
    for i, rset in enumerate(row_sets):
        for s in rset:
            out.setdefault(s, set()).add(i)
    return out


def run_market(
    market: str,
    unmapped_mode: bool = False,
    exclude_skills: Optional[Set[str]] = None,
    pa: str = "DE",
    yr: str = "2023-2025",
    priority_skills: Optional[List[str]] = None,
) -> None:
    """Run demand-covering clustering for one market; write skill_clusters.json and skill_clusters_demand.json.

    pa  – Practice Area abbreviation (e.g. 'DE', 'EPS'). Controls data/{pa}/ and skills/{Market}_{pa}/.
    yr  – Year range string used in CSV filenames (e.g. '2023-2025').
    """
    skills_dir = SKILLS_DIR / f"{market}_{pa}"
    if unmapped_mode:
        csv_path = DATA_DIR / pa / f"DFC_YTD_{yr}_{pa}_V2_unmapped_{market}.csv"
        out_dir = skills_dir / UNMAPPED_CLUSTERS_SUBDIR
        params = dict(UNMAPPED_PARAMS)
        jaccard_by_union = JACCARD_BY_UNION_UNMAPPED.get(market, JACCARD_BY_UNION_UNMAPPED["Americas"])
    else:
        csv_path = DATA_DIR / pa / f"DFC_YTD_{yr}_{pa}_V2_corrected_normalized_{market}.csv"
        coocc_path = skills_dir / "co_occurrence.json"
        out_dir = skills_dir
        params = _params(market, pa=pa)
        jaccard_by_union = JACCARD_BY_UNION_BY_MARKET.get(market)

    out_json_path = out_dir / OUTPUT_JSON_NAME

    if not csv_path.exists():
        LOGGER.warning("Market '%s': CSV not found %s, skipping", market, csv_path.name)
        return
    if not unmapped_mode and not coocc_path.exists():
        LOGGER.warning("Market '%s': co_occurrence not found %s, skipping", market, coocc_path.name)
        return

    LOGGER.info("Market '%s': loading %s", market, csv_path.name)
    df_market = load_market_csv(csv_path)
    total_demand = len(df_market)

    max_clusters = int(params["max_clusters"])
    min_jaccard = float(params["min_jaccard_for_coverage"])
    min_skills = int(params["min_skills_per_cluster"])
    max_skills = int(params["max_skills_per_cluster"])
    if jaccard_by_union:
        LOGGER.info(
            "Market '%s': using length-wise jaccard (union>=%s -> %s, else %s)",
            market,
            jaccard_by_union.get("union_size_threshold"),
            jaccard_by_union.get("jaccard_when_union_ge"),
            jaccard_by_union.get("jaccard_when_union_lt"),
        )

    # Parse every row's skill set once; rows without any skills are excluded (unmappable).
    row_sets, row_indices = build_row_skill_sets(df_market, exclude_skills=exclude_skills)
    if unmapped_mode:
        # In unmapped mode there is no pre-built co_occurrence.json; derive both the candidate
        # clusters and the "high-demand" skill counts directly from the unmapped rows.
        high_demand = {}
        for rset in row_sets:
            for s in rset:
                high_demand[s] = high_demand.get(s, 0) + 1
        candidates = build_candidate_clusters_from_row_sets(
            row_sets, min_skills, max_skills, int(params["max_candidate_clusters"])
        )
        LOGGER.info(
            "Market '%s' (unmapped): built %d candidate clusters from %d rows",
            market, len(candidates), len(row_sets),
        )
        # Persist the in-memory co-occurrence so it can be inspected offline.
        out_dir.mkdir(parents=True, exist_ok=True)
        coocc_payload = [
            {"skills": ", ".join(skills), "skill_count": len(skills), "true_demand": demand}
            for skills, demand in candidates
        ]
        coocc_path_unmapped = out_dir / "co_occurrence.json"
        with coocc_path_unmapped.open("w", encoding="utf-8") as f:
            json.dump(coocc_payload, f, indent=2)
        LOGGER.info("Market '%s' (unmapped): wrote %s (%d combos)", market, coocc_path_unmapped.name, len(coocc_payload))
    else:
        # Normal mode: load pre-computed co_occurrence and high-demand skills from disk.
        candidates = load_candidate_clusters(coocc_path, market, exclude_skills=exclude_skills, pa=pa)
        LOGGER.info(
            "Market '%s': loaded %d candidate clusters (%d-%d skills)",
            market, len(candidates), min_skills, max_skills,
        )
        high_demand = load_high_demand_skills(skills_dir)

    # ===================================================================
    # Step 1: Greedy set-cover — select up to max_clusters from candidates
    # ===================================================================
    # The greedy algorithm picks the candidate that covers the most uncovered rows at each
    # iteration (using the global min_jaccard threshold, not the length-based variant).
    # After coverage saturates, remaining slots are filled by highest true_demand.
    total_rows = len(row_sets)
    chosen_clusters, coverage = greedy_set_cover(
        row_sets, candidates, total_rows, max_clusters=max_clusters, min_jaccard=min_jaccard
    )
    LOGGER.info(
        "Market '%s': chosen %d clusters; coverage %.2f%% (Jaccard >= %.2f)",
        market, len(chosen_clusters), coverage * 100, min_jaccard,
    )

    # Initial row assignment after the greedy phase — needed to compute per_cluster_demand for
    # the injection scoring (Phase 2 uses demand to pick the lowest-demand cluster to modify).
    row_to_cluster, per_cluster_demand, ties_broken = assign_rows_exclusive(
        row_sets, chosen_clusters, min_jaccard, jaccard_by_union=jaccard_by_union,
        priority_skills=priority_skills,
    )

    # ===================================================================
    # Step 2: Multi-phase injection — ensure all high-demand skills are covered
    # ===================================================================
    # Reload high_demand here for the normal branch (already set in the unmapped branch above).
    # The injection function modifies chosen_clusters in place and returns an action log.
    if not unmapped_mode:
        high_demand = load_high_demand_skills(skills_dir)
    chosen_clusters, inject_meta = inject_missing_top_skills(
        chosen_clusters=chosen_clusters,
        row_sets=row_sets,
        per_cluster_demand=per_cluster_demand,
        high_demand=high_demand,
        max_skills_per_cluster=max_skills,
        max_clusters=max_clusters,
        exclude_skills=exclude_skills,
    )
    if int(inject_meta.get("actions_taken", 0)) > 0:
        LOGGER.info(
            "Market '%s': injection applied %d actions to cover missing top skills",
            market, int(inject_meta["actions_taken"]),
        )
        # Re-assign rows after cluster membership changed due to injection; demand counts change.
        row_to_cluster, per_cluster_demand, ties_broken = assign_rows_exclusive(
            row_sets, chosen_clusters, min_jaccard, jaccard_by_union=jaccard_by_union,
            priority_skills=priority_skills,
        )

    # ===================================================================
    # Step 3: Drop low-demand clusters & migrate high-demand skills
    # ===================================================================
    # Clusters below min_mapped_demand_per_cluster are removed.  Before removal, any
    # high-demand skill that appears only in a dropped cluster is migrated into a kept
    # cluster (by co-occurrence score first; by swap of a duplicate/non-focus skill second)
    # so that no high-demand skill disappears from the output.
    min_demand = int(params.get("min_mapped_demand_per_cluster", 0))
    keep_indices = [idx for idx in range(len(chosen_clusters)) if per_cluster_demand[idx] >= min_demand]
    keep_indices.sort()
    to_drop = [idx for idx in range(len(chosen_clusters)) if idx not in keep_indices]
    if to_drop:
        # Build an inverted index once for all migration scoring in this block.
        skill_rows = _skill_rows_from_row_sets(row_sets)
        # Track which skills are currently covered by kept clusters (to skip already-safe skills).
        skills_in_kept = {s for i in keep_indices for s in chosen_clusters[i]}
        # Count how many kept clusters each skill appears in (needed to find safe swap victims).
        skill_count_kept: Dict[str, int] = {}
        for i in keep_indices:
            for cs in chosen_clusters[i]:
                skill_count_kept[cs] = skill_count_kept.get(cs, 0) + 1
        for idx in to_drop:
            for s in chosen_clusters[idx]:
                # Only migrate skills that are in the high-demand list and not already in a kept cluster.
                if s not in high_demand or s in skills_in_kept:
                    continue
                s_rows = skill_rows.get(s, set())
                # --- Migration strategy 1: append to the kept cluster with best co-occurrence ---
                best_k, best_score = -1, -1
                for k in keep_indices:
                    cl = chosen_clusters[k]
                    if len(cl) >= max_skills or s in cl:
                        continue
                    # Score by intersection of the skill's rows with the cluster's combined rows.
                    cluster_rows = set()
                    for cs in cl:
                        cluster_rows |= skill_rows.get(cs, set())
                    score = len(s_rows & cluster_rows)
                    if score > best_score:
                        best_score = score
                        best_k = k
                if best_k >= 0:
                    # Free slot found: append without disrupting any existing skill.
                    chosen_clusters[best_k].append(s)
                    skills_in_kept.add(s)
                    skill_count_kept[s] = skill_count_kept.get(s, 0) + 1
                else:
                    # --- Migration strategy 2: swap a non-focus or duplicate skill in a kept cluster ---
                    # Prefer swapping a skill that appears in multiple kept clusters (count > 1)
                    # or is not in the high-demand list — swapping it loses no overall coverage.
                    for k in keep_indices:
                        if s in chosen_clusters[k]:
                            break
                        for cs in chosen_clusters[k]:
                            if cs == s:
                                continue
                            if cs not in high_demand or skill_count_kept.get(cs, 0) > 1:
                                chosen_clusters[k] = [s if x == cs else x for x in chosen_clusters[k]]
                                skills_in_kept.add(s)
                                skill_count_kept[s] = skill_count_kept.get(s, 0) + 1
                                skill_count_kept[cs] = skill_count_kept.get(cs, 1) - 1
                                if skill_count_kept[cs] <= 0:
                                    skills_in_kept.discard(cs)
                                break
                        else:
                            continue
                        break
    dropped_count = len(to_drop)
    if dropped_count > 0:
        keep_indices.sort()
        # Remap old cluster indices to new contiguous indices after removal.
        old_to_new = {old: new for new, old in enumerate(keep_indices)}
        chosen_clusters = [chosen_clusters[i] for i in keep_indices]
        # Rows that were assigned to a dropped cluster are now unmapped (-1).
        row_to_cluster = [old_to_new.get(cidx, -1) for cidx in row_to_cluster]
        per_cluster_demand = [sum(1 for c in row_to_cluster if c == i) for i in range(len(chosen_clusters))]
        LOGGER.info(
            "Market '%s': dropped %d clusters with mapped demand < %d; %d clusters remain",
            market, dropped_count, min_demand, len(chosen_clusters),
        )
        # Re-inject any high-demand skills lost during the drop (append/swap only; no new clusters).
        chosen_clusters, inject_meta2 = inject_missing_top_skills(
            chosen_clusters=chosen_clusters,
            row_sets=row_sets,
            per_cluster_demand=per_cluster_demand,
            high_demand=high_demand,
            max_skills_per_cluster=max_skills,
            max_clusters=max_clusters,
            allow_synthetic=False,
        )
        if int(inject_meta2.get("actions_taken", 0)) > 0:
            # Re-assign after post-drop injection changed cluster membership.
            row_to_cluster, per_cluster_demand, ties_broken = assign_rows_exclusive(
                row_sets, chosen_clusters, min_jaccard, jaccard_by_union=jaccard_by_union,
                priority_skills=priority_skills,
            )

    # ===================================================================
    # Step 4: Trim and replace poor-fit skills
    # ===================================================================
    # A skill is a "mismatch" in a cluster if its occurrence (count of assigned rows that
    # contain it) is below MIN_OCCURRENCE_FRACTION_OF_MAX * max_cluster_occurrence.
    # Using the fraction-of-max rule (rather than a median) is intentional: it correctly
    # handles 2-skill clusters where the median would always flag one skill as below the median.
    #
    # Americas: each mismatch is replaced by the best candidate from simulated demand.
    # EMEA: mismatch skills are trimmed without replacement (simpler / more conservative).
    # Empty clusters after trimming are dropped entirely.
    cluster_skill_occurrence = compute_cluster_skill_occurrence(
        row_sets, row_to_cluster, chosen_clusters, len(chosen_clusters)
    )
    skill_rows = _skill_rows_from_row_sets(row_sets)
    trimmed_any = False
    for idx, cl in enumerate(chosen_clusters):
        occs = [cluster_skill_occurrence[idx].get(s, 0) for s in cl]
        max_occ = max(occs) if occs else 0
        # min_occ is the floor below which a skill is considered a mismatch.
        min_occ = max(1, int(MIN_OCCURRENCE_FRACTION_OF_MAX * max_occ)) if max_occ > 0 else 1
        mismatches = [s for s in cl if cluster_skill_occurrence[idx].get(s, 0) < min_occ]
        # new_cl starts as the cluster without its mismatch skills.
        new_cl = [s for s in cl if cluster_skill_occurrence[idx].get(s, 0) >= min_occ]
        if market != "EMEA":
            excluded_for_replacement = set(mismatches)
            # Americas only: replace each mismatch with the skill that gives max mapped_demand when simulated
            for _ in mismatches:
                repl = find_best_replacement_by_simulated_demand(
                    row_sets, chosen_clusters, idx, new_cl, excluded_for_replacement,
                    skill_rows, min_jaccard, max_skills, MIN_OCCURRENCE_FRACTION_OF_MAX,
                    jaccard_by_union=jaccard_by_union,
                )
                if repl is not None and repl not in new_cl and len(new_cl) < max_skills:
                    new_cl.append(repl)
                    excluded_for_replacement.add(repl)
                    trimmed_any = True
                    LOGGER.info(
                        "Market '%s': cluster %d replaced low-occurrence skill with '%s' (best simulated demand)",
                        market, idx, repl,
                    )
                else:
                    # No replacement found; mark trimmed_any so a re-assignment is triggered later.
                    trimmed_any = trimmed_any or (len(new_cl) != len(cl))
            # If trimming pushed the cluster below min_skills, try to add replacements to fill back.
            while len(new_cl) < min_skills:
                repl = find_best_replacement_by_simulated_demand(
                    row_sets, chosen_clusters, idx, new_cl, excluded_for_replacement,
                    skill_rows, min_jaccard, max_skills, MIN_OCCURRENCE_FRACTION_OF_MAX,
                    jaccard_by_union=jaccard_by_union,
                )
                if repl is None or repl in new_cl:
                    break
                new_cl.append(repl)
                excluded_for_replacement.add(repl)
                trimmed_any = True
        else:
            # EMEA: trim only, no replacement — simpler cluster structure is preferred.
            trimmed_any = trimmed_any or (len(new_cl) != len(cl))
        if len(new_cl) != len(cl):
            trimmed_any = True
        chosen_clusters[idx] = new_cl
    # Drop clusters that became empty after trimming (all skills were mismatches).
    empty = [i for i, cl in enumerate(chosen_clusters) if len(cl) == 0]
    if empty:
        keep_idx = [i for i in range(len(chosen_clusters)) if i not in empty]
        old_to_new = {old: new for new, old in enumerate(keep_idx)}
        chosen_clusters = [chosen_clusters[i] for i in keep_idx]
        LOGGER.info("Market '%s': dropped %d clusters that had only 0-occurrence or low-fit skills", market, len(empty))

    # Re-assign rows from scratch against the final trimmed clusters so demand counts are accurate.
    if trimmed_any or empty:
        row_to_cluster, per_cluster_demand, ties_broken = assign_rows_exclusive(
            row_sets, chosen_clusters, min_jaccard, jaccard_by_union=jaccard_by_union,
            priority_skills=priority_skills,
        )
        cluster_skill_occurrence = compute_cluster_skill_occurrence(
            row_sets, row_to_cluster, chosen_clusters, len(chosen_clusters)
        )

    # ===================================================================
    # Step 5: Final drop — remove any cluster still below the demand threshold
    # ===================================================================
    # The trim-and-replace step can cause previously-kept clusters to lose rows (if their
    # Jaccard scores changed after skill substitution), potentially dropping below
    # min_mapped_demand_per_cluster.  This final unconditional drop ensures the output never
    # contains under-threshold clusters regardless of prior pipeline decisions.
    if min_demand > 0:
        below = [i for i in range(len(chosen_clusters)) if per_cluster_demand[i] < min_demand]
        if below:
            keep_idx = [i for i in range(len(chosen_clusters)) if i not in below]
            old_to_new = {old: new for new, old in enumerate(keep_idx)}
            chosen_clusters = [chosen_clusters[i] for i in keep_idx]
            # Rows that were assigned to a now-dropped cluster become unmapped.
            row_to_cluster = [old_to_new.get(cidx, -1) if cidx not in below else -1 for cidx in row_to_cluster]
            per_cluster_demand = [sum(1 for c in row_to_cluster if c == i) for i in range(len(chosen_clusters))]
            cluster_skill_occurrence = compute_cluster_skill_occurrence(
                row_sets, row_to_cluster, chosen_clusters, len(chosen_clusters)
            )
            LOGGER.info(
                "Market '%s': final drop removed %d clusters with mapped demand < %d",
                market, len(below), min_demand,
            )

    # ===================================================================
    # Step 6: Final metrics and output writing
    # ===================================================================
    # All demand/coverage numbers are computed against the final post-trim, post-drop state.
    total_mapped = sum(per_cluster_demand)
    unmapped = total_rows - total_mapped
    coverage = total_mapped / total_rows if total_rows else 0.0
    LOGGER.info(
        "Market '%s': final assignment total_mapped=%d, unmapped=%d, coverage=%.2f%%",
        market, total_mapped, unmapped, coverage * 100,
    )
    LOGGER.info("Market '%s': ties broken (same Jaccard): %d (smaller cluster)", market, ties_broken)

    unique_skills_in_clusters = sorted(set(s for cl in chosen_clusters for s in cl))
    skills_in_clusters_set = set(unique_skills_in_clusters)
    missed_list = [(s, high_demand[s]) for s in high_demand if s not in skills_in_clusters_set]
    missed_list.sort(key=lambda x: x[1], reverse=True)
    high_demand_skills_missed = dict(missed_list)
    LOGGER.info(
        "Market '%s': unique skills in clusters=%d; high-demand skills missed=%d (of %d high-demand)",
        market, len(unique_skills_in_clusters), len(high_demand_skills_missed), len(high_demand),
    )

    per_cluster_cv = compute_cluster_cv(
        df_market, row_indices, row_to_cluster, len(chosen_clusters), time_col=TIME_COL
    )
    per_cluster_cv_2025 = compute_cluster_cv(
        df_market, row_indices, row_to_cluster, len(chosen_clusters), time_col=TIME_COL, year_filter=2025
    )

    # Build one record per cluster, sorted by mapped_demand descending for easy review.
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

    # Assemble the full demand-metrics payload written to skill_clusters_demand.json.
    payload = {
        "market": market,
        "params": params,
        "min_jaccard_for_coverage": min_jaccard,
        "assignment_rule": "exclusive: each row assigned to the cluster with highest Jaccard (>= min_jaccard); tie-break by smaller cluster",
        "ties_broken": ties_broken,
        "total_demand": total_demand,
        "total_mapped_demand": total_mapped,
        "unmapped_demand": unmapped,
        "coverage_fraction": round(coverage, 4),
        "num_clusters": len(chosen_clusters),
        "unique_skills_in_clusters": unique_skills_in_clusters,
        "unique_skills_count": len(unique_skills_in_clusters),
        "high_demand_skills_missed": high_demand_skills_missed,
        "high_demand_skills_missed_count": len(high_demand_skills_missed),
        "injection_focus_missed_before": inject_meta.get("focus_skills_missed_before", []),
        "injection_focus_missed_after": inject_meta.get("focus_skills_missed_after", []),
        "injection_actions_taken": int(inject_meta.get("actions_taken", 0)),
        "injection_details": inject_meta.get("action_details", []),
        "clusters_sorted_by": "mapped_demand descending",
        "cv_note": "coefficient of variation (std/mean) of monthly demand per cluster",
        "cv_2025_note": "CV of monthly demand in 2025 only",
        "skill_occurrence_note": "count of mapped rows in this cluster containing each skill",
        "clusters": cluster_list,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    # skill_clusters_demand.json: full metrics payload for auditing and downstream analysis.
    with out_json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    LOGGER.info("Market '%s': wrote %s", market, out_json_path.name)

    # skill_clusters.json: compact file consumed by apply_clusters.py; includes min_jaccard
    # so the apply step uses the same threshold that was used during cluster construction.
    skill_clusters_path = out_dir / SKILL_CLUSTERS_JSON
    with skill_clusters_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "market": market,
                "num_clusters": len(chosen_clusters),
                "min_jaccard_for_coverage": min_jaccard,
                "clusters": [sorted(cl) for cl in chosen_clusters],
            },
            f,
            indent=2,
        )
    LOGGER.info("Market '%s': wrote %s (final clusters only)", market, SKILL_CLUSTERS_JSON)


def main() -> None:
    """Run demand-covering clustering for all markets; --unmapped runs on unmapped rows only.

    CLI entry point.  Parses arguments, applies any global filters (--exclude-skills,
    --priority-skills), and calls run_market() for each market in MARKETS.  The practice-area
    (--practice-area) and year-range (--year-range) arguments control which input files are
    read and which output subdirectories are written.
    """
    parser = argparse.ArgumentParser(description="Demand-covering skill clusters per market; --unmapped for second pass on unmapped rows.")
    parser.add_argument("--unmapped", action="store_true", help="Cluster unmapped rows only (max 5 clusters, relaxed Jaccard); writes to skills/<Market>_<PA>/unmapped_clusters/")
    parser.add_argument("--exclude-skills", type=str, default="", help="Comma-separated skills to exclude from row data and clusters (e.g. 'API Development'); not used in Jaccard or clustering.")
    parser.add_argument("--priority-skills", type=str, default="", help="Comma-separated skills; rows containing any of these are force-assigned to the cluster that contains that skill (no Jaccard threshold).")
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
    exclude_skills = {s.strip() for s in args.exclude_skills.split(",") if s.strip()} if args.exclude_skills else None
    priority_skills = [s.strip() for s in args.priority_skills.split(",") if s.strip()] if args.priority_skills else None
    if exclude_skills:
        LOGGER.info("Excluding %d skills from clustering: %s", len(exclude_skills), sorted(exclude_skills))
    if priority_skills:
        LOGGER.info("Priority skills (force-assign to containing cluster): %s", priority_skills)
    if args.unmapped:
        LOGGER.info("Unmapped mode: clustering rows from v2_unmapped CSV per market | PA=%s | yr=%s", pa, yr)
    else:
        LOGGER.info("Starting demand-covering clusters (skill_clusters.json + skill_clusters_demand.json per market) | PA=%s | yr=%s", pa, yr)
    for market in MARKETS:
        run_market(market, unmapped_mode=args.unmapped, exclude_skills=exclude_skills, pa=pa, yr=yr, priority_skills=priority_skills)
    LOGGER.info("Done (all markets processed)")


if __name__ == "__main__":
    main()
