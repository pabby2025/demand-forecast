"""
This script normalizes skills in `DFC_YTD_2023-2025_skill.csv` using
`skills/skill_normalization_llm.json`, filters them into stable/high-demand
groups, and produces a set of demand diagnostics for later clustering and
forecasting.

High-level steps
----------------
1. Load and initial cleanup
   - Read `data/DFC_YTD_2023-2025_skill.csv`.
   - Drop the `Skill Clusters` column immediately if it exists (this script is
     the only authority for any future clustering).

2. Skill normalization (full dataset, before dropping any years)
   - Work from `Technical Skills Required`.
   - Split each cell into leaf skills, handling commas/semicolons/colons and
     extracting skills inside parentheses as separate entries
     (e.g. `Design (UX, Prod/Srvc)` → `Design`, `UX`, `Prod/Srvc`).
   - Use `skills/skill_normalization_llm.json` to map each variant to a
     normalized skill name; if no mapping exists, keep the original leaf name.
   - Per row, deduplicate skills and create:
       - `Skills Normalized`: all normalized skills (including unmapped ones).
       - `Not found`: only unmapped leaf skills.
   - On this full dataset, compute total demand per normalized skill
     (each row contributes 1 per unique skill) and save per market:
       - `skills/<Market>/total_skills.json` (overall demand, including excluded years).

3. Year derivation and exclusion
   - Derive demand year from `Requirement Start Date`.
   - Drop all rows where the derived year equals `EXCLUDE_YEAR` (currently 2022).
   - Track and log:
       - total rows at start,
       - total rows after dropping the excluded year,
       - total rows after all later row filters.

4. Normalization on the filtered dataset
   - Re-run normalization on the year-filtered dataframe to keep `Skills Normalized`
     and `Not found` consistent with the reduced data.
   - Compute:
       - `total_skill_demand_counts_filtered`: total demand per normalized skill
         on the filtered dataset.
       - `unmapped_demand_counts`: total demand per unmapped leaf skill.
   - Save unmapped skill demand as:
       - `skills/<Market>/unmapped_skills.json`.

5. Year-wise demand on `Skills Normalized`
   - Using only `Skills Normalized`, compute `year -> {skill -> demand}` for all
     remaining years (2023+).
   - Write one JSON per year with that mapping:
       - `skills/<Market>/total_skills_<year>.json`.

6. Low-demand identification and Skill Groups construction
   - Using year-wise demand from step 5, mark a skill as "low demand" if:
       - It has some historical demand, and
       - Its maximum per-year demand is strictly less than
         `max(LOW_DEMAND_ABS_MIN, max_year_demand / 3)` for that skill.
   - Save all such low-demand skills and their total filtered demand as:
       - `skills/<Market>/low_demand_skills.json`.
   - Define `groupable_skills` as those whose total filtered demand is at least
     the market threshold (see `MIN_TOTAL_DEMAND_FOR_GROUPS_BY_MARKET`, default
     `MIN_TOTAL_DEMAND_FOR_GROUPS`) and are not low-demand.
   - Further refine `groupable_skills`:
       - Remove any skill whose demand in year 2025 is zero.
       - Compute coefficient of variation (CV) using only 2025 demand
         (`CV_YEARS = [2025]`); keep only skills with CV_2025 < `MAX_CV_FOR_GROUPS`.
       - Save CV-excluded skills and their year-wise demands to:
         `skills/<Market>/cv_excluded_skills.json`.
   - Build `Skill Groups`:
       - For each row, re-split `Skills Normalized` into leaf skills.
       - Keep only skills in `groupable_skills` and not low-demand.
       - Deduplicate, sort, and join as a comma+space string.
   - Skill Groups therefore contain only high-demand skills; rows with only
     low-demand skills end up with empty Skill Groups and are dropped, so the
     final output has no row consisting solely of low-demand skills.

7. Row-level filtering
   - Drop any row where `Skill Groups` is empty.
   - Then drop any row where `Skills Normalized` is empty.
   - Realign the derived years series to match the filtered rows.

8. Final high-demand skills and yearly demand (post-filter)
   - From the final filtered dataset:
       - Recompute year-wise demand for all skills using `Skills Normalized` and
         overwrite / write `skills/<Market>/total_skills_<year>.json`.
       - Treat the final `groupable_skills` as the high-demand set and write
         their total filtered demand to:
           - `skills/<Market>/high_demand_skills.json`.
   - Single-skill (high-demand alone) occurrence counts are written to
     `skills/<Market>/single_large_occ.json` (same list format as
     co_occurrence.json entries with skill_count 1).
   - Full co-occurrence (all skill combinations with true demand) is written to
     `skills/<Market>/co_occurrence.json` (same format as produced by skill_clustering).

9. Growth analysis (2023–2025)
   - Using year-wise demand from the final filtered data:
       - Compute per-year ranks for each skill (1 = highest demand in that year).
       - For each skill, compute:
           - year-wise demands,
           - year-wise ranks,
           - rank change from first to last year,
           - absolute rank change,
           - percentage change in demand where applicable,
           - a simple trend label: rising / declining / stable.
       - Select the top-N (default 50) by absolute rank change.
       - Save them to `skills/<Market>/skill_growth_analysis.json`.
       - Generate `skills/<Market>/skill_growth_analysis.png` showing their demand
         trajectories over the years.

10. XYZ (forecastability) segmentation
    - For the final high-demand skills, compute CV using only year 2025.
    - Segment skills into:
        - X-skills (stable): CV_2025 < 0.5
        - Y-skills (variable): 0.5 <= CV_2025 <= 1.0
        - Z-skills (sporadic): CV_2025 > 1.0
    - Save three JSONs mapping skill → {cv_2025, demands}:
        - `skills/<Market>/xyz_x_skills.json`
        - `skills/<Market>/xyz_y_skills.json`
        - `skills/<Market>/xyz_z_skills.json`

11. Final CSV output
    - Write the fully processed dataframe (after year and row filters) to:
        - `data/DFC_YTD_2023-2025_v1_corrected_normalized_<Market>.csv`
      which contains:
        - original metadata columns (except dropped ones),
        - `Skills Normalized`,
        - `Not found`,
        - `Skill Groups`.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import coloredlogs
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm


LOGGER = logging.getLogger(__name__)
coloredlogs.install(level=logging.INFO, logger=LOGGER, isatty=True)


PROJECT_ROOT = Path(__file__).resolve().parent
SKILLS_DIR = PROJECT_ROOT / "skills"
DATA_DIR = PROJECT_ROOT / "data"

NORMALIZATION_JSON_PATH = SKILLS_DIR / "skill_normalization_llm2.json"
ELIMINATE_SKILLS_JSON_PATH = SKILLS_DIR / "eliminate_skills.json"

# Market segmentation: normalization and all skill outputs run separately per market.
MARKET_COL = "Market"
MARKETS = ["Americas", "EMEA"]

MIN_YEAR = 2024  # Drop all rows whose demand year is strictly below this

# Years that are allowed to contribute to the aggregate `total_skills.json`.
# This ensures that `total_skills.json` only reflects 2024–2025 demand even if
# the input CSV later contains additional years (for example, 2026+).
TOTAL_SKILLS_YEARS = {2024, 2025}

# Minimum total demand for a skill to be used in Skill Groups (default).
MIN_TOTAL_DEMAND_FOR_GROUPS = 900
# Per-market overrides; EMEA uses a lower threshold so more skills qualify as high-demand.
MIN_TOTAL_DEMAND_FOR_GROUPS_BY_MARKET: Dict[str, int] = {"EMEA": 250}

# Base threshold for defining "low-demand" skills. The effective threshold is
# max(LOW_DEMAND_ABS_MIN, skill_total_demand / 3).
LOW_DEMAND_ABS_MIN = 100

MAX_CV_FOR_GROUPS = 0.9
CV_YEARS = [2025]

# API Development deduplication: if any of these canonical programming languages
# appears in the same row, "API Development" is redundant and gets removed.
API_DEVELOPMENT_CANONICAL = "API Development"
API_DEVELOPMENT_LANGUAGES: Set[str] = {
    "Java", "Python", "JavaScript", "Node JS", ".NET",
    "TypeScript", "Spring Boot", "PHP", "Ruby", "Go", "Kotlin", "Scala",
}

# Dynamic high-demand floor: always surface at least this many top skills,
# but only if the Nth skill still has demand >= the floor.
MIN_HIGH_DEMAND_TOP_N = 30
MIN_HIGH_DEMAND_TOP_N_FLOOR = 600

# Recency gate: a skill must have meaningful 2025 demand to qualify as high-demand.
# It must clear BOTH an absolute floor AND a fraction of its own total demand,
# so a skill coasting purely on historical volume cannot sneak through.
MIN_2025_DEMAND_ABS = 50           # absolute minimum 2025 occurrences
MIN_2025_DEMAND_FRACTION = 0.20    # 2025 demand must be >= 20% of total filtered demand

TECHNICAL_SKILLS_COL = "Technical Skills Required"
NORMALIZED_COL = "Skills Normalized"
NOT_FOUND_COL = "Not found"

PROJECT_MANAGEMENT_NORMALIZED_NAME = "Project Management"
PROJECT_MANAGEMENT_PREFIX = "project "

def load_normalization_mapping(json_path: Path) -> Dict[str, str]:
    """
    Load the normalization mapping from the given JSON file.

    The JSON is expected to be of the form:
    {
        "NORMALIZED_NAME": {
            "variants": ["Variant A", "Variant B", ...]
        },
        ...
    }

    Returns a dictionary mapping each variant (case-insensitive) to its normalized name.
    """
    LOGGER.info("Loading normalization mapping from %s", json_path)
    if not json_path.exists():
        raise FileNotFoundError(f"Normalization JSON not found at: {json_path}")

    # Read the raw JSON; each key is a canonical/normalized skill name.
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Invert the structure from {normalized -> {variants: [...]}} to
    # {variant_lower -> normalized} so lookups during normalization are O(1).
    variant_to_normalized: Dict[str, str] = {}
    for normalized_name, info in data.items():
        variants: Iterable[str] = info.get("variants", [])
        for variant in variants:
            # Normalize the lookup key to lowercase for case-insensitive matching.
            key = variant.strip().lower()
            if not key:
                continue
            # If duplicates exist, first one wins; this is usually fine.
            variant_to_normalized.setdefault(key, normalized_name)

    LOGGER.info("Loaded %d normalized skills with %d total variants",
                len(data), len(variant_to_normalized))
    return variant_to_normalized


def load_eliminate_skills(json_path: Path) -> Set[str]:
    """
    Load the list of skills that should be eliminated from downstream processing.

    Reads the ``eliminate`` array from the JSON file.  Skills in this list are
    removed from both the normalized output and the not-found (unmapped) list so
    that they never contribute to demand counts, Skill Groups, or co-occurrence data.

    Returns an empty set if the file does not exist, allowing the pipeline to run
    without any elimination list.
    """
    if not json_path.exists():
        # Not having an elimination list is a valid state — skip silently.
        LOGGER.info(
            "Eliminate-skills JSON not found at %s; proceeding without skill elimination",
            json_path,
        )
        return set()

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Strip whitespace and drop any blank entries to avoid accidental matches.
    raw_list = data.get("eliminate", [])
    eliminate_skills: Set[str] = {
        str(skill).strip() for skill in raw_list if str(skill).strip()
    }

    LOGGER.info("Loaded %d skills to eliminate from processing", len(eliminate_skills))
    return eliminate_skills


def split_leaf_skills(raw_value: Optional[str]) -> List[str]:
    """
    Split the `Technical Skills Required` string into individual leaf skills.

    This function splits on common delimiters (comma, semicolon, colon), as skills
    may be separated by any of these. It also extracts skills inside parentheses as
    separate entries so they can be normalized independently. It trims whitespace
    and drops empty items.

    Parsing is done in three phases:
    1. Delimiter-split phase: split the raw string on commas/semicolons/colons that
       appear *outside* parentheses, preserving parenthetical content intact.
    2. Parenthesis-expansion phase: for each token from phase 1, extract the outer
       (non-parenthetical) text and each parenthetical segment as separate skills.
    3. Special-case phase: handle the known "HTML CSS JavaScript" concatenation that
       lacks any delimiter.
    """
    if raw_value is None:
        return []
    text = str(raw_value).strip()
    if not text:
        return []

    # ----------------------------------------------------------------
    # Phase 1: Delimiter-split (respecting parenthesis nesting).
    # We walk the string character-by-character so that commas/semicolons/colons
    # that appear inside parentheses are NOT treated as delimiters.
    # ----------------------------------------------------------------
    delimiters = {",", ";", ":"}
    parts: List[str] = []
    current: List[str] = []
    depth = 0  # parenthesis depth

    for ch in text:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            # Avoid negative depth in case of mismatched parens
            depth = max(0, depth - 1)
            current.append(ch)
        elif ch in delimiters and depth == 0:
            # We are at a top-level delimiter — flush the current token.
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
        else:
            current.append(ch)

    # Flush any trailing token that had no trailing delimiter.
    token = "".join(current).strip()
    if token:
        parts.append(token)

    # ----------------------------------------------------------------
    # Phase 2: Parenthesis-expansion.
    # "Design (UX, Prod/Srvc)" -> ["Design", "UX", "Prod/Srvc"].
    # The outer label ("Design") and each inner item are emitted separately so
    # they are each normalized and counted independently.
    # ----------------------------------------------------------------
    expanded_with_parens: List[str] = []
    for item in parts:
        cleaned = item.strip()
        if not cleaned:
            continue
        outer_chars: List[str] = []
        inner_segments: List[str] = []
        depth = 0
        current_inner: List[str] = []

        for ch in cleaned:
            if ch == "(":
                depth += 1
                if depth == 1:
                    # Start collecting the inner content (don't include the '(' itself).
                    current_inner = []
                else:
                    # Nested paren — keep it as part of the inner text.
                    current_inner.append(ch)
                continue
            if ch == ")" and depth > 0:
                depth = max(0, depth - 1)
                if depth == 0:
                    # Closing the outermost paren — save this inner segment.
                    inner_text = "".join(current_inner).strip()
                    if inner_text:
                        inner_segments.append(inner_text)
                else:
                    current_inner.append(ch)
                continue
            if depth > 0:
                current_inner.append(ch)
            else:
                # Outside any parentheses — this is part of the outer label.
                outer_chars.append(ch)

        # Collapse extra internal whitespace from the outer label.
        outer_text = " ".join("".join(outer_chars).split()).strip()
        if outer_text:
            expanded_with_parens.append(outer_text)

        # Split each inner segment on its own delimiters and emit individual tokens.
        for segment in inner_segments:
            segment_tokens = (
                segment.replace(";", ",").replace(":", ",").split(",")
            )
            for token in segment_tokens:
                cleaned_token = token.strip()
                if cleaned_token:
                    expanded_with_parens.append(cleaned_token)

    # ----------------------------------------------------------------
    # Phase 3: Special-case handling for concatenated HTML/CSS/JavaScript.
    # Some source rows contain "HTML CSS JavaScript" without any delimiter.
    # Explicitly split this known pattern so each technology is counted separately.
    # ----------------------------------------------------------------
    expanded_parts: List[str] = []
    for item in expanded_with_parens:
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.lower() == "html css javascript":
            expanded_parts.extend(["HTML", "CSS", "JavaScript"])
        else:
            expanded_parts.append(cleaned)

    return expanded_parts


def split_skills_cell(raw_value: Optional[str]) -> List[str]:
    """Split a skill-group cell into individual skills."""
    return split_leaf_skills(raw_value)

def normalize_project_management_skill(leaf: str) -> Optional[str]:
    """
    Normalize skills that represent specific types of project work to Project Management.

    Any skill that starts with "Project " (case-insensitive), such as
    "Project Budgeting" or "project quality management", is mapped to
    "Project Management".
    """
    if not leaf:
        return None

    stripped = leaf.strip()
    if not stripped:
        return None

    key = stripped.lower()
    if not key.startswith(PROJECT_MANAGEMENT_PREFIX):
        return None

    return PROJECT_MANAGEMENT_NORMALIZED_NAME


def normalize_skills_for_row(
    raw_value: Optional[str],
    variant_to_normalized: Dict[str, str],
    eliminate_skills: Optional[Set[str]] = None,
) -> Tuple[List[str], List[str]]:
    """
    Normalize the skills for a single row.

    Decision flow for each leaf skill:
    1. If the leaf starts with "Project " (case-insensitive), map it straight to
       "Project Management" without consulting the JSON mapping — this catches all
       sub-types (e.g. "Project Budgeting") uniformly.
    2. Otherwise, look up the lowercase form in ``variant_to_normalized``.
       - Hit  -> use the canonical normalized name.
       - Miss -> keep the original leaf name as-is and add it to the not-found set.
    3. After building the normalized set, apply two post-processing passes:
       - API deduplication: discard "API Development" when a specific language
         (Java, Python, etc.) is already present on the same row.
       - Elimination: remove any skill that appears in the ``eliminate_skills`` set.

    Returns:
    - normalized_unique: list of unique normalized skills (sorted for determinism).
      Unmapped skills are also included here using their original leaf names.
    - not_found_unique: list of unique unmapped leaf skills (sorted for determinism).
    """
    # Phase 1: split the raw cell value into individual leaf tokens.
    leaf_skills = split_leaf_skills(raw_value)
    normalized_set: Set[str] = set()
    not_found_set: Set[str] = set()

    for leaf in leaf_skills:
        key = leaf.strip().lower()
        if not key:
            continue

        # Decision branch A: "Project X" prefix rule — takes priority over JSON mapping
        # so that sub-types like "Project Quality Management" are always consolidated.
        project_normalized = normalize_project_management_skill(leaf)
        if project_normalized is not None:
            normalized_set.add(project_normalized)
            continue

        # Decision branch B: JSON mapping lookup (case-insensitive).
        normalized = variant_to_normalized.get(key)
        if normalized:
            # Mapped variant -> use the canonical normalized name.
            normalized_set.add(normalized)
        else:
            # Unmapped variant -> keep original leaf name and record as not-found.
            # Keeping it in the normalized set means unmapped skills still appear
            # in "Skills Normalized" output for visibility.
            normalized_set.add(leaf)
            not_found_set.add(leaf)  # keep original casing for reporting

    # Phase 2 post-processing: API Development deduplication.
    # Remove "API Development" when a core programming language is also present —
    # in that context the language already implies API development work.
    if API_DEVELOPMENT_CANONICAL in normalized_set and (
        normalized_set & API_DEVELOPMENT_LANGUAGES
    ):
        normalized_set.discard(API_DEVELOPMENT_CANONICAL)

    # Phase 3 post-processing: explicit skill elimination.
    if eliminate_skills:
        # Remove any skills that are explicitly marked for elimination so they
        # never appear in the normalized outputs or downstream demand counts.
        normalized_set = {s for s in normalized_set if s not in eliminate_skills}
        not_found_set = {s for s in not_found_set if s not in eliminate_skills}

    # Sort both lists for reproducible output ordering across runs.
    normalized_list = sorted(normalized_set)
    not_found_list = sorted(not_found_set)
    return normalized_list, not_found_list


def format_skills_for_output(skills: List[str]) -> str:
    """
    Format a list of skills for CSV output as a comma+space separated string.
    """
    if not skills:
        return ""
    return ", ".join(skills)


def process_dataframe(
    df: pd.DataFrame,
    variant_to_normalized: Dict[str, str],
    eliminate_skills: Optional[Set[str]] = None,
) -> Tuple[
    pd.DataFrame,
    Set[str],
    Dict[str, int],
    Dict[str, int],
]:
    """
    Process the dataframe, adding normalized and not-found columns.

    Iterates over every row in ``df``, calls ``normalize_skills_for_row`` for each
    value in the ``Technical Skills Required`` column, and accumulates:
    - ``Skills Normalized``: comma-separated normalized skills for each row.
    - ``Not found``: comma-separated unmapped leaf skills for each row.
    - Aggregate demand counts (number of rows containing each skill).

    Demand counting rule: each row contributes exactly 1 to the demand count for
    each *unique* skill it contains — duplicate skills within the same row are
    already deduplicated by ``normalize_skills_for_row``.

    Returns:
    - The modified dataframe (with the two new columns added in-place).
    - The global set of all unmapped skills (union across all rows).
    - A dict with overall demand counts per normalized skill
      (including unmapped skills by their original names).
    - A dict with demand counts per unmapped skill.
    """
    if TECHNICAL_SKILLS_COL not in df.columns:
        raise KeyError(
            f"Expected column '{TECHNICAL_SKILLS_COL}' not found in input CSV."
        )

    LOGGER.info("Normalizing skills for %d rows", len(df))

    # Accumulators populated during the row-by-row loop below.
    all_unmapped: Set[str] = set()
    total_skill_demand_counts: Dict[str, int] = {}
    unmapped_demand_counts: Dict[str, int] = {}

    # Collect column values as plain lists first; assigning a list to a DataFrame
    # column is far faster than growing the column one cell at a time.
    normalized_col_values: List[str] = []
    not_found_col_values: List[str] = []

    # Main normalization loop — one call to normalize_skills_for_row per CSV row.
    for _idx, raw_value in tqdm(
        df[TECHNICAL_SKILLS_COL].items(), desc="Normalizing skills"
    ):
        normalized_list, not_found_list = normalize_skills_for_row(
            raw_value, variant_to_normalized, eliminate_skills
        )

        # Format as comma-separated strings for CSV output.
        normalized_col_values.append(format_skills_for_output(normalized_list))
        not_found_col_values.append(format_skills_for_output(not_found_list))

        # Track the global union of all unmapped skill names (for summary logging).
        all_unmapped.update(not_found_list)

        # Update overall demand counts: per row, each unique skill (mapped/unmapped)
        # counts once — this mirrors how downstream clustering weights demand.
        for skill in normalized_list:
            total_skill_demand_counts[skill] = (
                total_skill_demand_counts.get(skill, 0) + 1
            )
        # Track unmapped demand separately so it can be saved to unmapped_skills.json.
        for skill in not_found_list:
            unmapped_demand_counts[skill] = unmapped_demand_counts.get(skill, 0) + 1

    # Assign the collected column values back to the dataframe in one vectorised step.
    df[NORMALIZED_COL] = normalized_col_values
    df[NOT_FOUND_COL] = not_found_col_values

    LOGGER.info(
        "Finished normalization. Found %d unique unmapped skills",
        len(all_unmapped),
    )
    return (
        df,
        all_unmapped,
        total_skill_demand_counts,
        unmapped_demand_counts,
    )


def derive_demand_years(df: pd.DataFrame) -> pd.Series:
    """
    Derive a demand year for each row.

    Uses `Requirement Start Date` as the only source. If it is missing or
    unparseable, the year is left as NaN. No fallback is applied.
    """
    primary = pd.to_datetime(
        df.get("Requirement Start Date"), errors="coerce"
    )
    return primary.dt.year


def compute_yearly_skill_counts(
    df: pd.DataFrame,
    years: pd.Series,
    column_name: str,
) -> Dict[int, Dict[str, int]]:
    """
    Compute yearly demand counts for each skill based on a delimited skill column.

    Only rows with a non-null year are considered.

    Important indexing note: ``df[column_name].items()`` yields ``(df_index, value)``
    pairs where ``df_index`` is the *DataFrame* row label (which may differ from the
    positional index after filtering/resets).  ``years`` is passed as a positional-
    aligned Series (reset_index was applied before calling this function), so we use
    ``years.iloc[idx]`` with the *positional* index derived from the iteration order
    rather than the label — this relies on the caller ensuring that ``df`` and
    ``years`` are positionally aligned after any resets.
    """
    yearly_skill_counts: Dict[int, Dict[str, int]] = {}

    for idx, skills_str in df[column_name].items():
        # ``idx`` here is the DataFrame row label; since both df and years have been
        # reset_index(drop=True) before this call, label == position, so iloc is safe.
        year_val = years.iloc[idx]
        if pd.isna(year_val):
            # Rows without a parseable date cannot be assigned to a year; skip them.
            continue
        year_int = int(year_val)
        # setdefault avoids a separate key-existence check for each new year.
        per_year = yearly_skill_counts.setdefault(year_int, {})

        # Reuse the normalization splitter to get leaf skills; these will
        # correspond to the normalized/grouped entries used for counting earlier.
        leaf_skills = split_leaf_skills(skills_str)
        # Deduplicate within the row: each unique skill contributes at most 1 demand
        # unit per row per year, consistent with process_dataframe.
        unique_skills = set(leaf_skills)
        for skill in unique_skills:
            per_year[skill] = per_year.get(skill, 0) + 1

    return yearly_skill_counts


def compute_skill_cvs(
    yearly_skill_counts: Dict[int, Dict[str, int]],
    skills: Set[str],
    years_of_interest: Iterable[int],
) -> Dict[str, Optional[float]]:
    """
    Compute coefficient of variation (CV) for each skill over the given years.

    CV formula (population standard deviation / mean):
        CV = std(demands) / mean(demands)
    where std uses the population (N) denominator — appropriate here because the
    given years are the entire period of interest, not a sample from a larger set.

    A CV of None is returned for skills whose average demand is zero (to avoid
    division-by-zero), which callers treat as effectively excluded.
    """
    years_sorted = sorted(years_of_interest)
    cvs: Dict[str, Optional[float]] = {}

    for skill in skills:
        # Gather the demand in each year of interest; missing years count as zero.
        demands = [yearly_skill_counts.get(y, {}).get(skill, 0) for y in years_sorted]
        mean = sum(demands) / len(demands)
        if mean == 0:
            # Cannot compute a meaningful CV when there is no demand at all.
            cvs[skill] = None
            continue
        # Population variance: sum of squared deviations divided by N (not N-1).
        variance = sum((d - mean) ** 2 for d in demands) / len(demands)
        std = variance ** 0.5
        # CV = std / mean.  Lower values indicate more stable demand over time.
        cvs[skill] = std / mean

    return cvs


def analyze_skill_growth(
    yearly_skill_counts: Dict[int, Dict[str, int]],
    years_of_interest: Iterable[int],
    output_json: Path,
    output_plot: Path,
    top_n: int = 50,
) -> None:
    """
    Analyze skills with unusually large rank changes across the given years.

    - Builds per-year ranks for each skill (1 = highest demand).
    - Computes rank_change = rank_start - rank_end, where start is the earliest
      year in years_of_interest and end is the latest.
      Positive rank_change means the skill moved *up* (lower rank number = higher
      position), i.e. it is rising; negative means it is declining.
    - Selects top_n skills by absolute rank_change so both rising and declining
      skills are surfaced.
    - Writes their per-year demands and ranks to a JSON file.
    - Generates a simple line plot of demand over years for these skills.
    """
    years_sorted = sorted(years_of_interest)
    if len(years_sorted) < 2:
        # Growth analysis requires at least a start and an end year to compare.
        LOGGER.info(
            "Not enough years for growth analysis (need at least 2, got %d)",
            len(years_sorted),
        )
        return

    # ----------------------------------------------------------------
    # Phase 1: Collect the union of all skills seen across all years.
    # Skills absent in a particular year will be assigned a default rank.
    # ----------------------------------------------------------------
    all_skills: Set[str] = set()
    for year in years_sorted:
        all_skills.update(yearly_skill_counts.get(year, {}).keys())

    if not all_skills:
        LOGGER.info("No skills found for growth analysis in years %s", years_sorted)
        return

    # ----------------------------------------------------------------
    # Phase 2: Build per-year rank tables.
    # Ranking is demand-descending (highest demand = rank 1).
    # Skills absent in a year receive rank = (number of present skills) + 1,
    # treating absence as being at the bottom of the ranking for that year.
    # ----------------------------------------------------------------
    year_ranks: Dict[int, Dict[str, int]] = {}
    for year in years_sorted:
        counts = yearly_skill_counts.get(year, {})
        # Sort skills present that year by demand desc, then name.
        present_skills = sorted(
            counts.items(), key=lambda kv: (-kv[1], kv[0].lower())
        )
        ranks: Dict[str, int] = {}
        for idx, (skill, _) in enumerate(present_skills, start=1):
            ranks[skill] = idx
        # Skills absent that year get a rank of len(present_skills) + 1.
        default_rank = len(present_skills) + 1
        for skill in all_skills:
            ranks.setdefault(skill, default_rank)
        year_ranks[year] = ranks

    # ----------------------------------------------------------------
    # Phase 3: Compute per-skill rank changes and demand trend labels.
    # rank_change > 0 -> skill improved (moved up) = rising
    # rank_change < 0 -> skill worsened (moved down) = declining
    # rank_change == 0 -> no movement = stable
    # ----------------------------------------------------------------
    start_year = years_sorted[0]
    end_year = years_sorted[-1]
    skill_summaries = []
    for skill in all_skills:
        demands_by_year: Dict[int, int] = {}
        ranks_by_year: Dict[int, int] = {}
        for year in years_sorted:
            demands_by_year[year] = yearly_skill_counts.get(year, {}).get(skill, 0)
            ranks_by_year[year] = year_ranks[year][skill]
        # A positive rank_change means rank number decreased (better position).
        rank_change = ranks_by_year[start_year] - ranks_by_year[end_year]
        abs_change = abs(rank_change)

        start_demand = demands_by_year[start_year]
        end_demand = demands_by_year[end_year]
        if start_demand > 0:
            # Percentage change relative to the first year's demand.
            demand_pct_change = (end_demand - start_demand) / start_demand * 100.0
        else:
            # Cannot express a percentage when base demand is zero.
            demand_pct_change = None

        # Assign a human-readable trend label based on rank direction.
        if rank_change > 0:
            trend = "rising"
        elif rank_change < 0:
            trend = "declining"
        else:
            trend = "stable"

        skill_summaries.append(
            {
                "skill": skill,
                "demands": {str(y): demands_by_year[y] for y in years_sorted},
                "ranks": {str(y): ranks_by_year[y] for y in years_sorted},
                "rank_change_start_to_end": rank_change,
                "abs_rank_change": abs_change,
                "demand_pct_change_start_to_end": demand_pct_change,
                "trend": trend,
            }
        )

    # ----------------------------------------------------------------
    # Phase 4: Select the top_n skills by absolute rank change and write outputs.
    # Sorting by abs_rank_change descending ensures both the most dramatically
    # rising and most dramatically declining skills are surfaced.
    # ----------------------------------------------------------------
    skill_summaries.sort(
        key=lambda s: (-s["abs_rank_change"], s["skill"].lower())
    )
    top_skills = skill_summaries[:top_n]

    # Write JSON output.
    growth_payload = {
        "years": years_sorted,
        "skills": top_skills,
    }
    LOGGER.info(
        "Writing growth analysis for %d skills to %s",
        len(top_skills),
        output_json,
    )
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(growth_payload, f, ensure_ascii=False, indent=2)

    # Generate a simple line plot of demand over years for these skills.
    LOGGER.info(
        "Creating growth analysis plot for %d skills at %s",
        len(top_skills),
        output_plot,
    )
    plt.figure(figsize=(12, 8))
    for summary in top_skills:
        demands = [summary["demands"][str(y)] for y in years_sorted]
        plt.plot(
            years_sorted,
            demands,
            marker="o",
            label=summary["skill"],
        )

    plt.xlabel("Year")
    plt.ylabel("Demand (rows with skill)")
    plt.title("Skills with Unusual Growth / Decline (by Rank Change)")
    plt.legend(loc="best", fontsize="small")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_plot, dpi=200)
    plt.close()


def segment_skills_by_cv(
    yearly_skill_counts: Dict[int, Dict[str, int]],
    skills: Set[str],
    years_of_interest: Iterable[int],
    output_dir: Path,
) -> None:
    """
    Segment skills into X/Y/Z groups based on coefficient of variation (CV).

    XYZ segmentation classifies skills by the stability of their demand:
    - X (Stable): CV < 0.5   — demand is highly predictable; easy to forecast.
    - Y (Variable): 0.5 <= CV <= 1.0 — moderate variability; requires careful planning.
    - Z (Sporadic): CV > 1.0 — highly erratic demand; difficult to forecast reliably.

    Skills with zero average demand across the years of interest are skipped
    (they would produce an undefined CV and are not meaningful to segment).

    Outputs three JSON files under ``output_dir`` (one per segment), sorted
    alphabetically by skill name for readability.
    """
    years_sorted = sorted(years_of_interest)
    x_skills: Dict[str, Dict[str, object]] = {}
    y_skills: Dict[str, Dict[str, object]] = {}
    z_skills: Dict[str, Dict[str, object]] = {}

    for skill in skills:
        # Collect demand for each year; missing years count as zero.
        demands = [yearly_skill_counts.get(y, {}).get(skill, 0) for y in years_sorted]
        mean = sum(demands) / len(demands)
        if mean == 0:
            # Skills with no demand in any of the given years are not segmented.
            continue
        # Population CV: std / mean using the N denominator (same as compute_skill_cvs).
        variance = sum((d - mean) ** 2 for d in demands) / len(demands)
        std = variance ** 0.5
        cv = std / mean

        payload = {
            "cv": round(cv, 6),
            "demands": {str(y): yearly_skill_counts.get(y, {}).get(skill, 0) for y in years_sorted},
        }

        # Assign to segment based on CV thresholds defined in the module docstring.
        if cv < 0.5:
            x_skills[skill] = payload
        elif cv <= 1.0:
            y_skills[skill] = payload
        else:
            z_skills[skill] = payload

    def write_segment(segment: Dict[str, Dict[str, object]], filename: str) -> None:
        """Sort segment alphabetically and write to JSON so downstream consumers get stable ordering."""
        output_path = output_dir / filename
        ordered = dict(sorted(segment.items(), key=lambda kv: kv[0].lower()))
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(ordered, f, ensure_ascii=False, indent=2)

    write_segment(x_skills, "xyz_x_skills.json")
    write_segment(y_skills, "xyz_y_skills.json")
    write_segment(z_skills, "xyz_z_skills.json")


def save_demand_counts(counts: Dict[str, int], output_path: Path, label: str) -> None:
    """
    Save demand counts as a JSON file sorted from highest to lowest demand.
    The JSON will be an object {skill: demand_count, ...} with keys ordered
    by decreasing demand.
    """
    LOGGER.info(
        "Writing %d %s skills with demand counts to %s",
        len(counts),
        label,
        output_path,
    )
    # Sort by demand count descending, then by skill name for stability
    sorted_items = sorted(
        counts.items(), key=lambda kv: (-kv[1], kv[0].lower())
    )
    ordered_counts = {skill: demand for skill, demand in sorted_items}
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(ordered_counts, f, ensure_ascii=False, indent=2)


def build_cooccurrence_data(
    df: pd.DataFrame,
    skill_groups_col: str = "Skill Groups",
) -> Dict[str, int]:
    """
    Build exact demand counts for all unique skill combinations (same logic as skill_clustering).
    Each row counts as one demand unit; combination key is sorted, comma-separated skills.
    """
    LOGGER.info("Building exact co-occurrence data (true combination demand)...")
    combination_counts: Dict[str, int] = {}
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Counting combinations"):
        skills = sorted(set(split_skills_cell(row[skill_groups_col])))
        if not skills:
            continue
        combo_key = ", ".join(skills)
        combination_counts[combo_key] = combination_counts.get(combo_key, 0) + 1
    LOGGER.info(
        "Found %d unique skill combinations (true demand stacks)",
        len(combination_counts),
    )
    return combination_counts


def save_cooccurrence_json(
    combination_counts: Dict[str, int],
    output_path: Path,
) -> None:
    """
    Save co-occurrence data sorted by demand descending; format matches skill_clustering.
    Each entry: {"skills": "A, B", "skill_count": 2, "true_demand": N}.
    """
    sorted_combos = sorted(
        combination_counts.items(), key=lambda x: x[1], reverse=True
    )
    payload = []
    for combo, demand in sorted_combos:
        skills = [s.strip() for s in combo.split(",")]
        payload.append(
            {
                "skills": combo,
                "skill_count": len(skills),
                "true_demand": int(demand),
            }
        )
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    LOGGER.info(
        "Saved %d skill combinations (true demand) to %s",
        len(payload),
        output_path,
    )


def compute_and_save_single_large_occ(
    df: pd.DataFrame,
    skill_groups_col: str,
    output_path: Path,
) -> None:
    """
    Count rows where Skill Groups contains exactly one (high-demand) skill and
    save as single_large_occ.json in the same list format as co_occurrence.json.
    """
    single_counts: Dict[str, int] = {}
    for val in df[skill_groups_col].astype(str):
        if not val or not val.strip():
            continue
        skills = split_leaf_skills(val)
        if len(skills) != 1:
            continue
        skill = skills[0].strip()
        if skill:
            single_counts[skill] = single_counts.get(skill, 0) + 1
    sorted_items = sorted(
        single_counts.items(), key=lambda kv: (-kv[1], kv[0].lower())
    )
    payload = [
        {"skills": skill, "skill_count": 1, "true_demand": int(demand)}
        for skill, demand in sorted_items
    ]
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    LOGGER.info(
        "Wrote %d single-skill (high-demand alone) entries to %s",
        len(payload),
        output_path,
    )


def _run_normalization_for_market(
    df_market: pd.DataFrame,
    years_market: pd.Series,
    skills_dir: Path,
    output_csv_path: Path,
    variant_to_normalized: Dict[str, str],
    eliminate_skills: Set[str],
    market_name: str,
    min_total_demand_for_groups: int,
) -> int:
    """
    Run the full normalization pipeline for a single market; all outputs go under skills_dir.

    This function implements steps 2–11 from the module docstring for one market slice.
    It is called once per market entry in MARKETS.  All intermediate and final JSON/CSV
    outputs are written to ``skills_dir`` and the path given by ``output_csv_path``.

    Returns the final row count for this market after all filters.
    """
    initial_demand = len(df_market)

    # ------------------------------------------------------------------
    # Step 2a: Compute total_skills.json using only the configured years.
    # total_skills.json is meant to reflect stable aggregate demand across
    # the 2023-2025 window even if future input data contains later years.
    # ------------------------------------------------------------------
    if TOTAL_SKILLS_YEARS:
        mask_total = years_market.isin(TOTAL_SKILLS_YEARS)
        df_total_for_aggregate = df_market[mask_total].reset_index(drop=True)
        LOGGER.info(
            "Market '%s': using %d rows (years %s) to build total_skills.json",
            market_name,
            len(df_total_for_aggregate),
            sorted(TOTAL_SKILLS_YEARS),
        )
    else:
        # No year restriction configured — use the full market slice.
        df_total_for_aggregate = df_market

    (
        _df_total,
        _unmapped_total,
        total_skill_demand_counts,
        _unmapped_demand_counts_total,
    ) = process_dataframe(
        df_total_for_aggregate, variant_to_normalized, eliminate_skills
    )
    save_demand_counts(
        total_skill_demand_counts,
        skills_dir / "total_skills.json",
        label="total (limited to configured years for aggregate)",
    )

    # ------------------------------------------------------------------
    # Step 3: Year exclusion — drop all rows whose demand year < MIN_YEAR.
    # Work on a copy so the caller's slice remains unmodified.
    # ------------------------------------------------------------------
    df = df_market.copy()
    years = years_market.copy()
    mask_keep = years >= MIN_YEAR
    dropped = (~mask_keep).sum()
    if dropped:
        LOGGER.info(
            "Market '%s': dropping %d rows with demand year < %d",
            market_name,
            dropped,
            MIN_YEAR,
        )
    df = df[mask_keep].reset_index(drop=True)
    years = years[mask_keep].reset_index(drop=True)

    demand_after_exclude_year = len(df)
    LOGGER.info(
        "Market '%s': demand after dropping years < %d: %d",
        market_name,
        MIN_YEAR,
        demand_after_exclude_year,
    )

    # ------------------------------------------------------------------
    # Step 4: Re-run normalization on the year-filtered dataset.
    # This ensures Skills Normalized and Not found reflect only the rows
    # that will be used for all downstream demand analysis.
    # ------------------------------------------------------------------
    (
        df_processed,
        unmapped,
        total_skill_demand_counts_filtered,
        unmapped_demand_counts,
    ) = process_dataframe(df, variant_to_normalized, eliminate_skills)

    # ------------------------------------------------------------------
    # Step 5: Compute year-wise demand on Skills Normalized (pre-filter).
    # Used for low-demand detection and CV computation below.
    # ------------------------------------------------------------------
    yearly_counts_normalized = compute_yearly_skill_counts(
        df_processed, years, NORMALIZED_COL
    )

    # ------------------------------------------------------------------
    # Step 6a: Build the initial groupable_skills set using demand thresholds.
    # Dynamic threshold: always include the top MIN_HIGH_DEMAND_TOP_N skills
    # (provided demand >= MIN_HIGH_DEMAND_TOP_N_FLOOR), plus all skills that
    # clear the absolute min_total_demand_for_groups threshold.
    # ------------------------------------------------------------------
    LOGGER.info(
        "Market '%s': building Skill Groups with min total demand threshold %d",
        market_name,
        min_total_demand_for_groups,
    )
    sorted_by_demand = sorted(
        total_skill_demand_counts_filtered.items(), key=lambda kv: -kv[1]
    )
    # Top-N override: guarantee at least MIN_HIGH_DEMAND_TOP_N skills are included
    # as long as they meet the floor, so high-demand markets don't silently drop skills.
    top_n_skills: Set[str] = {
        skill
        for i, (skill, cnt) in enumerate(sorted_by_demand)
        if i < MIN_HIGH_DEMAND_TOP_N and cnt >= MIN_HIGH_DEMAND_TOP_N_FLOOR
    }
    groupable_skills: Set[str] = {
        skill
        for skill, cnt in total_skill_demand_counts_filtered.items()
        if cnt >= min_total_demand_for_groups or skill in top_n_skills
    }

    # ------------------------------------------------------------------
    # Step 6b: Identify and remove low-demand skills from groupable_skills.
    # A skill is "low demand" if its peak year count is below the adaptive
    # threshold max(LOW_DEMAND_ABS_MIN, peak_count // 3).
    # ------------------------------------------------------------------
    low_demand_skills: Dict[str, int] = {}
    for skill, cnt in total_skill_demand_counts_filtered.items():
        per_year_counts = [
            yearly_counts_normalized.get(year, {}).get(skill, 0)
            for year in sorted(yearly_counts_normalized.keys())
        ]
        if sum(per_year_counts) == 0:
            # Skill has no demand in any year — not classifiable as low-demand.
            continue
        max_year_count = max(per_year_counts)
        # The threshold scales with the skill's own peak so that high-volume skills
        # need higher absolute consistency to avoid being flagged.
        threshold = max(LOW_DEMAND_ABS_MIN, max_year_count // 3)
        if 0 < max_year_count < threshold:
            low_demand_skills[skill] = cnt

    save_demand_counts(
        low_demand_skills,
        skills_dir / "low_demand_skills.json",
        label="low-demand",
    )
    # Remove low-demand skills from the high-demand candidate set.
    for skill in low_demand_skills:
        groupable_skills.discard(skill)

    # ------------------------------------------------------------------
    # Step 6c: Recency gate — require meaningful 2025 demand.
    # Skills that were historically popular but have faded are excluded so
    # Skill Groups stay relevant to current hiring needs.
    # A skill must clear BOTH an absolute floor AND a fraction of its own total.
    # ------------------------------------------------------------------
    skills_2025 = yearly_counts_normalized.get(2025, {})
    insufficient_2025: Set[str] = set()
    for skill in groupable_skills:
        demand_2025 = skills_2025.get(skill, 0)
        total_demand = total_skill_demand_counts_filtered.get(skill, 0)
        min_required = max(MIN_2025_DEMAND_ABS, int(total_demand * MIN_2025_DEMAND_FRACTION))
        if demand_2025 < min_required:
            insufficient_2025.add(skill)
    if insufficient_2025:
        LOGGER.info(
            "Market '%s': recency gate removed %d skills with insufficient 2025 demand: %s",
            market_name,
            len(insufficient_2025),
            sorted(insufficient_2025),
        )
        for skill in insufficient_2025:
            groupable_skills.discard(skill)

    # ------------------------------------------------------------------
    # Step 10 (early): XYZ segmentation using the current groupable_skills.
    # Segmented before CV exclusion so that the XYZ files reflect all
    # high-demand candidates, not just CV-eligible ones.
    # ------------------------------------------------------------------
    segment_skills_by_cv(
        yearly_counts_normalized,
        skills=groupable_skills,
        years_of_interest=CV_YEARS,
        output_dir=skills_dir,
    )

    # ------------------------------------------------------------------
    # Step 6d: CV filtering — remove skills with too-variable demand.
    # Skills with CV >= MAX_CV_FOR_GROUPS are unpredictable enough that
    # including them in Skill Groups would distort clustering.
    # ------------------------------------------------------------------
    cv_by_skill = compute_skill_cvs(
        yearly_counts_normalized, groupable_skills, CV_YEARS
    )
    cv_excluded: Dict[str, Dict[str, object]] = {}
    cv_eligible: Set[str] = set()
    for skill, cv in cv_by_skill.items():
        if cv is None or cv >= MAX_CV_FOR_GROUPS:
            # Record the reason for exclusion (cv value and demands) for auditability.
            cv_excluded[skill] = {
                "cv": None if cv is None else round(cv, 6),
                "total_demand": total_skill_demand_counts_filtered.get(skill, 0),
                "demands": {
                    str(y): yearly_counts_normalized.get(y, {}).get(skill, 0)
                    for y in CV_YEARS
                },
            }
        else:
            cv_eligible.add(skill)

    if cv_excluded:
        cv_excluded_path = skills_dir / "cv_excluded_skills.json"
        LOGGER.info(
            "Market '%s': writing %d CV-excluded skills to %s",
            market_name,
            len(cv_excluded),
            cv_excluded_path,
        )
        ordered = dict(sorted(cv_excluded.items(), key=lambda kv: kv[0].lower()))
        with cv_excluded_path.open("w", encoding="utf-8") as f:
            json.dump(ordered, f, ensure_ascii=False, indent=2)

    # The final groupable_skills set after all filtering passes is cv_eligible.
    groupable_skills = cv_eligible

    # ------------------------------------------------------------------
    # Step 6e: Build the "Skill Groups" column per row.
    # Each row keeps only those of its Skills Normalized that are in
    # groupable_skills and are not low-demand; the result is sorted and joined.
    # ------------------------------------------------------------------
    def build_skill_groups_cell(skills_normalized: str) -> str:
        """Filter a normalized skills string down to high-demand, non-low-demand skills."""
        if not isinstance(skills_normalized, str) or not skills_normalized.strip():
            return ""
        raw_skills = split_leaf_skills(skills_normalized)
        filtered = {
            s
            for s in raw_skills
            if s and s in groupable_skills and s not in low_demand_skills
        }
        if not filtered:
            return ""
        return ", ".join(sorted(filtered))

    LOGGER.info("Market '%s': creating 'Skill Groups' column", market_name)
    df_processed["Skill Groups"] = df_processed.get(NORMALIZED_COL, "").apply(
        build_skill_groups_cell
    )

    # ------------------------------------------------------------------
    # Step 7a: Drop rows with empty Skill Groups.
    # These are rows whose only skills are low-demand or CV-excluded.
    # Both df_processed and years must be filtered together so their
    # positional alignment is preserved for compute_yearly_skill_counts.
    # ------------------------------------------------------------------
    before_drop_groups = len(df_processed)
    mask_nonempty_groups = (
        df_processed["Skill Groups"].astype(str).str.strip().astype(bool)
    )
    df_processed = df_processed[mask_nonempty_groups]
    years = years[mask_nonempty_groups]
    df_processed = df_processed.reset_index(drop=True)
    years = years.reset_index(drop=True)
    after_drop_groups = len(df_processed)
    LOGGER.info(
        "Market '%s': dropped %d rows with empty Skill Groups",
        market_name,
        before_drop_groups - after_drop_groups,
    )

    # ------------------------------------------------------------------
    # Step 7b: Drop rows with empty Skills Normalized.
    # A row that has no parseable/normalized skills at all provides no signal.
    # ------------------------------------------------------------------
    before_drop_normalized = len(df_processed)
    mask_nonempty_normalized = (
        df_processed[NORMALIZED_COL].astype(str).str.strip().astype(bool)
    )
    df_processed = df_processed[mask_nonempty_normalized]
    years = years[mask_nonempty_normalized]
    df_processed = df_processed.reset_index(drop=True)
    years = years.reset_index(drop=True)
    after_drop_normalized = len(df_processed)
    LOGGER.info(
        "Market '%s': dropped %d rows with empty Skills Normalized",
        market_name,
        before_drop_normalized - after_drop_normalized,
    )

    # ------------------------------------------------------------------
    # Step 8: Recompute year-wise demand from the final filtered dataset.
    # These counts are the authoritative per-year demand figures used for
    # all subsequent JSON writes and growth analysis.
    # ------------------------------------------------------------------
    yearly_skill_counts_filtered = compute_yearly_skill_counts(
        df_processed, years, NORMALIZED_COL
    )

    # Write the final normalized CSV for this market.
    LOGGER.info(
        "Market '%s': writing normalized CSV to %s",
        market_name,
        output_csv_path,
    )
    df_processed.to_csv(output_csv_path, index=False)

    # Save unmapped (not-found) skill demand for quality-assurance review.
    save_demand_counts(
        unmapped_demand_counts,
        skills_dir / "unmapped_skills.json",
        label="unmapped",
    )

    # Save high-demand skill totals (total filtered demand, not year-wise).
    high_demand_skills = {
        skill: total_skill_demand_counts_filtered.get(skill, 0)
        for skill in groupable_skills
    }
    save_demand_counts(
        high_demand_skills,
        skills_dir / "high_demand_skills.json",
        label="high-demand",
    )

    # Save single-skill occurrence counts (rows where Skill Groups has exactly 1 skill).
    compute_and_save_single_large_occ(
        df_processed,
        "Skill Groups",
        skills_dir / "single_large_occ.json",
    )

    # Build and save the full co-occurrence table (all skill-combination demand counts).
    combo_counts = build_cooccurrence_data(df_processed, skill_groups_col="Skill Groups")
    save_cooccurrence_json(combo_counts, skills_dir / "co_occurrence.json")

    # Write per-year demand JSONs using the final filtered skill counts.
    for year, counts in sorted(yearly_skill_counts_filtered.items()):
        save_demand_counts(
            counts,
            skills_dir / f"total_skills_{year}.json",
            label=f"total for {year}",
        )

    # ------------------------------------------------------------------
    # Step 9: Growth analysis — rank changes across 2023–2025.
    # ------------------------------------------------------------------
    analyze_skill_growth(
        yearly_skill_counts_filtered,
        years_of_interest=[2023, 2024, 2025],
        output_json=skills_dir / "skill_growth_analysis.json",
        output_plot=skills_dir / "skill_growth_analysis.png",
        top_n=50,
    )

    # Summary log so row-count changes are easily traceable in the run log.
    final_row_count = len(df_processed)
    LOGGER.info(
        "Market '%s': total demand at start: %d, after dropping years < %d: %d, final rows: %d",
        market_name,
        initial_demand,
        MIN_YEAR,
        demand_after_exclude_year,
        final_row_count,
    )
    LOGGER.info(
        "Market '%s': total skills (filtered): %d, Unmapped skills: %d",
        market_name,
        len(total_skill_demand_counts_filtered),
        len(unmapped_demand_counts),
    )
    return final_row_count


def main() -> None:
    """
    Entry point: load mapping, process CSV per market, and write outputs under skills/<Market>/.

    Orchestrates the 11-step pipeline described in the module docstring.
    Steps 2–11 are executed per-market inside ``_run_normalization_for_market``.
    Steps handled directly here: argument parsing, path resolution, initial CSV
    load, initial cleanup (Step 1), year derivation (Step 3 setup), and market
    segmentation before delegating to the per-market function.
    """
    # ----------------------------------------------------------------
    # Argument parsing: allow callers to override the practice area,
    # year range, and skills to exclude without touching source code.
    # ----------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Normalize skills and produce demand/co-occurrence outputs per market.")
    parser.add_argument(
        "--exclude-skills",
        type=str,
        default="",
        help="Comma-separated skills to exclude from normalization output (e.g. 'API Development'). Merged with eliminate_skills.json if present.",
    )
    parser.add_argument(
        "--practice-area",
        type=str,
        default="DE",
        metavar="PA",
        help=(
            "Practice Area abbreviation (e.g. DE, EPS, ADM). Controls which data/{PA}/ "
            "sub-folder is read and which skills/{Market}_{PA}/ folders are written. Default: DE"
        ),
    )
    parser.add_argument(
        "--year-range",
        type=str,
        default="2023-2025",
        metavar="YYYY-YYYY",
        help="Year range used in the input/output filenames (e.g. 2023-2025). Default: 2023-2025",
    )
    args = parser.parse_args()

    # ----------------------------------------------------------------
    # Resolve dynamic paths from --practice-area / --year-range so the
    # script can be run against different PA/year datasets without editing
    # any hardcoded constants.
    # ----------------------------------------------------------------
    pa = args.practice_area.strip()
    yr = args.year_range.strip()
    input_csv_path  = DATA_DIR / pa / f"DFC_YTD_{yr}_{pa}_V2_corrected.csv"
    # Output CSV path template (market name appended in the loop below)
    output_csv_stem = DATA_DIR / pa / f"DFC_YTD_{yr}_{pa}_V2_corrected_normalized"

    # ----------------------------------------------------------------
    # Load the eliminate-skills list first so it can be merged with any
    # CLI-provided exclusions before normalization begins.
    # ----------------------------------------------------------------
    eliminate_skills = load_eliminate_skills(ELIMINATE_SKILLS_JSON_PATH)
    if args.exclude_skills:
        # Merge CLI-provided exclusions into the eliminate set.
        extra = {s.strip() for s in args.exclude_skills.split(",") if s.strip()}
        eliminate_skills = eliminate_skills | extra
        LOGGER.info("Exclude list from args: %d skills; total to eliminate: %d", len(extra), len(eliminate_skills))
    LOGGER.info("Starting skill normalization pipeline (per market) | PA=%s | yr=%s", pa, yr)

    # ----------------------------------------------------------------
    # Step 1 (setup): Load the normalization mapping JSON before reading
    # the input CSV so that any missing-file errors surface early.
    # ----------------------------------------------------------------
    variant_to_normalized = load_normalization_mapping(NORMALIZATION_JSON_PATH)

    if not input_csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found at: {input_csv_path}")

    # Step 1: Load and initial cleanup of the input CSV.
    LOGGER.info("Reading input CSV: %s", input_csv_path)
    df_all = pd.read_csv(input_csv_path)

    if MARKET_COL not in df_all.columns:
        raise ValueError(
            "Input CSV must contain column '%s' for market segmentation.",
            MARKET_COL,
        )

    # Drop the legacy "Skill Cluster" column immediately; this script is the
    # sole authority for skill grouping and does not inherit prior clustering.
    if "Skill Cluster" in df_all.columns:
        LOGGER.info("Dropping 'Skill Cluster' column at initial load")
        df_all = df_all.drop(columns=["Skill Cluster"])

    initial_demand = len(df_all)
    LOGGER.info("Initial total demand (rows): %d", initial_demand)

    # Step 3 (setup): Derive demand years for all rows up-front; the per-market
    # function uses these to apply year exclusion and year-wise aggregations.
    years_all = derive_demand_years(df_all)

    # ----------------------------------------------------------------
    # Market loop: split the dataset by market and run the full pipeline
    # for each market independently.  This keeps all outputs namespaced
    # so Americas and EMEA never overwrite each other's files.
    # ----------------------------------------------------------------
    market_norm = df_all[MARKET_COL].astype(str).str.strip().str.casefold()
    row_counts_by_market: Dict[str, int] = {}
    for market in MARKETS:
        mkey = market.casefold()
        mask = market_norm == mkey
        df_market = df_all[mask].reset_index(drop=True)
        years_market = years_all[mask].reset_index(drop=True)
        if len(df_market) == 0:
            LOGGER.warning("No rows for market '%s', skipping", market)
            continue

        # Create the per-market output directory if it does not already exist.
        skills_dir = SKILLS_DIR / f"{market}_{pa}"
        skills_dir.mkdir(parents=True, exist_ok=True)
        # e.g. data/DE/DFC_YTD_2023-2025_DE_V2_corrected_normalized_Americas.csv
        output_csv_path = Path(str(output_csv_stem) + f"_{market}.csv")
        # Use the market-specific minimum demand threshold if one is configured.
        min_demand = MIN_TOTAL_DEMAND_FOR_GROUPS_BY_MARKET.get(
            market, MIN_TOTAL_DEMAND_FOR_GROUPS
        )
        # Delegate Steps 2–11 to the per-market pipeline function.
        final_count = _run_normalization_for_market(
            df_market=df_market,
            years_market=years_market,
            skills_dir=skills_dir,
            output_csv_path=output_csv_path,
            variant_to_normalized=variant_to_normalized,
            eliminate_skills=eliminate_skills,
            market_name=market,
            min_total_demand_for_groups=min_demand,
        )
        row_counts_by_market[market] = final_count

    # ----------------------------------------------------------------
    # Final summary: log aggregate and per-market row counts so it is easy
    # to confirm all markets were processed and nothing was silently dropped.
    # ----------------------------------------------------------------
    total_final_rows = sum(row_counts_by_market.values())
    LOGGER.info("Skill normalization completed (all markets processed)")
    LOGGER.info("Final row counts: total=%d", total_final_rows)
    for market, count in sorted(row_counts_by_market.items()):
        LOGGER.info("  %s: %d", market, count)


if __name__ == "__main__":
    main()

