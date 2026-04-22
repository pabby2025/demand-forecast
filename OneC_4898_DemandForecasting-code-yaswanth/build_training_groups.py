"""
Build Training Groups Dataset
==============================

PURPOSE
-------
This script builds the training dataset by creating rolling cutoffs through historical data
and engineering features for count forecasting.  It is intentionally decoupled from the
model training step so that the expensive dataset-building pass can be cached and reused
across multiple training runs.

GROUPING DIMENSIONS
-------------------
The script can group demand records by any subset of the following columns:
  - Skill Cluster          (S)
  - Country + SO GRADE + Skill Cluster  (RLC)
  - BU + Skill Cluster     (BS)
  - BU + SO GRADE + Country + Skill Cluster  (BRLC)

The canonical suffix notation (S / RLC / BS / BRLC) is defined in _CANONICAL_GROUP_SUFFIX
and must stay in sync with run_pipeline.sh GROUP_INITIALS and data_split.py.

FEATURE ENGINEERING
-------------------
For each (group, cutoff_date, months_ahead) combination the following feature families
are computed:
  - Lag / window counts      : events in the last 30 d before cutoff
  - SMA / WMA baselines      : simple and exponentially-weighted moving averages (3 m, 6 m, 12 m)
  - Demand growth rates      : 3 m, 6 m, 9 m YoY percentage changes (capped at ±300 %)
  - Trend slope              : normalised OLS slope over last 6 complete months
  - Hierarchy aggregates     : country-level seasonality, (country × skill) seasonality strength
  - Growth trajectory        : CAGR-based classification (Fast Growing → Fast Declining)
  - Bridge features          : M0–M2 predictions fed as features for M3+ horizon predictions
  - FTE / UPLF features      : forecast FTE from UPLF planning data (skipped with --no-uplf)
  - Calendar / horizon       : target month, fiscal year flags, days-to-target, YoY growth rate
  - Quarter growth           : country-level macro growth % from external CSV (optional)

INPUT FILES
-----------
  data_<SUFFIX>/train_data.csv          -- processed training demand records (from data_split.py)
  data_<SUFFIX>/test_data.csv           -- processed test demand records
  data_<SUFFIX>/Quarter_growth_by_country_uplf_train.csv  -- optional macro growth file

OUTPUT FILES
------------
  data_<SUFFIX>/training_dataset.parquet  -- main training dataset (parquet, fast I/O)
  data_<SUFFIX>/training_dataset.csv      -- same dataset in CSV for manual inspection
  data_<SUFFIX>/training_dataset_summary.json -- statistics (sample counts, target stats)
  data_<SUFFIX>/test_dataset.parquet      -- test dataset (single cutoff per group)
  data_<SUFFIX>/group_manifest.json       -- per-group file index (when --individual-groups)
"""

# ================================================================================================
# IMPORTS
# ================================================================================================

import polars as pl
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import warnings
import os
import logging
import coloredlogs
from tqdm import tqdm
import argparse
import json
import re
from scipy import stats

from data_split import resolve_results_dir

warnings.filterwarnings('ignore')

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s - %(message)s', isatty=True)

# ================================================================================================
# GROWTH TRAJECTORY CLASSIFICATION
# ================================================================================================

# Canonical suffix for the results folder.
# MUST stay in sync with GROUP_INITIALS in run_pipeline.sh and with data_split.py.
#
# Notation key:
#   S    -- Skill Cluster only (coarsest grouping; fewest groups, most data per group)
#   RLC  -- Role-Location-Cluster: SO GRADE + Country + Skill Cluster
#   BS   -- BU + Skill Cluster (business-unit + skill; medium granularity)
#   BRLC -- BU + RLC: the finest supported grouping (BU + SO GRADE + Country + Skill Cluster)
#
# When group_by_cols exactly matches one of these frozensets, the canonical short code is used
# for directory naming (e.g., results_RLC/data_RLC/).  Unknown combinations fall back to the
# initials of the column names joined together (see build_group_suffix).
_CANONICAL_GROUP_SUFFIX = {
    frozenset(['Skill Cluster']): 'S',
    frozenset(['Country', 'SO GRADE', 'Skill Cluster']): 'RLC',
    frozenset(['BU', 'Skill Cluster']): 'BS',
    frozenset(['BU', 'SO GRADE', 'Country', 'Skill Cluster']): 'BRLC',
}


def build_group_suffix(group_by_cols: list) -> str:
    """Return the canonical short suffix for a given set of group-by columns.

    Strategy (two-tier lookup):
      1. Canonical lookup: if the frozenset of column names exactly matches one of the
         entries in _CANONICAL_GROUP_SUFFIX, the pre-defined short code is returned
         (e.g., ['Country', 'SO GRADE', 'Skill Cluster'] -> 'RLC').
      2. Initials fallback: for unrecognised combinations, the first uppercase letter of
         each column name is concatenated (e.g., ['BU', 'City'] -> 'BC').  If the
         result is still empty the sentinel 'ALL' is returned.

    The returned suffix is used in directory names such as results_RLC/data_RLC/, so it
    must remain consistent with run_pipeline.sh and data_split.py.

    Args:
        group_by_cols: Ordered list of column names used for grouping.

    Returns:
        str: Short suffix string (e.g., 'S', 'RLC', 'BS', 'BRLC', or initials).
    """
    if not group_by_cols:
        return 'ALL'
    normalized = [col.strip() for col in group_by_cols if col and col.strip()]
    if not normalized:
        return 'ALL'
    key = frozenset(normalized)
    # Phase 1: exact canonical lookup
    suffix = _CANONICAL_GROUP_SUFFIX.get(key)
    if suffix is not None:
        return suffix
    # Phase 2: initials fallback for non-standard column combinations
    initials = ''.join(col.strip()[0].upper() for col in group_by_cols if col and col.strip())
    return initials or 'ALL'


def sanitize_filename_token(value) -> str:
    """Convert an arbitrary value to a filesystem-safe lowercase slug.

    Transformation steps:
      1. Cast to str and lower-case.
      2. Replace every run of characters that are NOT alphanumeric with a single '_'.
      3. Strip leading/trailing underscores.

    Edge cases:
      - None -> 'unknown'
      - All-special-character strings (e.g. '---') -> 'unknown' (after strip everything is gone)
      - Spaces, slashes, dots, parentheses are all collapsed to '_'.

    Example:
        sanitize_filename_token('United Kingdom')  -> 'united_kingdom'
        sanitize_filename_token('C# / .NET')       -> 'c_net'
        sanitize_filename_token(None)              -> 'unknown'

    Args:
        value: Any value that can be cast to str, or None.

    Returns:
        str: Lowercase alphanumeric slug suitable for use in file/directory names.
    """
    if value is None:
        return 'unknown'
    token = re.sub(r'[^a-z0-9]+', '_', str(value).lower()).strip('_')
    return token or 'unknown'


def _normalize_group_key(group_key) -> tuple:
    """Wrap a scalar group key in a tuple so all keys are uniformly tuple-typed.

    Polars' ``partition_by`` returns a single scalar (not a tuple) when there is
    only one grouping column.  This helper normalises both the scalar and the
    already-tuple cases so downstream code can always treat group keys as tuples.

    Args:
        group_key: A group key value — either a plain scalar (str, int, …) or a tuple.

    Returns:
        tuple: The key wrapped in a 1-tuple if it was a scalar, or unchanged if it
               was already a tuple.
    """
    if isinstance(group_key, tuple):
        return group_key
    return (group_key,)


def _partition_by_group(df: pl.DataFrame, group_by_cols: list) -> dict:
    """Partition a Polars DataFrame into per-group sub-DataFrames keyed by normalised tuples.

    Calls ``pl.DataFrame.partition_by`` and re-keys the result dict so every key is a
    tuple (via ``_normalize_group_key``), regardless of whether there is one or multiple
    grouping columns.

    Args:
        df: Source DataFrame that contains all ``group_by_cols`` columns.
        group_by_cols: Column names to partition by.

    Returns:
        dict: Mapping of ``tuple -> pl.DataFrame`` for each unique group combination.
    """
    partitions = df.partition_by(group_by_cols, as_dict=True)
    normalized = {}
    for key, part_df in partitions.items():
        normalized[_normalize_group_key(key)] = part_df
    return normalized


def _build_group_keys_df(group_by_cols: list, group_keys: list) -> pl.DataFrame:
    """Construct a Polars DataFrame from a list of group-key tuples for use in joins.

    The resulting DataFrame has one column per entry in ``group_by_cols`` and one row
    per key in ``group_keys``.  It is intended to be passed to ``pl.DataFrame.join``
    as the right-hand side to filter a larger DataFrame to only the desired groups.

    Args:
        group_by_cols: Column names matching the positional values in each key tuple.
        group_keys: List of tuples where each tuple element corresponds to the
                    column at the same position in ``group_by_cols``.

    Returns:
        pl.DataFrame | None: A narrow DataFrame with one row per group key,
                             or None if ``group_keys`` is empty.
    """
    if not group_keys:
        return None
    rows = [dict(zip(group_by_cols, key)) for key in group_keys]
    return pl.DataFrame(rows)


def _filter_df_by_group_keys(df: pl.DataFrame, group_by_cols: list, group_keys: list) -> pl.DataFrame:
    """Return only the rows of ``df`` whose group-key combination appears in ``group_keys``.

    Performs an inner join on ``group_by_cols`` between ``df`` and the DataFrame built
    from ``group_keys``, effectively subsetting ``df`` to the requested groups.

    Args:
        df: Source DataFrame to filter.
        group_by_cols: Column names that form the composite group key.
        group_keys: List of group-key tuples to keep.

    Returns:
        pl.DataFrame: Subset of ``df`` containing only rows belonging to the specified
                      groups.  Returns an empty DataFrame (zero rows, same schema) if
                      ``group_keys`` is empty or the keys DataFrame is None/empty.
    """
    if not group_keys:
        return df.head(0)
    keys_df = _build_group_keys_df(group_by_cols, group_keys)
    if keys_df is None or len(keys_df) == 0:
        return df.head(0)
    return df.join(keys_df, on=group_by_cols, how='inner')


def _compute_group_eligibility(test_df: pl.DataFrame, group_by_cols: list) -> tuple:
    """Compute per-group eligibility metrics from the test dataset.

    Returns every group present in the test data together with size/demand statistics.
    No minimum-threshold filtering is applied here; callers decide which groups are
    "eligible" based on the returned metrics.

    When ``target_count`` is available (the normal case), the primary sort key is
    average target count descending (highest demand groups first).  If the column is
    absent the function falls back to row count.

    Args:
        test_df: Test dataset Polars DataFrame, expected to contain ``group_by_cols``
                 and optionally ``target_count`` and ``window_start`` columns.
        group_by_cols: Column names that identify each group.

    Returns:
        tuple[pl.DataFrame, str]:
          - A DataFrame with one row per group, sorted by demand descending, with
            columns: group-key columns, ``test_rows``, and (if available)
            ``test_avg_target``, ``test_total_target``, ``months_count``.
          - The name of the primary eligibility metric used for sorting
            ('avg_target_count' or 'test_rows').
    """
    if 'target_count' in test_df.columns:
        aggregations = [
            pl.len().alias('test_rows'),
            pl.col('target_count').mean().alias('test_avg_target'),
            pl.col('target_count').sum().alias('test_total_target')
        ]
        if 'window_start' in test_df.columns:
            aggregations.append(pl.col('window_start').n_unique().alias('months_count'))

        test_counts = (
            test_df.group_by(group_by_cols)
            .agg(aggregations)
            .sort('test_avg_target', descending=True)
        )
        eligibility_metric = 'avg_target_count'
    else:
        test_counts = (
            test_df.group_by(group_by_cols)
            .agg(pl.len().alias('test_rows'))
            .sort('test_rows', descending=True)
        )
        eligibility_metric = 'test_rows'

    return test_counts, eligibility_metric


def write_empty_group_manifest(output_dir: str, group_by_cols: list,
                               eligibility_metric: str,
                               eligibility_source: str) -> None:
    """Write an empty group manifest when no groups exist in test data."""
    os.makedirs(output_dir, exist_ok=True)
    manifest = {
        'group_by_cols': group_by_cols,
        'eligibility_metric': eligibility_metric,
        'eligibility_source': eligibility_source,
        'generated_at': datetime.now().isoformat(),
        'groups': []
    }
    manifest_path = os.path.join(output_dir, 'group_manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2, default=str)
    logging.info(f"  Group manifest: {manifest_path}")


def _compute_group_cv(train_df: pl.DataFrame, group_by_cols: list) -> dict:
    """Compute the Coefficient of Variation (CV) of target counts for each group.

    CV = std(target_count) / mean(target_count) across all training rows for a group.

    Interpretation:
      - CV < 0.5  : relatively stable demand — suitable for an individual model.
      - CV >= 0.5 : highly variable demand — individual model may overfit noise;
                    the global model is used instead (controlled by ``cv_threshold``).
      - CV = inf  : mean is zero — group has no historical demand signal at all.

    Args:
        train_df: Training dataset Polars DataFrame containing ``target_count``
                  and all ``group_by_cols`` columns.
        group_by_cols: Column names that identify each group.

    Returns:
        dict: Mapping of ``tuple(group_key_values) -> float(cv)``.
              Returns an empty dict if ``target_count`` is missing.
    """
    if 'target_count' not in train_df.columns:
        logging.warning("  target_count column not found in training data; cannot compute CV.")
        return {}

    cv_df = (
        train_df.group_by(group_by_cols)
        .agg([
            pl.col('target_count').mean().alias('mean_target'),
            pl.col('target_count').std().alias('std_target'),
        ])
    )

    cv_map = {}
    for row in cv_df.iter_rows(named=True):
        group_key = tuple(row[col] for col in group_by_cols)
        mean_val = row['mean_target'] or 0.0
        std_val = row['std_target'] or 0.0
        cv = (std_val / mean_val) if mean_val > 0 else float('inf')
        cv_map[group_key] = cv
    return cv_map


def _normalize_requirement_type(val) -> str:
    """Collapse all Requirement Type variants into one of two canonical labels.

    The raw data contains values such as 'Backfill', 'Backfill - Internal', and
    'Backfill - External', as well as various forms of 'New Demand'.  This function
    maps them to a binary categorisation used when computing backfill:new-demand ratios
    that are written to ``backfill_new_demand_ratios.json``.

    Mapping rules:
      - None or NaN                       -> 'New Demand'
      - Any string starting with 'backfill' (case-insensitive) -> 'Backfill'
      - Everything else                   -> 'New Demand'

    Args:
        val: Raw Requirement Type value (str, float NaN, or None).

    Returns:
        str: One of 'Backfill' or 'New Demand'.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "New Demand"
    s = str(val).strip()
    return "Backfill" if s.lower().startswith("backfill") else "New Demand"


def write_individual_group_datasets(train_dataset_path: str, test_dataset_path: str,
                                    group_by_cols: list, output_dir: str,
                                    max_groups: int = None, cv_threshold: float = 0.5,
                                    raw_train_csv_path: str = None) -> None:
    """Write per-group train/test datasets for all groups present in test data.

    When max_groups is set and total eligible groups exceed it, only the top max_groups
    (by demand, with CV < cv_threshold) get individual datasets. Groups beyond that
    use the global model only (no combined remainder dataset). If raw_train_csv_path
    is provided and contains a 'Requirement type' column, writes backfill_new_demand_ratios.json
    (train-data only) keyed by Group_Label for use in results; Requirement type is
    normalized via _normalize_requirement_type before computing ratios.
    """
    if not group_by_cols:
        logging.error("  No group_by columns provided; cannot build per-group datasets.")
        return

    if not os.path.exists(train_dataset_path) or not os.path.exists(test_dataset_path):
        logging.error("  Missing train/test dataset files for per-group export.")
        return

    train_df = pl.read_parquet(train_dataset_path)
    test_df = pl.read_parquet(test_dataset_path)

    missing_train_cols = [col for col in group_by_cols if col not in train_df.columns]
    missing_test_cols = [col for col in group_by_cols if col not in test_df.columns]
    if missing_train_cols or missing_test_cols:
        logging.error(f"  Group-by columns missing for per-group export. Train missing: {missing_train_cols}, Test missing: {missing_test_cols}")
        return

    os.makedirs(output_dir, exist_ok=True)

    valid_counts, eligibility_metric = _compute_group_eligibility(
        test_df=test_df,
        group_by_cols=group_by_cols
    )

    train_partitions = _partition_by_group(train_df, group_by_cols)
    test_partitions = _partition_by_group(test_df, group_by_cols)

    # Compute CV per group from training data.
    # CV is used as a quality gate: high-CV groups are too noisy to benefit from
    # a dedicated individual model and are better served by the global model.
    cv_map = _compute_group_cv(train_df, group_by_cols)

    # Determine which groups get individual models vs remainder.
    # valid_counts is already sorted by demand descending (highest-demand groups first).
    all_group_rows = list(valid_counts.iter_rows(named=True))
    total_groups = len(all_group_rows)

    individual_group_rows = []
    remainder_group_rows = []

    if max_groups is not None and total_groups > max_groups:
        # Cap the number of individual models to avoid excessive training time.
        # Selection is greedy: iterate demand-descending and accept the first N groups
        # whose CV is below cv_threshold.  Groups that are rejected (either because
        # max_groups is already reached or because CV >= threshold) fall through to
        # remainder_group_rows and will use the global model at inference time.
        logging.info(f"  Total groups ({total_groups}) exceeds max_groups ({max_groups}); "
                     f"selecting top {max_groups} by demand with CV < {cv_threshold}.")

        # valid_counts is already sorted by test_avg_target desc — iterate top-down
        # and pick the first max_groups entries that pass the CV gate.
        selected_count = 0
        for row in all_group_rows:
            group_key = tuple(row[col] for col in group_by_cols)
            group_cv = cv_map.get(group_key, float('inf'))
            if selected_count < max_groups and group_cv < cv_threshold:
                individual_group_rows.append(row)
                selected_count += 1
            else:
                # Either the cap is reached or CV is too high — use global model only
                remainder_group_rows.append(row)

        logging.info(f"  Selected {len(individual_group_rows)} individual groups (CV < {cv_threshold}), "
                     f"{len(remainder_group_rows)} groups use global model only (no remainder model).")
        # Log CV values for transparency
        for row in individual_group_rows:
            gk = tuple(row[col] for col in group_by_cols)
            logging.info(f"    Individual: {gk} | CV={cv_map.get(gk, 'N/A'):.3f} | "
                         f"demand={row.get('test_total_target', row.get('test_rows', '?'))}")
        for row in remainder_group_rows[:5]:
            gk = tuple(row[col] for col in group_by_cols)
            logging.info(f"    Global-only (sample): {gk} | CV={cv_map.get(gk, 'N/A'):.3f} | "
                         f"demand={row.get('test_total_target', row.get('test_rows', '?'))}")
        if len(remainder_group_rows) > 5:
            logging.info(f"    ... and {len(remainder_group_rows) - 5} more groups using global model only")
    else:
        # All groups get individual models (no cap needed)
        individual_group_rows = all_group_rows
        if max_groups is not None:
            logging.info(f"  Total groups ({total_groups}) <= max_groups ({max_groups}); all groups get individual models.")

    manifest_rows = []
    csv_rows = []

    logging.info(
        f"  Writing per-group datasets to {output_dir} (eligibility_metric={eligibility_metric})..."
    )
    if len(individual_group_rows) == 0 and len(remainder_group_rows) == 0:
        logging.warning("  No groups in test data; writing empty manifest.")

    # Write individual group datasets
    for idx, row in enumerate(tqdm(individual_group_rows, total=len(individual_group_rows),
                                   desc="Writing per-group datasets", unit="group"), start=1):
        group_key = tuple(row[col] for col in group_by_cols)
        train_group_df = train_partitions.get(group_key)
        test_group_df = test_partitions.get(group_key)

        if test_group_df is None:
            logging.warning(f"  Missing test data for group {group_key}; skipping.")
            continue
        if train_group_df is None or len(train_group_df) == 0:
            logging.warning(f"  Missing training data for group {group_key}; skipping.")
            continue

        group_token = "__".join(sanitize_filename_token(val) for val in group_key)
        group_id = f"g{idx:05d}"

        train_file = f"train_{group_id}__{group_token}.parquet"
        test_file = f"test_{group_id}__{group_token}.parquet"

        train_path = os.path.join(output_dir, train_file)
        test_path = os.path.join(output_dir, test_file)

        train_group_df.write_parquet(train_path)
        test_group_df.write_parquet(test_path)

        group_values = {col: row[col] for col in group_by_cols}
        group_cv = cv_map.get(group_key, None)
        manifest_entry = {
            'group_id': group_id,
            'group_values': group_values,
            'train_file': train_file,
            'test_file': test_file,
            'train_rows': len(train_group_df),
            'test_rows': int(row['test_rows']),
            'is_remainder': False,
        }
        if group_cv is not None:
            manifest_entry['cv'] = round(group_cv, 4)
        if 'test_avg_target' in row:
            manifest_entry['test_avg_target'] = float(row['test_avg_target'])
        if 'test_total_target' in row:
            manifest_entry['test_total_target'] = float(row['test_total_target'])
        if 'months_count' in row:
            manifest_entry['months_count'] = int(row['months_count'])
        manifest_rows.append(manifest_entry)

        csv_row = {
            'group_id': group_id,
            'train_file': train_file,
            'test_file': test_file,
            'train_rows': len(train_group_df),
            'test_rows': int(row['test_rows'])
        }
        if group_cv is not None:
            csv_row['cv'] = round(group_cv, 4)
        if 'test_avg_target' in row:
            csv_row['test_avg_target'] = float(row['test_avg_target'])
        if 'test_total_target' in row:
            csv_row['test_total_target'] = float(row['test_total_target'])
        if 'months_count' in row:
            csv_row['months_count'] = int(row['months_count'])
        csv_row.update(group_values)
        csv_rows.append(csv_row)

    manifest = {
        'group_by_cols': group_by_cols,
        'eligibility_metric': eligibility_metric,
        'eligibility_source': test_dataset_path,
        'max_groups': max_groups,
        'cv_threshold': cv_threshold,
        'total_groups_before_cap': total_groups,
        'individual_groups': len(individual_group_rows),
        'remainder_groups': len(remainder_group_rows),
        'generated_at': datetime.now().isoformat(),
        'groups': manifest_rows
    }

    manifest_path = os.path.join(output_dir, 'group_manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2, default=str)

    # Historical backfill:new_demand ratio from train data only (for Backfill/New_demand sheets in results)
    if raw_train_csv_path and os.path.exists(raw_train_csv_path) and manifest_rows:
        try:
            raw_df = pl.read_csv(raw_train_csv_path)
            req_col = None
            for c in raw_df.columns:
                if c.strip().lower() == "requirement type":
                    req_col = c
                    break
            if req_col is not None and all(c in raw_df.columns for c in group_by_cols):
                raw_df = raw_df.with_columns(
                    pl.Series("_req_type", [_normalize_requirement_type(v) for v in raw_df[req_col]])
                )
                counts = (
                    raw_df.group_by(group_by_cols + ["_req_type"])
                    .agg(pl.len().alias("_n"))
                    .to_dicts()
                )
                key_to_counts = {}
                for row in counts:
                    key = tuple(row[c] for c in group_by_cols)
                    req_type = row["_req_type"]
                    n = row["_n"]
                    if key not in key_to_counts:
                        key_to_counts[key] = {"Backfill": 0, "New Demand": 0}
                    key_to_counts[key][req_type] = n
                ratios_by_label = {}
                for entry in manifest_rows:
                    if entry.get("is_remainder"):
                        continue
                    gv = entry.get("group_values", {})
                    if "_remainder" in gv:
                        continue
                    group_label = ", ".join(f"{c}={gv[c]}" for c in group_by_cols if c in gv)
                    key = tuple(gv[c] for c in group_by_cols if c in gv)
                    counts_ = key_to_counts.get(key, {"Backfill": 0, "New Demand": 0})
                    b, nd = counts_["Backfill"], counts_["New Demand"]
                    total = b + nd
                    ratio_backfill = (b / total) if total else 0.07
                    ratios_by_label[group_label] = {
                        "backfill": b,
                        "new_demand": nd,
                        "ratio_backfill": round(ratio_backfill, 4),
                    }
                if ratios_by_label:
                    level_avg = float(np.mean([r["ratio_backfill"] for r in ratios_by_label.values()]))
                    payload = dict(ratios_by_label)
                    payload["_level_avg_ratio_backfill"] = round(level_avg, 4)
                    ratios_path = os.path.join(output_dir, "backfill_new_demand_ratios.json")
                    with open(ratios_path, "w") as f:
                        json.dump(payload, f, indent=2)
                    logging.info(f"  Wrote train-data backfill:new_demand ratios: {ratios_path} (level_avg_backfill={level_avg:.2%})")
            else:
                if req_col is None:
                    logging.debug("  No 'Requirement type' column in raw train CSV; skipping backfill ratios.")
        except Exception as e:
            logging.warning("  Failed to compute backfill:new_demand ratios from raw train CSV: %s", e)

    if csv_rows:
        csv_path = os.path.join(output_dir, 'group_manifest.csv')
        pl.DataFrame(csv_rows).write_csv(csv_path)

    n_individual = len(manifest_rows)
    suffix = f"; {len(remainder_group_rows)} groups use global model only" if remainder_group_rows else ""
    logging.info(f"  Per-group datasets complete: {n_individual} individual{suffix} groups written.")
    logging.info(f"  Group manifest: {manifest_path}")

def classify_growth_trajectory(yearly_data: list, recent_6m_count: int = None) -> tuple:
    """
    Classify group's growth trajectory based on yearly demand patterns with recent activity validation.
    
    Step-by-step process:
    1. Validate input data and sort by year
    2. Check recent activity to prevent classifying dead groups as growing
    3. Calculate key metrics: CAGR, trend slope, volatility, momentum, acceleration
    4. Apply classification rules based on thresholds
    5. Return classification with supporting metrics
    
    Classification categories:
    - Fast Growing: Sustained high growth (>30% CAGR or accelerating) AND active in recent 6 months
    - Emerging: Recent upward momentum (positive trend in last 2 years) AND active in recent 6 months
    - Stable: Consistent demand with low volatility (<15% variation)
    - Declining: Negative growth trend
    - Fast Declining: Rapid demand drop (>30% decline or accelerating decline)
    - Volatile: High variance with no clear trend (CV > 0.5)
    - Insufficient Data: Not enough history for classification
    
    Args:
        yearly_data: List of (year, count) tuples sorted by year
        recent_6m_count: Count of demand in last 6 months (optional, for recency validation)
    
    Returns:
        tuple: (classification label, growth_rate, trend_strength, volatility_score)
    """
    # Step 1: Validate input and prepare data
    if not yearly_data or len(yearly_data) < 2:
        return ('Insufficient Data', 0.0, 0.0, 0.0)
    
    # Sort by year to ensure chronological order
    yearly_data = sorted(yearly_data, key=lambda x: x[0])
    years = np.array([y[0] for y in yearly_data])
    counts = np.array([y[1] for y in yearly_data], dtype=float)
    
    # Ensure no negative counts (data quality check)
    counts = np.maximum(counts, 0)
    
    n_years = len(counts)
    
    # Step 2: Check recent activity to prevent classifying dead groups as growing
    # This prevents classifying dead groups as growing
    recent_year_count = counts[-1]
    has_recent_activity = recent_year_count >= 2.0  # At least 2 annualized events
    
    # Additional check: if recent_6m_count is provided, use it for stricter validation
    if recent_6m_count is not None:
        has_recent_activity = has_recent_activity and recent_6m_count > 0
    
    # Step 3: Calculate key metrics for classification
    
    # 3.1. CAGR (Compound Annual Growth Rate)
    if counts[0] > 0 and counts[-1] > 0:
        cagr = (np.power(counts[-1] / counts[0], 1.0 / (n_years - 1)) - 1.0) * 100
    else:
        # Handle zero values
        if counts[0] == 0 and counts[-1] > 0:
            cagr = 100.0  # Treat as 100% growth
        elif counts[0] > 0 and counts[-1] == 0:
            cagr = -100.0  # Complete decline
        else:
            cagr = 0.0
    
    # 3.2. Linear trend slope (normalized by mean)
    if np.mean(counts) > 0:
        # Fit linear regression: counts = a * year + b
        from scipy import stats
        slope, _, r_value, _, _ = stats.linregress(years, counts)
        # Normalize slope by mean count for comparability across groups
        normalized_slope = slope / np.mean(counts)
        trend_strength = abs(r_value)  # R-squared indicates trend strength
    else:
        normalized_slope = 0.0
        trend_strength = 0.0
    
    # 3.3. Volatility (Coefficient of Variation)
    if np.mean(counts) > 0:
        cv = np.std(counts) / np.mean(counts)
    else:
        cv = 0.0
    
    # 3.4. Recent momentum (last 2 years vs previous)
    if n_years >= 3:
        recent_avg = np.mean(counts[-2:])
        previous_avg = np.mean(counts[:-2])
        if previous_avg > 0:
            recent_momentum = ((recent_avg - previous_avg) / previous_avg) * 100
        else:
            recent_momentum = 100.0 if recent_avg > 0 else 0.0
    else:
        # For 2 years, use simple YoY growth
        if counts[0] > 0:
            recent_momentum = ((counts[1] - counts[0]) / counts[0]) * 100
        else:
            recent_momentum = 100.0 if counts[1] > 0 else 0.0
    
    # 3.5. Acceleration (check if trend is accelerating)
    acceleration = 0.0
    if n_years >= 3:
        # Compare growth in first half vs second half
        mid = n_years // 2
        first_half_mean = np.mean(counts[:mid])
        second_half_mean = np.mean(counts[mid:])
        if first_half_mean > 0:
            acceleration = ((second_half_mean - first_half_mean) / first_half_mean) * 100
    
    # Step 4: Apply classification rules with recency validation
    classification = 'Stable'
    
    # Fast Growing: CAGR > 30% OR (recent momentum > 40% AND trend_strength > 0.7)
    # BUT only if there's recent activity
    if cagr > 30 or (recent_momentum > 40 and trend_strength > 0.7):
        if has_recent_activity:
            classification = 'Fast Growing'
        else:
            # Was growing but now inactive - reclassify as declining
            classification = 'Fast Declining'
    
    # Emerging: Positive recent momentum and positive trend
    # BUT only if there's recent activity
    elif recent_momentum > 15 and normalized_slope > 0 and trend_strength > 0.5:
        if has_recent_activity:
            classification = 'Emerging'
        else:
            # Was emerging but now inactive - reclassify as declining
            classification = 'Declining'
    
    # Fast Declining: CAGR < -30% OR (recent momentum < -40% AND trend_strength > 0.7)
    # OR no recent activity despite historical presence
    elif cagr < -30 or (recent_momentum < -40 and trend_strength > 0.7) or (not has_recent_activity and np.mean(counts[:-1]) > 2):
        classification = 'Fast Declining'
    
    # Declining: Negative trend with reasonable strength
    elif recent_momentum < -15 and normalized_slope < 0 and trend_strength > 0.5:
        classification = 'Declining'
    
    # Volatile: High variance with no clear trend
    elif cv > 0.5 and trend_strength < 0.5:
        classification = 'Volatile'
    
    # Stable: Low volatility, no strong trend
    elif cv < 0.25 and abs(cagr) < 15 and has_recent_activity:
        classification = 'Stable'
    
    # Moderate Growth/Decline (catch remaining cases)
    elif cagr > 5 and has_recent_activity:
        classification = 'Moderate Growth'
    elif cagr < -5:
        classification = 'Moderate Decline'
    elif not has_recent_activity:
        # Default for inactive groups
        classification = 'Fast Declining'
    
    return (classification, float(cagr), float(trend_strength), float(cv))


def calculate_growth_trajectory_classifications(df: pl.DataFrame, group_by_cols: list) -> dict:
    """
    Calculate growth trajectory classifications for all groups based on yearly patterns.

    Step-by-step process:
    1. Parse and validate date columns
    2. Extract year and month from dates
    3. Group data by specified columns + year and count events
    4. For each unique group:
       a. Calculate recent 6-month activity count
       b. Extract yearly time series with annualization for partial years
       c. Classify trajectory using classify_growth_trajectory()
    5. Log distribution statistics
    6. Return mapping of group keys to trajectory information

    Args:
        df: Training dataframe with dates
        group_by_cols: Grouping columns

    Returns:
        dict: Mapping of group keys to trajectory info dict with classification, cagr, trend_strength, volatility
    """
    trajectory_map = {}
    
    try:
        # Step 1: Parse dates if needed
        df_work = df.clone()
        if 'Req_Date' not in df_work.columns:
            df_work = df_work.with_columns([
                parse_date_flexible(pl.col('Requirement Start Date')).alias('Req_Date')
            ]).filter(pl.col('Req_Date').is_not_null())
        else:
            # Ensure Req_Date is Date type, not string
            # Check if it's already a date type
            if df_work['Req_Date'].dtype != pl.Date:
                # Try to parse it
                df_work = df_work.with_columns([
                    parse_date_flexible(pl.col('Req_Date')).alias('Req_Date')
                ]).filter(pl.col('Req_Date').is_not_null())
        
        if len(df_work) == 0:
            return {}
        
        # Ensure Req_Date is Date type before extracting year
        req_date_dtype = df_work['Req_Date'].dtype
        if req_date_dtype not in [pl.Date, pl.Datetime]:
            # It's likely a string, try parsing
            df_work = df_work.with_columns([
                parse_date_flexible(pl.col('Req_Date')).alias('Req_Date')
            ]).filter(pl.col('Req_Date').is_not_null())
        
        # If it's Datetime, cast to Date
        if df_work['Req_Date'].dtype == pl.Datetime:
            df_work = df_work.with_columns([
                pl.col('Req_Date').cast(pl.Date).alias('Req_Date')
            ])
        
        # Step 2: Extract year and month from dates
        df_work = df_work.with_columns([
            pl.col('Req_Date').dt.year().alias('year'),
            pl.col('Req_Date').dt.month().alias('month')
        ])
        
        # Get current year from max date to identify partial years (for annualization)
        current_year = df_work.select(pl.col('Req_Date').dt.year().max()).item()
        
        # Step 3: Group by specified columns + year, count events
        agg_cols = group_by_cols + ['year']
        grouped = df_work.group_by(agg_cols).agg([
            pl.count().alias('count')
        ])
        
        # Extract unique group keys from grouped data
        group_keys_set = set()
        for row in grouped.iter_rows(named=True):
            group_key = tuple(row.get(col) for col in group_by_cols)
            group_keys_set.add(group_key)
        
        # Get max date to calculate recent 6 months activity
        max_date = df_work.select(pl.col('Req_Date').max()).item()
        
        # Step 4: Process each group to calculate trajectory
        for group_key in group_keys_set:
            # Filter to this group
            group_filter = pl.lit(True)
            for i, col in enumerate(group_by_cols):
                group_filter = group_filter & (pl.col(col) == group_key[i])
            
            group_data = grouped.filter(group_filter)
            
            # Step 4a: Calculate recent 6-month count for recency validation
            group_filter_full = pl.lit(True)
            for i, col in enumerate(group_by_cols):
                group_filter_full = group_filter_full & (pl.col(col) == group_key[i])
            
            group_all_data = df_work.filter(group_filter_full)
            
            # Count events in last 6 months (180 days) to validate recent activity
            recent_6m_count = 0
            if len(group_all_data) > 0 and max_date is not None:
                # Calculate date 6 months ago
                from datetime import timedelta
                six_months_ago = max_date - timedelta(days=180)
                recent_data = group_all_data.filter(pl.col('Req_Date') >= six_months_ago)
                recent_6m_count = len(recent_data)
            
            # Step 4b: Extract yearly data with annualization for partial years
            yearly_data = []
            for row in group_data.iter_rows(named=True):
                year = row.get('year')
                count = row.get('count', 0)
                
                # Annualize if this is the current year (partial year)
                if year == current_year:
                    year_data = group_all_data.filter(pl.col('year') == year)
                    if len(year_data) > 0:
                        # Get unique months with data
                        months_with_data = year_data['month'].unique().sort()
                        n_months = len(months_with_data)
                        
                        # Annualize: multiply by (12 / number of months)
                        if n_months > 0 and n_months < 12:
                            count = count * (12.0 / n_months)
                            logging.debug(f"Annualized {year} for group {group_key}: {n_months} months -> {count:.1f}")
                
                if year is not None:
                    yearly_data.append((year, count))
            
            # Step 4c: Classify trajectory with recent activity check
            classification, cagr, trend_strength, volatility = classify_growth_trajectory(yearly_data, recent_6m_count)
            
            trajectory_map[group_key] = {
                'trajectory_class': classification,
                'cagr': cagr,
                'trend_strength': trend_strength,
                'volatility': volatility,
                'n_years': len(yearly_data),
                'recent_6m_count': recent_6m_count
            }
        
        logging.info(f"Calculated growth trajectories for {len(trajectory_map)} groups")
        
        # Step 5: Log distribution and recency statistics
        class_counts = {}
        inactive_count = 0
        for info in trajectory_map.values():
            cls = info['trajectory_class']
            class_counts[cls] = class_counts.get(cls, 0) + 1
            if info.get('recent_6m_count', 0) == 0:
                inactive_count += 1
        
        logging.info(f"Trajectory distribution: {class_counts}")
        logging.info(f"Groups with zero demand in last 6 months: {inactive_count}/{len(trajectory_map)} ({100*inactive_count/len(trajectory_map):.1f}%)")
        
    except Exception as e:
        logging.warning(f"Error calculating growth trajectories: {e}")
    
    return trajectory_map




# ================================================================================================
# FTE-BASED FEATURE ENGINEERING (FROM GROUP-BASED UPLF → DFC MAPPING)
# ================================================================================================


def _parse_fte_month(col_name: str):
    """
    Parse month/year from an FTE column name like 'Jan-24_Actuals' or 'Mar-26_Forecast'.

    Returns:
        tuple[int, int] | None: (year, month) for sorting, or None if parsing fails.
    """
    try:
        # Expect pattern like 'Jan-24_Actuals'
        base = col_name.split('_')[0]
        m = re.match(r'([A-Za-z]{3})-(\d{2})', base)
        if not m:
            return None
        month_str, yy = m.groups()
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        month = month_map.get(month_str.upper())
        if month is None:
            return None
        year = 2000 + int(yy)
        return year, month
    except Exception:
        return None


def add_fte_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Using per-row FTE month × type history, compute FTE features from UPLF data.

    Step-by-step process:
    1. Identify FTE columns by suffix (_Actuals, _Forecast, _Adjustments)
    2. Sort columns chronologically by parsing month/year from column names
    3. Extract FTE matrices as numpy arrays for efficient computation
    4. Identify missing UPLF data using has_uplf_data flag
    5. Compute features:
       a. Recent demand features (forecast last month)
       b. Categorical encodings (industry segment, customer classification)
    6. Apply missing data rule: set all FTE features to 0.0 if UPLF data is missing
    7. Add computed features to dataframe

    Features computed:
      - fte_forecast_last_month: Most recent forecast value
      - industry_segment_encoded: Encoded industry segment category
      - customer_classification_encoded: Encoded customer classification

    Missing data rule:
      If has_uplf_data == 1 (missing UPLF data) → all FTE features = 0.0.
      Uses has_uplf_data flag from data_split.py instead of checking for -1 values.
    """
    logging.info("Adding FTE-based features from UPLF history...")

    # Step 1: Identify FTE columns by suffix
    actual_cols = [c for c in df.columns if c.endswith('_Actuals')]
    forecast_cols = [c for c in df.columns if c.endswith('_Forecast')]
    adj_cols = [c for c in df.columns if c.endswith('_Adjustments')]

    if not actual_cols and not forecast_cols and not adj_cols:
        logging.warning("No FTE columns (*_Actuals/_Forecast/_Adjustments) found; skipping FTE features.")
        return df

    # Step 2: Sort columns chronologically using parsed month/year
    def sort_by_month(cols):
        keyed = []
        for c in cols:
            key = _parse_fte_month(c)
            if key is not None:
                keyed.append((key, c))
        keyed.sort()
        return [c for _, c in keyed]

    actual_cols = sort_by_month(actual_cols)
    forecast_cols = sort_by_month(forecast_cols)
    adj_cols = sort_by_month(adj_cols)

    if not actual_cols:
        logging.warning("No parsable *_Actuals columns after month parsing; skipping FTE features.")
        return df

    # Step 3: Extract FTE matrices as numpy for efficient row-wise feature computation
    actual_mat = df.select(actual_cols).to_numpy()
    forecast_mat = df.select(forecast_cols).to_numpy() if forecast_cols else None
    adj_mat = df.select(adj_cols).to_numpy() if adj_cols else None

    # Step 4: Use has_uplf_data flag to identify missing UPLF data (1 = missing, 0 = has data)
    if 'has_uplf_data' in df.columns:
        missing_mask = df['has_uplf_data'].to_numpy() == 1  # 1 = missing UPLF data
    else:
        # Fallback: if flag doesn't exist, check for all-zero FTE columns (might indicate missing)
        all_fte_mats = [m for m in [actual_mat, forecast_mat, adj_mat] if m is not None]
        if all_fte_mats:
            combined = np.concatenate(all_fte_mats, axis=1)
            missing_mask = (combined == 0).all(axis=1)  # All zeros might indicate missing
        else:
            missing_mask = np.zeros(actual_mat.shape[0], dtype=bool)

    n_rows, n_months = actual_mat.shape
    last_idx = n_months - 1

    # Helper for safe slices (handle <3 months case)
    def last_k(arr, k):
        if arr.shape[1] == 0:
            return np.zeros(arr.shape[0])
        if arr.shape[1] < k:
            return arr
        return arr[:, -k:]

    # Step 5: Compute features
    # 5a. Recent Demand features
    last3_actual = last_k(actual_mat, 3)

    if forecast_mat is not None and forecast_mat.shape[1] > 0:
        fte_forecast_last_month = forecast_mat[:, forecast_mat.shape[1] - 1]
        last3_forecast = last_k(forecast_mat, 3)
    else:
        fte_forecast_last_month = np.zeros(n_rows)
        last3_forecast = np.zeros_like(last3_actual)

    # 5b. Categorical encodings from UPLF strings
    def encode_categorical(col_name: str, encoded_name: str):
        if col_name not in df.columns:
            return np.full(n_rows, -1, dtype=float)
        col = df[col_name].fill_null("").cast(pl.Utf8)
        values = col.to_list()
        uniq = {}
        codes = np.empty(n_rows, dtype=float)
        next_code = 0
        for i, v in enumerate(values):
            key = v if v is not None else ""
            if key == "":
                codes[i] = -1.0
            else:
                if key not in uniq:
                    uniq[key] = next_code
                    next_code += 1
                codes[i] = float(uniq[key])
        return codes

    industry_segment_encoded = encode_categorical("Industry Segment", "industry_segment_encoded")
    customer_classification_encoded = encode_categorical("Customer Classification", "customer_classification_encoded")

    # Step 6: Apply missing rule: if has_uplf_data == 1 (missing) → all FTE features = 0.0
    def apply_missing_rule(arr):
        arr = arr.astype(float)
        arr[missing_mask] = 0.0  # Use 0.0 instead of -1.0 for CatBoost compatibility
        return arr

    feature_series = {
        "fte_forecast_last_month": apply_missing_rule(fte_forecast_last_month),
        # Note: fte_actuals_yoy_same_month and fte_adjustments_last_month removed - zero SHAP importance
        "industry_segment_encoded": industry_segment_encoded,
        "customer_classification_encoded": customer_classification_encoded,
    }

    # Step 7: Add computed features to dataframe
    df = df.with_columns([pl.Series(name, vals) for name, vals in feature_series.items()])
    logging.info("  Added 5 FTE-based features.")
    return df


# ================================================================================================
# SMA AND WMA FEATURE COMPUTATION
# ================================================================================================

def compute_sma_wma_features(cutoff, past_events, months_history=[3, 6]):
    """
    Compute Simple Moving Average (SMA) and Weighted Moving Average (WMA) features
    from historical monthly counts.
    
    This function calculates baseline forecasting features that can help ML models
    learn from simple time series patterns. SMA uses equal weights, while WMA uses
    exponential smoothing (more recent months get higher weights).
    
    Args:
        cutoff: Cutoff date for feature computation
        past_events: List of past event dates before cutoff
        months_history: List of months to use for averaging (default: [3, 6])
    
    Returns:
        dict: Dictionary with SMA and WMA features for each month history
    """
    features = {}
    
    if len(past_events) == 0:
        # No history - return zeros for all features
        for months in months_history:
            features[f'sma_{months}m'] = 0.0
            features[f'wma_{months}m'] = 0.0
        return features
    
    # Convert cutoff to date if it's datetime
    if isinstance(cutoff, datetime):
        cutoff_date = cutoff.date()
    else:
        cutoff_date = cutoff
    
    # Compute monthly counts for the last N months before cutoff
    monthly_counts = []
    for month_offset in range(max(months_history)):
        # Calculate the month we're looking at (going backwards from cutoff)
        target_year = cutoff_date.year
        target_month = cutoff_date.month - month_offset
        
        # Handle year rollover
        while target_month < 1:
            target_year -= 1
            target_month += 12
        
        # Get first and last day of that month
        month_start = datetime(target_year, target_month, 1).date()
        if target_month == 12:
            month_end = datetime(target_year + 1, 1, 1).date()
        else:
            month_end = datetime(target_year, target_month + 1, 1).date()
        
        # Count events in this month
        def to_date(d):
            if isinstance(d, datetime):
                return d.date()
            elif hasattr(d, 'date'):
                return d.date()
            else:
                return d
        
        month_count = len([d for d in past_events if month_start <= to_date(d) < month_end])
        monthly_counts.append(month_count)
    
    # Reverse to get chronological order (oldest first)
    monthly_counts = list(reversed(monthly_counts))
    
    # Compute SMA and WMA for each requested history length
    for months in months_history:
        if len(monthly_counts) < months:
            # Not enough history - use available months or zero
            available_counts = monthly_counts[:len(monthly_counts)] if len(monthly_counts) > 0 else [0]
            sma_value = np.mean(available_counts) if len(available_counts) > 0 else 0.0
            
            # For WMA, use exponential smoothing with available data
            if len(available_counts) > 0:
                alpha = 0.3  # Exponential smoothing factor
                weights = np.power(1 - alpha, np.arange(len(available_counts)))[::-1]  # Most recent gets highest weight
                weights = weights / weights.sum()
                wma_value = np.sum(weights * np.array(available_counts))
            else:
                wma_value = 0.0
        else:
            # Enough history - compute SMA and WMA
            recent_counts = monthly_counts[-months:]  # Last N months
            
            # SMA: Simple average
            sma_value = np.mean(recent_counts)
            
            # WMA: Weighted moving average with exponential smoothing
            alpha = 0.3  # Exponential smoothing factor (higher = more weight on recent)
            weights = np.power(1 - alpha, np.arange(len(recent_counts)))[::-1]  # Most recent gets highest weight
            weights = weights / weights.sum()  # Normalize weights
            wma_value = np.sum(weights * np.array(recent_counts))
        
        features[f'sma_{months}m'] = float(max(0.0, sma_value))  # Ensure non-negative
        features[f'wma_{months}m'] = float(max(0.0, wma_value))  # Ensure non-negative
    
    return features


def compute_growth_and_trend_features(cutoff, past_events):
    """Compute demand growth rates and a normalised linear trend from historical monthly counts.

    All growth percentages compare a "recent" window to the equal-length "previous" window
    immediately before it.  E.g. demand_growth_3m compares months [-3, -1] vs [-6, -4]
    relative to the cutoff.

    Features computed
    -----------------
    demand_growth_3m : % change in demand over the past 3 months vs the prior 3 months.
                       Requires >= 5 events in the prior window to be non-zero; capped ±300%.
    demand_growth_6m : Same but for 6-month windows.
    demand_growth_9m : Same but for 9-month windows.
    recent_vs_historical_ratio : Mean monthly rate in last 3 months divided by mean rate in
                                  last 6 months.  Values > 1 indicate acceleration; < 1 indicate
                                  deceleration.  Capped [0.2, 5.0].
    trend_slope_6m   : Normalised OLS slope (events/month / mean_monthly_events) over the last
                       6 complete calendar months.  Positive = growing, negative = shrinking.
                       Capped ±2.0.  Requires >= 3 months and >= 5 total events.

    The constant _MIN_EVENTS_FOR_GROWTH = 5 prevents noisy percentage calculations when the
    comparison window has very little data (e.g. 1 -> 2 events would appear as 100% growth).

    Args:
        cutoff: Reference date (``datetime.date`` or ``datetime``).  Month offsets are counted
                backwards from this date.
        past_events: List of historical event dates (``datetime.date`` or ``datetime``)
                     that occurred BEFORE ``cutoff``.

    Returns:
        dict: Feature name -> float.  All features default to 0.0 (or 1.0 for ratios)
              when there is insufficient history.
    """
    features = {}

    if len(past_events) == 0:
        features['demand_growth_3m'] = 0.0
        features['demand_growth_6m'] = 0.0
        features['demand_growth_9m'] = 0.0
        features['recent_vs_historical_ratio'] = 1.0
        features['trend_slope_6m'] = 0.0
        return features

    if isinstance(cutoff, datetime):
        cutoff_date = cutoff.date()
    else:
        cutoff_date = cutoff

    def to_date(d):
        if isinstance(d, datetime):
            return d.date()
        elif hasattr(d, 'date'):
            return d.date()
        return d

    # Build monthly event counts for the last 19 calendar months before cutoff.
    # 19 months needed: offset 0 (partial cutoff month) + 9 recent + 9 comparison = 19
    monthly_counts = []
    for month_offset in range(19):
        target_year = cutoff_date.year
        target_month = cutoff_date.month - month_offset

        while target_month < 1:
            target_year -= 1
            target_month += 12

        month_start = datetime(target_year, target_month, 1).date()
        if target_month == 12:
            month_end = datetime(target_year + 1, 1, 1).date()
        else:
            month_end = datetime(target_year, target_month + 1, 1).date()

        count = len([d for d in past_events if month_start <= to_date(d) < month_end])
        monthly_counts.append(count)

    # Minimum event threshold: growth percentages are unreliable when the comparison
    # period has very few events (e.g. 3 -> 5 = "66% growth" is noise, not signal).
    # Raising this value reduces false positive "growth" signals for sparse groups;
    # lowering it allows growth to be measured even for low-volume groups.
    _MIN_EVENTS_FOR_GROWTH = 5

    # monthly_counts layout (index = distance from cutoff in complete months):
    #   [0]      = partial cutoff month (may be incomplete; always excluded from windows)
    #   [1..3]   = last 3 complete months  (recent window for 3m growth)
    #   [4..6]   = previous 3 complete months (comparison window for 3m growth)
    #   [1..6]   = last 6 complete months  (recent window for 6m growth)
    #   [7..12]  = previous 6 complete months (comparison window for 6m growth)
    #   [1..9]   = last 9 complete months  (recent window for 9m growth)
    #   [10..18] = previous 9 complete months (comparison window for 9m growth)
    sum_recent_3m = sum(monthly_counts[1:4])    # offsets 1-3 = last 3 complete months
    sum_prev_3m = sum(monthly_counts[4:7])      # offsets 4-6 = prior 3 complete months

    if sum_prev_3m >= _MIN_EVENTS_FOR_GROWTH:
        raw = (sum_recent_3m - sum_prev_3m) / sum_prev_3m * 100.0
        features['demand_growth_3m'] = float(min(max(raw, -300.0), 300.0))
    else:
        features['demand_growth_3m'] = 0.0

    # offsets 1-6 = last 6 complete months, offsets 7-12 = previous 6 complete months
    sum_recent_6m = sum(monthly_counts[1:7])
    sum_prev_6m = sum(monthly_counts[7:13])

    if sum_prev_6m >= _MIN_EVENTS_FOR_GROWTH:
        raw = (sum_recent_6m - sum_prev_6m) / sum_prev_6m * 100.0
        features['demand_growth_6m'] = float(min(max(raw, -300.0), 300.0))
    else:
        features['demand_growth_6m'] = 0.0

    # offsets 1-9 = last 9 complete months, offsets 10-18 = previous 9 complete months
    sum_recent_9m = sum(monthly_counts[1:10])
    sum_prev_9m = sum(monthly_counts[10:19])

    if sum_prev_9m >= _MIN_EVENTS_FOR_GROWTH:
        raw = (sum_recent_9m - sum_prev_9m) / sum_prev_9m * 100.0
        features['demand_growth_9m'] = float(min(max(raw, -300.0), 300.0))
    else:
        features['demand_growth_9m'] = 0.0

    # Short-term vs medium-term demand ratio (acceleration indicator)
    # Capped at [0.2, 5.0] to prevent extreme values from tiny denominators
    mean_recent_3m = sum_recent_3m / 3.0
    mean_recent_6m = sum_recent_6m / 6.0
    ratio = mean_recent_3m / max(mean_recent_6m, 0.1)
    features['recent_vs_historical_ratio'] = float(min(max(ratio, 0.2), 5.0))

    # Normalized linear regression slope over the last 6 complete months (oldest first)
    # Capped at [-2.0, 2.0] to prevent extreme normalized slopes
    trend_counts = list(reversed(monthly_counts[1:7]))
    if len(trend_counts) >= 3 and sum(trend_counts) >= _MIN_EVENTS_FOR_GROWTH:
        x = np.arange(len(trend_counts), dtype=float)
        y = np.array(trend_counts, dtype=float)
        mean_y = np.mean(y)

        n = len(x)
        numerator = n * np.sum(x * y) - np.sum(x) * np.sum(y)
        denominator = n * np.sum(x ** 2) - np.sum(x) ** 2
        slope = numerator / max(denominator, 1e-10)

        raw_slope = slope / max(mean_y, 1.0)
        features['trend_slope_6m'] = float(min(max(raw_slope, -2.0), 2.0))
    else:
        features['trend_slope_6m'] = 0.0

    return features


# ================================================================================================
# GROWTH BY COUNTRY LOADING
# ================================================================================================


def load_quarter_growth_map(quarter_growth_file):
    """Load the quarter-growth-by-country CSV into an in-memory lookup dictionary.

    The CSV (typically ``Quarter_growth_by_country_uplf_train.csv``) contains one row
    per (Country, year, month) combination with the column
    ``quarter_growth_pct_from_sum`` holding the percentage change in headcount
    demand for that country in that calendar month.

    The function normalises country names to UPPER-CASE stripped strings so that
    minor casing differences between the CSV and the training data do not cause
    lookup misses.

    Expected CSV columns:
        Country                    -- country name string
        year                       -- 4-digit integer year
        month                      -- integer month (1-12)
        quarter_growth_pct_from_sum -- float percentage (e.g., 3.5 means +3.5 %)

    Missing or unreadable file:
        Returns an empty dict; the caller then defaults all growth values to 0.0
        (see ``get_quarter_growth_value``).

    Args:
        quarter_growth_file: Filesystem path to the CSV file, or None.

    Returns:
        dict: Mapping of ``(COUNTRY_UPPER, year_int, month_int) -> float`` growth pct.
    """
    quarter_growth_map = {}

    if not quarter_growth_file or not os.path.exists(quarter_growth_file):
        logging.warning(f"Quarter growth file not found or not provided: {quarter_growth_file}, defaulting all quarter growth to 0.0")
        return quarter_growth_map

    try:
        df = pl.read_csv(quarter_growth_file)
        expected_cols = {'Country', 'year', 'month', 'quarter_growth_pct_from_sum'}
        missing = expected_cols.difference(set(df.columns))
        if missing:
            logging.warning(f"Quarter growth file {quarter_growth_file} is missing expected columns {missing}, defaulting all quarter growth to 0.0")
            return {}

        df = df.with_columns([
            pl.col('Country').cast(pl.Utf8, strict=False),
            pl.col('year').cast(pl.Int32, strict=False),
            pl.col('month').cast(pl.Int32, strict=False),
            pl.col('quarter_growth_pct_from_sum').cast(pl.Float64, strict=False)
        ])

        for row in df.iter_rows(named=True):
            country = row.get('Country')
            year = row.get('year')
            month = row.get('month')
            quarter_growth = row.get('quarter_growth_pct_from_sum')

            if country is None or year is None or month is None:
                # Skip keys that cannot be used; they will be treated as 0.0 later
                continue

            key = (str(country).strip().upper(), int(year), int(month))
            try:
                quarter_growth_val = float(quarter_growth) if quarter_growth is not None else 0.0
            except Exception:
                quarter_growth_val = 0.0
            quarter_growth_map[key] = quarter_growth_val

        logging.info(f"Loaded quarter growth map from {quarter_growth_file} with {len(quarter_growth_map)} (country, year, month) entries")
        return quarter_growth_map

    except Exception as e:
        logging.warning(f"Failed to load quarter growth data from {quarter_growth_file}: {e}")
        return {}


def get_quarter_growth_value(quarter_growth_map, country_val, year, month):
    """Safely retrieve the quarter-growth percentage for a (country, year, month) triple.

    Country names are normalised to UPPER-CASE with surrounding whitespace stripped
    to match the key format built in ``load_quarter_growth_map``.

    Returns 0.0 in all error / missing cases so that the feature degrades gracefully:
      - The map is None or empty.
      - country_val, year, or month is None.
      - The key is not present in the map.
      - The stored value cannot be cast to float.

    Args:
        quarter_growth_map: Dict returned by ``load_quarter_growth_map``, or None.
        country_val: Country identifier string from the training/test row.
        year: Integer calendar year of the cutoff or target month.
        month: Integer calendar month (1-12) of the cutoff or target month.

    Returns:
        float: Quarter growth percentage, or 0.0 if unavailable.
    """
    if not quarter_growth_map or country_val is None or year is None or month is None:
        return 0.0

    try:
        key = (str(country_val).strip().upper(), int(year), int(month))
    except Exception:
        return 0.0

    val = quarter_growth_map.get(key)
    try:
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0

# ================================================================================================
# HELPER FUNCTIONS
# ================================================================================================

def parse_date_flexible(date_col):
    """Parse a string date column into a Polars ``Date`` series, trying three formats.

    Format priority:
      1. ISO 8601 : YYYY-MM-DD   (most common in exported CSVs)
      2. US slash : MM/DD/YYYY   (common in Excel exports)
      3. US dash  : MM-DD-YYYY   (less common, included for completeness)

    Each format is attempted in order; the first non-null parse wins.  Rows that do
    not match any format result in null, which callers should ``.filter(is_not_null())``.

    Args:
        date_col: A Polars Utf8/String expression or Series containing raw date strings.

    Returns:
        A Polars expression that evaluates to ``pl.Date`` (null where parsing fails).
    """
    parsed_iso   = date_col.str.strptime(pl.Date, '%Y-%m-%d', strict=False)
    parsed_slash = date_col.str.strptime(pl.Date, '%m/%d/%Y', strict=False)
    parsed_dash  = date_col.str.strptime(pl.Date, '%m-%d-%Y', strict=False)
    return (
        pl.when(parsed_iso.is_not_null()).then(parsed_iso)
        .when(parsed_slash.is_not_null()).then(parsed_slash)
        .otherwise(parsed_dash)
    )


def compute_trend_slope(dates, cutoff, window_days=90):
    """Compute a linear trend slope (events/day) for recent events using OLS.

    Fits a simple linear regression of cumulative-event-count against calendar-day
    offset within the trailing ``window_days`` window before ``cutoff``.  A positive
    slope means demand is accelerating; a negative slope means it is declining.

    The slope is measured in "cumulative events per day", which is equivalent to the
    average daily event rate within the window.

    Args:
        dates: Full list of historical event dates for the group.
        cutoff: Reference date; only events strictly before this date are considered.
        window_days: Look-back window in calendar days (default: 90).

    Returns:
        float: OLS slope in events per day, or 0.0 if fewer than 2 events fall in
               the window or if all events have the same timestamp.
    """
    if len(dates) < 2:
        return 0.0

    # Restrict to events within the look-back window and before the cutoff
    recent_dates = [d for d in dates if (cutoff - d).days <= window_days and d < cutoff]

    if len(recent_dates) < 2:
        return 0.0

    # x = days since first event in window (numeric predictor)
    # y = cumulative count index (0, 1, 2, …), acting as the response variable
    first_recent = min(recent_dates)
    x = np.array([(d - first_recent).days for d in recent_dates])
    y = np.arange(len(recent_dates))  # Cumulative count

    # OLS closed-form: slope = Cov(x, y) / Var(x)
    n = len(x)
    if n < 2 or np.std(x) < 1e-6:
        return 0.0

    slope = np.sum((x - np.mean(x)) * (y - np.mean(y))) / np.sum((x - np.mean(x)) ** 2)
    return float(slope)


def compute_lagged_window_counts(cutoff, past_events, lookback_periods, past_events_np=None, cutoff_days=None, min_date=None):
    """Count demand events that fall in each trailing window before the cutoff date.

    For each period ``p`` in ``lookback_periods`` the function counts events in the
    half-open interval ``[cutoff - p days, cutoff)``.  The result is stored as
    ``events_last_{p}d`` in the returned dict.

    Two code paths:
      Fast path  -- When ``past_events_np`` (numpy int array of days since ``min_date``)
                    and ``cutoff_days`` are provided, vectorised numpy comparisons are used.
                    This is ~10x faster for large groups and is the normal production path.
      Fallback   -- If the numpy arrays are unavailable, falls back to a list comprehension
                    using raw date arithmetic.

    Args:
        cutoff: Cutoff date (``datetime.date``); defines the right edge of every window.
        past_events: List of event dates before cutoff (used in fallback path only).
        lookback_periods: List of window sizes in calendar days (e.g., [30, 90, 180]).
        past_events_np: Optional numpy int array of past event dates expressed as days
                        since ``min_date``.  Enables fast-path vectorisation.
        cutoff_days: Optional int — the cutoff date expressed as days since ``min_date``.
        min_date: The global minimum date in the dataset (origin for day-offset arithmetic).

    Returns:
        dict: ``{'events_last_{p}d': int, ...}`` for each period in ``lookback_periods``.
    """
    lagged_counts = {}

    # Use vectorized numpy operations if available
    if past_events_np is not None and cutoff_days is not None:
        for period in lookback_periods:
            # Compute events in the period before cutoff
            window_start_days = cutoff_days - period
            window_end_days = cutoff_days
            count = int(np.sum((past_events_np >= window_start_days) & (past_events_np < window_end_days)))
            lagged_counts[f'events_last_{period}d'] = count

    else:
        # Fallback to original implementation
        for period in lookback_periods:
            window_start = cutoff - timedelta(days=period)
            window_end = cutoff
            count = len([d for d in past_events if window_start <= d < window_end])
            lagged_counts[f'events_last_{period}d'] = count


    return lagged_counts


def compute_hierarchy_aggregates(df, cutoff, group_keys, group_by_cols):
    """
    Compute hierarchy-level demand aggregates with seasonality features.
    
    Step-by-step process:
    1. Filter data to events before cutoff date
    2. Extract group key values (country, skill cluster, SO GRADE)
    3. Compute Level 1: Country-level aggregates (seasonality, same-month averages)
    4. Compute Level 2A: (Country, Skill Cluster) aggregates (total events, seasonality strength)
    5. Compute Level 2B: (Country, SO GRADE) aggregates (total events)
    6. Compute BU-level aggregates (if BU is in grouping columns)
    7. Return aggregated features dictionary
    
    Hierarchy: Country → Skill Cluster → SO GRADE (or Country → SO GRADE → Skill Cluster)
    Adds aggregate features at each level so sparse leaf groups can borrow strength from parents.
    
    Args:
        df: Full dataframe with all events
        cutoff: Cutoff date for filtering past events
        group_keys: Tuple of group key values
        group_by_cols: List of grouping column names
    
    Returns:
        dict: Dictionary of hierarchy-level aggregate features
    """
    aggregates = {}

    # Step 1: Filter data before cutoff
    df_past = df.filter(pl.col('Req_Date') < cutoff)

    if len(df_past) == 0:
        return _get_default_hierarchy_aggregates()

    # Step 2: Extract group key values
    country_val = None
    skill_cluster_val = None
    so_grade_val = None
    
    if 'Country' in group_by_cols:
        country_idx = group_by_cols.index('Country')
        country_val = group_keys[country_idx] if country_idx < len(group_keys) else None

    if 'Skill Cluster' in group_by_cols:
        skill_idx = group_by_cols.index('Skill Cluster')
        skill_cluster_val = group_keys[skill_idx] if skill_idx < len(group_keys) else None
    
    if 'SO GRADE' in group_by_cols:
        grade_idx = group_by_cols.index('SO GRADE')
        so_grade_val = group_keys[grade_idx] if grade_idx < len(group_keys) else None

    # Step 3: LEVEL 1: COUNTRY-LEVEL AGGREGATES
        if country_val is not None:
            country_df = df_past.filter(pl.col('Country') == country_val)
            country_events = len(country_df)
            country_dates = country_df['Req_Date'].to_list()

            if len(country_dates) > 0:
                country_span = max((cutoff - min(country_dates)).days, 1)
            
            # Country-level seasonality features
            country_month_counts = {}
            for date in country_dates:
                month = date.month
                country_month_counts[month] = country_month_counts.get(month, 0) + 1
            
            if len(country_month_counts) > 0:
                month_values = list(country_month_counts.values())

                # Month-specific averages
                cutoff_month = cutoff.month
                aggregates['country_avg_same_month'] = country_month_counts.get(cutoff_month, 0) / max(1, country_span / 365.0)
            else:
                aggregates['country_avg_same_month'] = 0.0
        else:
            aggregates.update(_get_default_country_aggregates())

    # ====================================================================
    # LEVEL 2A: (COUNTRY, SKILL CLUSTER) AGGREGATES
    # ====================================================================
    if country_val is not None and skill_cluster_val is not None:
        country_skill_df = df_past.filter(
            (pl.col('Country') == country_val) & 
            (pl.col('Skill Cluster') == skill_cluster_val)
        )
        country_skill_events = len(country_skill_df)
        country_skill_dates = country_skill_df['Req_Date'].to_list()

        if len(country_skill_dates) > 0:
            country_skill_span = max((cutoff - min(country_skill_dates)).days, 1)
            # Removed country_skill_total_events due to high correlation with country_avg_same_month
            
            # (Country, Skill) seasonality
            country_skill_month_counts = {}
            for date in country_skill_dates:
                month = date.month
                country_skill_month_counts[month] = country_skill_month_counts.get(month, 0) + 1
            
            if len(country_skill_month_counts) > 0:
                month_values = list(country_skill_month_counts.values())
                aggregates['country_skill_seasonality_strength'] = float((max(month_values) - min(month_values)) / max(np.mean(month_values), 1.0))
            else:
                aggregates['country_skill_seasonality_strength'] = 0.0
        else:
            aggregates.update(_get_default_country_skill_aggregates())

    # ====================================================================
    # LEVEL 2B: (COUNTRY, SO GRADE) AGGREGATES
    # ====================================================================
    if country_val is not None and so_grade_val is not None:
        country_grade_df = df_past.filter(
            (pl.col('Country') == country_val) & 
            (pl.col('SO GRADE') == so_grade_val)
        )
        country_grade_events = len(country_grade_df)
        country_grade_dates = country_grade_df['Req_Date'].to_list()

        if len(country_grade_dates) > 0:
            country_grade_span = max((cutoff - min(country_grade_dates)).days, 1)
            
            # (Country, SO GRADE) seasonality
            country_grade_month_counts = {}
            for date in country_grade_dates:
                month = date.month
                country_grade_month_counts[month] = country_grade_month_counts.get(month, 0) + 1
            
            if len(country_grade_month_counts) > 0:
                month_values = list(country_grade_month_counts.values())
        else:
            aggregates.update(_get_default_country_grade_aggregates())


    # BU-level aggregates (keep existing for backward compatibility)
    if 'BU' in group_by_cols:
        bu_idx = group_by_cols.index('BU')
        bu_val = group_keys[bu_idx] if bu_idx < len(group_keys) else None

        if bu_val is not None:
            bu_df = df_past.filter(pl.col('BU') == bu_val)
            bu_events = len(bu_df)
            bu_dates = bu_df['Req_Date'].to_list()

            if len(bu_dates) > 0:
                bu_span = max((cutoff - min(bu_dates)).days, 1)
                aggregates['bu_demand_density'] = bu_events / bu_span
                aggregates['bu_total_events'] = bu_events
                aggregates['bu_events_last_90d'] = len([d for d in bu_dates if (cutoff - d).days <= 90])
            else:
                aggregates['bu_demand_density'] = 0.0
                aggregates['bu_total_events'] = 0
                aggregates['bu_events_last_90d'] = 0

    return aggregates

def _get_default_hierarchy_aggregates():
    """Return default values for all hierarchy aggregates when no data available."""
    defaults = _get_default_country_aggregates()
    defaults.update(_get_default_country_skill_aggregates())
    defaults.update(_get_default_country_grade_aggregates())
    defaults.update({
        'bu_demand_density': 0.0,
        'bu_total_events': 0,
        'bu_events_last_90d': 0
    })
    return defaults

def _get_default_country_aggregates():
    """Default country-level aggregates."""
    return {
        'country_avg_same_month': 0.0
    }

def _get_default_country_skill_aggregates():
    """Default (Country, Skill Cluster) aggregates."""
    return {
        'country_skill_seasonality_strength': 0.0
    }

def _get_default_country_grade_aggregates():
    """Default (Country, SO GRADE) aggregates."""
    return {}

def compute_enriched_features(cutoff, past_events, group_keys, group_by_cols, df_full=None,
                             group_df=None, has_quantity=False, past_events_np=None,
                             cutoff_days=None, min_date=None, lookback_periods=None,
                             skill_events_map=None, so_grade_events_map=None, so_grade_skill_events_map=None,
                             trajectory_classifications=None):
    """
    Compute enriched feature set including lagged counts, trends, calendar, hierarchy aggregates, and growth trajectories.
    
    Step-by-step process:
    1. Initialize feature dictionary and compute basic volume/recency metrics
    2. Compute lagged window counts (events in previous 30d, 90d, 180d windows)
    3. Compute hierarchy-level aggregates (country, skill, grade level features)
    4. Compute zero-inflation features (consecutive zeros, zero patterns)
    5. Compute seasonal pattern features (month cosine encoding)
    6. Compute skill-level temporal patterns (zero dynamics, seasonality, volatility)
    7. Compute SO GRADE-level aggregates
    8. Add growth trajectory features (classification, CAGR, trend strength, volatility)
    9. Return enriched feature dictionary
    
    Args:
        cutoff: Cutoff date for feature computation
        past_events: List of past event dates before cutoff
        group_keys: Tuple of group key values
        group_by_cols: List of grouping column names
        df_full: Full dataframe for hierarchy aggregates (optional)
        group_df: Group-specific dataframe (optional)
        has_quantity: Whether Quantity column exists
        past_events_np: Numpy array of past events as days since min_date (for vectorization)
        cutoff_days: Cutoff date as days since min_date (for vectorization)
        min_date: Minimum date in dataset (for vectorization)
        lookback_periods: List of lookback periods in days (default: [30])
        skill_events_map: Map of skill cluster to event days array (optional)
        so_grade_events_map: Map of SO GRADE to event days array (optional)
        so_grade_skill_events_map: Map of (SO GRADE, Skill) to event days array (optional)
        trajectory_classifications: Map of group key to trajectory info (optional)
    
    Returns:
        dict: Dictionary of enriched features
    """
    # Step 1: Initialize feature dictionary and compute basic volume/recency metrics
    enriched = {}
    cutoff_days_global = cutoff_days if cutoff_days is not None else ((cutoff - min_date).days if min_date is not None else None)
    
    # Default lookback periods if not provided
    if lookback_periods is None:
        lookback_periods = [30]  # Keep only 30d, removed 90d and 180d due to high correlation with 30d

    # Basic volume and recency metrics
    total_events = len(past_events)
    
    # Use vectorized operations if numpy array is provided for performance
    if past_events_np is not None and cutoff_days is not None:
        days_from_cutoff = cutoff_days - past_events_np
        # Dynamically compute for all lookback periods
        events_last_periods = {period: int(np.sum(days_from_cutoff <= period)) for period in lookback_periods}
        # Extract values in order
        events_last_30d = events_last_periods.get(lookback_periods[0], 0)
    else:
        # Fallback to list comprehension - use lookback_periods
        events_last_30d = len([d for d in past_events if (cutoff - d).days <= lookback_periods[0]])

    # Compute history span for normalization
    if len(past_events) > 0:
        first_event = min(past_events)
        last_event = max(past_events)
        days_since_first = (cutoff - first_event).days
        days_since_last = (cutoff - last_event).days
        history_span = max(days_since_first, 1)
    else:
        days_since_first = 365
        days_since_last = 365
        history_span = 365

    # Step 2: Compute lagged window counts (events in previous windows before cutoff)
    lagged_counts = compute_lagged_window_counts(cutoff, past_events, lookback_periods,
                                                  past_events_np, cutoff_days, min_date)
    enriched.update(lagged_counts)



    # Step 3: Compute hierarchy-level aggregates (country, skill, grade level features)
    if df_full is not None:
        hierarchy_agg = compute_hierarchy_aggregates(df_full, cutoff, group_keys, group_by_cols)
        enriched.update(hierarchy_agg)

        # Relative demand ratios: leaf group vs parent aggregates (borrowing strength)
        group_density = total_events / max(history_span, 1)
    
    else:
        # Defaults if no full dataframe available
        defaults = _get_default_hierarchy_aggregates()
        enriched.update(defaults)


    # Step 4: Compute zero-inflation features (consecutive zeros, zero patterns)
    # Compute window counts for recent periods to analyze zero patterns
    recent_window_counts = []
    if past_events_np is not None and cutoff_days is not None:
        # Vectorized version for performance
        for i in range(min(6, max(1, len(past_events) // 30))):  # Look at last 6 windows (180 days)
            window_end_days = cutoff_days - i * 30
            window_start_days = window_end_days - 30
            window_count = int(np.sum((past_events_np >= window_start_days) & (past_events_np < window_end_days)))
            recent_window_counts.append(window_count)
    else:
        # Fallback version using list comprehension
        for i in range(min(6, max(1, len(past_events) // 30))):  # Look at last 6 windows (180 days)
            window_end = cutoff - timedelta(days=i * 30)
            window_start = window_end - timedelta(days=30)
            window_count = len([d for d in past_events if window_start <= d < window_end])
            recent_window_counts.append(window_count)
    
    # Calculate consecutive zeros for internal use (not as a feature)
    consecutive_zeros = 0
    for wc in recent_window_counts:
        if wc == 0:
            consecutive_zeros += 1
        else:
            break  # Streak broken

    # Step 5: Compute seasonal pattern features (month cosine encoding)
    enriched['cutoff_month_cos'] = np.cos(2 * np.pi * cutoff.month / 12)

    # Step 6: Compute skill-level temporal patterns (zero dynamics, seasonality, volatility)
    skill_cluster_val = None
    if 'Skill Cluster' in group_by_cols:
        skill_idx = group_by_cols.index('Skill Cluster')
        if skill_idx < len(group_keys):
            skill_cluster_val = group_keys[skill_idx]
    skill_events_arr = None
    if skill_events_map is not None and skill_cluster_val in skill_events_map:
        skill_events_arr = skill_events_map[skill_cluster_val]

    def count_recent(arr, days):
        if arr is None or cutoff_days_global is None:
            return 0
        return int(np.sum((arr < cutoff_days_global) & (arr >= cutoff_days_global - days)))

    # Skill-level zero dynamics (analyze zero patterns at skill level)
    skill_zero_windows = []
    if skill_events_arr is not None and cutoff_days_global is not None:
        for i in range(3):
            window_end = cutoff_days_global - (i * 30)
            window_start = window_end - 30
            window_count = int(np.sum((skill_events_arr < window_end) & (skill_events_arr >= window_start)))
            skill_zero_windows.append(window_count)
    skill_zero_rate = 0.0
    if len(skill_zero_windows) > 0:
        skill_zero_rate = sum(1 for wc in skill_zero_windows if wc == 0) / float(len(skill_zero_windows))

    # Skill-level seasonality and volatility (monthly dispersion across all skill events)
    if skill_events_arr is not None and cutoff_days_global is not None and len(skill_events_arr) > 0:
        months = []
        for day_val in skill_events_arr:
            if day_val < cutoff_days_global and min_date is not None:
                event_date = min_date + timedelta(days=int(day_val))
                months.append(event_date.month)
        if len(months) > 0:
            month_counts = np.bincount(np.array(months), minlength=13)[1:]  # ignore zero index
            month_mean = month_counts.mean() if len(month_counts) > 0 else 0.0

    # Step 7: Compute SO GRADE-level aggregates
    so_grade_val = None
    if 'SO GRADE' in group_by_cols:
        grade_idx = group_by_cols.index('SO GRADE')
        if grade_idx < len(group_keys):
            so_grade_val = group_keys[grade_idx]
    so_grade_events_arr = None
    if so_grade_events_map is not None and so_grade_val in so_grade_events_map:
        so_grade_events_arr = so_grade_events_map[so_grade_val]

    if so_grade_events_arr is not None and cutoff_days_global is not None:
        so_grade_mask = so_grade_events_arr < cutoff_days_global
        so_grade_total_events = int(np.sum(so_grade_mask))
        so_grade_events_last_90d = count_recent(so_grade_events_arr, 90)
        if np.any(so_grade_mask):
            first_grade_event = so_grade_events_arr[so_grade_mask].min()
            grade_span = max(cutoff_days_global - first_grade_event, 1)
        else:
            grade_span = 365

    # Enhanced zero patterns (group-level analysis)
    zero_streaks = []
    current_streak = 0
    for wc in recent_window_counts:
        if wc == 0:
            current_streak += 1
        else:
            if current_streak > 0:
                zero_streaks.append(current_streak)
            current_streak = 0
    if current_streak > 0:
        zero_streaks.append(current_streak)

    windows_considered = max(len(recent_window_counts), 1)
    base_recovery = 1.0 - (consecutive_zeros / windows_considered)

    # Correlation of zero patterns between skill and SO GRADE (cross-level feature)
    skill_zero_flags = []
    so_grade_zero_flags = []
    if skill_events_arr is not None and cutoff_days_global is not None:
        for i in range(min(6, max(1, len(past_events) // 30))):
            window_end = cutoff_days_global - (i * 30)
            window_start = window_end - 30
            window_count = int(np.sum((skill_events_arr < window_end) & (skill_events_arr >= window_start)))
            skill_zero_flags.append(1 if window_count == 0 else 0)
    if so_grade_events_arr is not None and cutoff_days_global is not None:
        for i in range(len(skill_zero_flags) or 6):
            window_end = cutoff_days_global - (i * 30)
            window_start = window_end - 30
            window_count = int(np.sum((so_grade_events_arr < window_end) & (so_grade_events_arr >= window_start)))
            so_grade_zero_flags.append(1 if window_count == 0 else 0)

    skill_zero_correlation = 0.0
    if len(skill_zero_flags) > 1 and len(so_grade_zero_flags) > 1:
        min_len = min(len(skill_zero_flags), len(so_grade_zero_flags))
        sz = np.array(skill_zero_flags[:min_len])
        gz = np.array(so_grade_zero_flags[:min_len])
        if np.std(sz) > 1e-6 and np.std(gz) > 1e-6:
            skill_zero_correlation = float(np.corrcoef(sz, gz)[0, 1])

    # Step 8: Add growth trajectory features (classification, CAGR, trend strength, volatility)
    if trajectory_classifications is not None:
        group_key = tuple(group_keys)
        trajectory_info = trajectory_classifications.get(group_key, {})
        
        # Classification label (categorical feature)
        enriched['trajectory_class'] = trajectory_info.get('trajectory_class', 'Insufficient Data')
        
        # Numerical metrics (continuous features)
        enriched['trajectory_cagr'] = float(trajectory_info.get('cagr', 0.0))
        enriched['trajectory_trend_strength'] = float(trajectory_info.get('trend_strength', 0.0))
    else:
        # Default values if trajectory classifications not available
        enriched['trajectory_class'] = 'Insufficient Data'
        enriched['trajectory_cagr'] = 0.0
        enriched['trajectory_trend_strength'] = 0.0

    # Step 9: Add SMA and WMA baseline features (3m, 6m, 12m)
    sma_wma_features = compute_sma_wma_features(cutoff, past_events, months_history=[3, 6, 12])
    enriched.update(sma_wma_features)

    # Step 10: Add growth and trend features (demand growth rates, acceleration, normalized trend slope)
    growth_trend_features = compute_growth_and_trend_features(cutoff, past_events)
    enriched.update(growth_trend_features)

    # Step 11: Return enriched feature dictionary
    return enriched


def build_count_forecasting_dataset(df, group_by_cols, months_ahead=[0, 1], date_format='%d/%m/%y', additional_features=None, quarter_growth_map=None, training_file=None, no_uplf=False):
    """
    Build leaf-level count forecasting dataset by rolling cutoffs through training history.

    Step-by-step process:
    1. Initialize lookback periods
    2. Calculate stability classifications for skill clusters
    3. Calculate growth trajectory classifications for all groups
    4. Parse and validate date columns
    5. Pre-compute skill and SO GRADE level event histories for cross-level features
    6. Generate rolling cutoff dates (monthly intervals through training history)
    7. For each group:
       a. Sort group data chronologically
       b. Convert dates to numpy arrays for vectorized operations
       c. Pre-compute additional feature values (doesn't change with cutoff)
       d. For each cutoff date:
          - Filter past events before cutoff
          - Compute basic recency/volume features
          - Compute enriched features (lagged counts, trends, hierarchy aggregates)
          - For each month ahead:
             * Calculate target month boundaries
             * Count events in target calendar month
             * Compute month-specific features
             * Create training sample with all features and target
    8. Convert samples to DataFrame and sort by cutoff date
    9. Post-processing validation (check for constant/correlated features)
    10. Return final training dataset

    For each group (RLC: Country, SO GRADE, Skill Cluster), creates training samples at different
    cutoff dates, engineers recency/volume features, and attaches true calendar month counts as targets.

    Supports both ISO format (YYYY-MM-DD) and custom format dates.
    
    Args:
        df: Training dataframe with dates and grouping columns
        group_by_cols: List of columns to group by
        months_ahead: List of months ahead to predict (default: [0, 1])
        date_format: Date format string for parsing (default: '%d/%m/%y')
        additional_features: List of column names to include as features (NOT grouping columns)
        quarter_growth_map: Map of (country, year, month) to quarter growth percentage
        training_file: Deprecated parameter, kept for backward compatibility
        no_uplf: If True, skip UPLF-related features
    
    Returns:
        pl.DataFrame: Training dataset with features and target_count column
    """
    if additional_features is None:
        additional_features = []
    
    # Step 1: Initialize lookback periods
    # Lookback periods define the trailing windows (in calendar days) used for
    # computing lagged count features.  Only 30 days is retained here; 90 d and
    # 180 d windows were removed after SHAP analysis showed they were highly
    # correlated with the 30 d window and contributed near-zero marginal importance.
    lookback_periods = [30]  # Single 30-day window kept; 90d/180d removed (high correlation)

    logging.info("Building count forecasting dataset with rolling cutoffs...")
    logging.info(f"  Forecast months ahead: {months_ahead}")
    logging.info(f"  Lookback periods for features: {lookback_periods}")
    logging.info(f"  Additional feature columns: {additional_features}")
    if quarter_growth_map:
        logging.info("  Quarter growth map provided: will include country_quarter_growth_cutoff feature")
    else:
        logging.info("  No growth map provided: quarter growth features will not be included")

    # Step 2: Calculate growth trajectory classifications for all groups
    logging.info("  Calculating growth trajectory classifications from yearly patterns...")
    trajectory_classifications = calculate_growth_trajectory_classifications(df, group_by_cols)
    if trajectory_classifications:
        logging.info(f"  Calculated growth trajectories for {len(trajectory_classifications)} groups - will include as features")
    else:
        logging.info("  No trajectory classifications calculated - trajectory features will use defaults")

    # Step 3: Parse dates using flexible parsing
    df = df.with_columns([
        parse_date_flexible(pl.col('Requirement Start Date')).alias('Req_Date')
    ]).filter(pl.col('Req_Date').is_not_null())

    # Sort by date for chronological processing
    df = df.sort('Req_Date')
    min_date = df['Req_Date'].min()
    max_date = df['Req_Date'].max()

    logging.info(f"  Date range: {min_date} to {max_date}")

    # Step 4: Pre-compute skill and SO GRADE level histories for cross-level features
    # These maps enable efficient computation of skill/grade-level aggregates during feature engineering
    skill_events_map = {}
    if 'Skill Cluster' in df.columns:
        for skill_key, skill_df in df.group_by('Skill Cluster'):
            dates = skill_df['Req_Date'].to_list()
            if len(dates) == 0:
                continue
            day_offsets = np.array([(d - min_date).days for d in dates])
            skill_events_map[skill_key] = np.sort(day_offsets)

    so_grade_events_map = {}
    if 'SO GRADE' in df.columns:
        for grade_key, grade_df in df.group_by('SO GRADE'):
            dates = grade_df['Req_Date'].to_list()
            if len(dates) == 0:
                continue
            day_offsets = np.array([(d - min_date).days for d in dates])
            so_grade_events_map[grade_key] = np.sort(day_offsets)

    so_grade_skill_events_map = {}
    if 'SO GRADE' in df.columns and 'Skill Cluster' in df.columns:
        for (grade_key, skill_key), combo_df in df.group_by(['SO GRADE', 'Skill Cluster']):
            dates = combo_df['Req_Date'].to_list()
            if len(dates) == 0:
                continue
            day_offsets = np.array([(d - min_date).days for d in dates])
            if grade_key not in so_grade_skill_events_map:
                so_grade_skill_events_map[grade_key] = {}
            so_grade_skill_events_map[grade_key][skill_key] = np.sort(day_offsets)

    # Step 5: Generate cutoff dates
    # For TRAINING datasets we use rolling monthly cutoffs.
    # For TEST datasets we use a SINGLE GLOBAL CUTOFF for all groups to ensure
    # consistent horizons (M0–M1, based on months_ahead parameter) across all groups, as required for evaluation.
    cutoff_dates = []
    
    # Check if this is a test dataset (by checking if training_file path contains "test")
    is_test_dataset = 'test' in str(training_file).lower() if training_file else False
    
    if is_test_dataset:
        # ======================================================================
        # TEST DATASET LOGIC (NO ROLLING CUTOFFS)
        # ======================================================================
        # Data split configuration (see data_split.py) defines:
        #   Normal:      test_start = 2025-05-01, actual_test_start = 2025-07-01
        #   Publishing:  test_start = 2025-11-01, actual_test_start = 2026-01-01
        # For evaluation we want a SINGLE cutoff (derived from --publishing flag) for ALL groups,
        # and from that cutoff we predict M0–M5 (based on months_ahead parameter).
        # This guarantees every group has predictions for all target months,
        # even if its historical demand is zero up to the cutoff.
        single_cutoff = _FORECAST_CUTOFF
        
        # Safety: clamp cutoff into the available date range if needed
        if single_cutoff < min_date:
            logging.warning(f"  Requested test cutoff {single_cutoff} is before min_date {min_date}; using min_date instead.")
            single_cutoff = min_date
        _publishing_mode = globals().get('_IS_PUBLISHING', False)
        if single_cutoff > max_date and not _publishing_mode:
            # In publishing mode the test parquet only contains historical feature data
            # (Nov-Dec 2025 buffer) — the cutoff (2026-01-01) is intentionally AFTER max_date.
            # Clamping here would shift M0 to December 2025 instead of January 2026.
            logging.warning(f"  Requested test cutoff {single_cutoff} is after max_date {max_date}; using max_date instead.")
            single_cutoff = max_date
        elif single_cutoff > max_date and _publishing_mode:
            logging.info(f"  Publishing mode: keeping cutoff {single_cutoff} even though it exceeds max_date {max_date} (feature buffer data only)")
        
        cutoff_dates = [single_cutoff]
        logging.info(f"  Test dataset detected: using SINGLE global cutoff date {single_cutoff} for all groups (no rolling cutoffs)")
    else:
        # ======================================================================
        # TRAINING DATASET LOGIC (ROLLING CUTOFFS)
        # ======================================================================
        # For training dataset: Start 60 days after first event (minimum history)
        current_date = min_date + timedelta(days=60)
        logging.info(f"  Training dataset: Starting cutoff dates from {current_date}")
        
        # Calculate the latest possible cutoff.
        # We allow cutoffs very close to max_date because target months that fall
        # beyond the data boundary are individually skipped in the sample loop below.
        # This lets us include a June cutoff when training ends June 30, gaining
        # features computed from May activity (closer to test feature distribution).
        min_history_needed = 1
        
        while current_date <= max_date - timedelta(days=min_history_needed):
            cutoff_dates.append(current_date)
            current_date += relativedelta(months=1)  # Monthly cutoffs for rolling window approach
        
        logging.info(f"  Generated {len(cutoff_dates)} cutoff dates")
        if len(cutoff_dates) > 0:
            logging.info(f"  Cutoff date range: {cutoff_dates[0]} to {cutoff_dates[-1]}")

    # Step 6: Process each group to create training samples
    # Group data by the specified columns
    grouped = list(df.group_by(group_by_cols))
    all_samples = []

    total_groups = len(grouped)
    group_count = 0
    skipped_beyond_boundary = 0  # Tracks samples skipped because target month exceeds data range

    # Pre-compute cutoff dates as numpy array for faster comparison (vectorization)
    cutoff_dates_np = np.array([(d - min_date).days for d in cutoff_dates])

    for group_keys, group_df in tqdm(grouped, total=total_groups, desc="Building training groups", unit="group"):
        group_count += 1
        group_name = "_".join(str(k) for k in group_keys) if len(group_keys) > 1 else str(group_keys[0])

        # Step 7a: Sort group chronologically (do once per group)
        group_df = group_df.sort('Req_Date')
        group_dates = group_df['Req_Date'].to_list()
    
        # For training we keep the existing minimum history requirement to avoid
        # very sparse groups. For TEST, we must keep ALL groups (even with zero
        # or very few historical events) so that every group receives predictions for all target months.
        if (not is_test_dataset) and len(group_dates) < 3:
            continue

        # Step 7b: Convert group dates to days since min_date for faster comparison (vectorization)
        group_dates_np = np.array([(d - min_date).days for d in group_dates])
        
        # Step 7c: Pre-compute additional feature values once per group (doesn't change with cutoff)
        additional_feature_values = {}
        for feat_col in additional_features:
            unique_values = group_df[feat_col].unique().drop_nulls()
            if len(unique_values) > 0:
                if len(unique_values) == 1:
                    additional_feature_values[feat_col] = str(unique_values[0])
                else:
                    value_counts = group_df[feat_col].value_counts().sort('count', descending=True)
                    most_common = value_counts[feat_col][0]
                    additional_feature_values[feat_col] = str(most_common) if most_common is not None else 'MISSING'
            else:
                additional_feature_values[feat_col] = 'MISSING'
        
        has_quantity = 'Quantity' in group_df.columns

        # Resolve country for this group once (if present in group_by_cols) for quarter growth features
        country_val = None
        if 'Country' in group_by_cols:
            country_idx = group_by_cols.index('Country')
            if country_idx < len(group_keys):
                country_val = group_keys[country_idx]

        # Step 7d: For each cutoff date, create training samples
        for cutoff_idx, cutoff in enumerate(cutoff_dates):
            # Find events before cutoff using vectorized comparison (much faster than list comprehension)
            cutoff_days = (cutoff - min_date).days
            past_mask = group_dates_np < cutoff_days
            past_indices = np.where(past_mask)[0]
            
            # For training we require minimum history per cutoff; for TEST we allow
            # even groups with zero history so that they still get predictions.
            if (not is_test_dataset) and len(past_indices) < 3:
                continue
            
            # Get past events (use indices for faster access)
            if len(past_indices) == 0:
                # No history before cutoff for this group (common in test for groups
                # whose first actuals occur after the global cutoff). We still need
                # to create samples with defaulted recency/history features.
                past_events = []
                past_events_np = np.array([], dtype=int)
                total_events = 0
                days_from_cutoff = np.array([], dtype=int)
                # Use a large sentinel to indicate "no recent event"
                days_since_last = 365
                days_since_first_event = 365
                history_span_days = 365
            else:
                past_events = [group_dates[i] for i in past_indices]
                past_events_np = group_dates_np[past_indices]
    
                # Engineer basic recency/volume features using vectorized operations
                last_event_idx = past_indices[-1]
                last_event = group_dates[last_event_idx]
                days_since_last = cutoff_days - past_events_np[-1]
    
                # Volume features using vectorized date differences
                total_events = len(past_events)
                days_from_cutoff = cutoff_days - past_events_np
                # Dynamically compute events for each lookback period
                events_last_periods = {}
                for period in lookback_periods:
                    events_last_periods[period] = int(np.sum(days_from_cutoff <= period))
                
                # Map to standard variable names (for feature engineering code below)
                events_last_30d = events_last_periods.get(lookback_periods[0], 0)
                
                first_event_idx = past_indices[0]
                first_event = group_dates[first_event_idx]
                days_since_first_event = cutoff_days - past_events_np[0]
                history_span_days = max(days_since_first_event, 1)

            # Basic seasonal features
            seasonal_features = {
                'cutoff_month': cutoff.month,
            }

            # Calculate momentum_30_vs_90: ratio of 30-day to 90-day event rates (recent vs medium-term trend)
            count_30d = int(np.sum(days_from_cutoff <= 30))
            count_90d = int(np.sum(days_from_cutoff <= 90))
            rate_30d = (count_30d / 30.0) * 365.0 if count_30d > 0 else 0.0
            rate_90d = (count_90d / 90.0) * 365.0 if count_90d > 0 else 0.0
            
            if rate_90d > 0:
                momentum_30_vs_90 = rate_30d / rate_90d
            else:
                momentum_30_vs_90 = 1.0 if rate_30d > 0 else 0.0

            recency_features = {
                'momentum_30_vs_90': momentum_30_vs_90
            }

            # Compute enriched features (lagged counts, trends, calendar, hierarchy aggregates, trajectories)
            enriched_features = compute_enriched_features(
                cutoff=cutoff,
                past_events=past_events,
                group_keys=group_keys,
                group_by_cols=group_by_cols,
                df_full=df,
                group_df=group_df,
                has_quantity=has_quantity,
                past_events_np=past_events_np,
                cutoff_days=cutoff_days,
                min_date=min_date,
                lookback_periods=lookback_periods,
                skill_events_map=skill_events_map,
                so_grade_events_map=so_grade_events_map,
                so_grade_skill_events_map=so_grade_skill_events_map,
                trajectory_classifications=trajectory_classifications
            )

            # ================================================================
            # PRE-COMPUTE BRIDGE PERIOD DATA (once per cutoff, shared by all months)
            # ================================================================
            # Bridge period = M0, M1, M2 months after cutoff. For M3-M5 horizons,
            # these bridge counts provide the model with recent demand information
            # that would otherwise be missing (the pre-cutoff features become stale).
            # During training: actual counts.  During test: placeholders overwritten
            # by Phase 1 predictions at inference time.

            def _to_date_safe(d):
                """Normalize a date-like value to a plain date object."""
                if isinstance(d, datetime):
                    return d.date()
                elif hasattr(d, 'date'):
                    return d.date()
                return d

            # Actual counts for the 2 bridge months (M0, M1 from cutoff)
            bridge_month_counts = []
            for b_offset in range(2):
                b_year = cutoff.year
                b_month = cutoff.month + b_offset
                while b_month > 12:
                    b_year += 1
                    b_month -= 12
                b_start = datetime(b_year, b_month, 1).date()
                if b_month == 12:
                    b_end = datetime(b_year + 1, 1, 1).date()
                else:
                    b_end = datetime(b_year, b_month + 1, 1).date()
                b_count = len([d for d in group_dates if b_start <= _to_date_safe(d) < b_end])
                bridge_month_counts.append(b_count)

            # 6 complete calendar months immediately before cutoff (chronological order)
            pre_cutoff_monthly = []
            for pre_offset in range(6):
                p_year = cutoff.year
                p_month = cutoff.month - pre_offset - 1
                while p_month < 1:
                    p_year -= 1
                    p_month += 12
                p_start = datetime(p_year, p_month, 1).date()
                if p_month == 12:
                    p_end = datetime(p_year + 1, 1, 1).date()
                else:
                    p_end = datetime(p_year, p_month + 1, 1).date()
                p_count = len([d for d in group_dates if p_start <= _to_date_safe(d) < p_end])
                pre_cutoff_monthly.append(p_count)
            pre_cutoff_monthly = list(reversed(pre_cutoff_monthly))  # chronological order

            # Step 7e: Create one sample per month ahead (long format) with month_ahead as a feature
            for months in months_ahead:
                # Calculate the target month boundaries
                target_year = cutoff.year
                target_month = cutoff.month + months

                # Handle year rollover
                while target_month > 12:
                    target_year += 1
                    target_month -= 12

                # Get first and last day of target month
                month_start = datetime(target_year, target_month, 1).date()
                # Get last day of month (28-31)
                if target_month == 12:
                    month_end = datetime(target_year + 1, 1, 1).date()
                else:
                    month_end = datetime(target_year, target_month + 1, 1).date()

                # Skip target months that extend beyond the training data boundary.
                # Without this check, recent cutoffs produce samples where the target
                # month has no events (data hasn't been collected yet), creating
                # misleading zero-target rows that bias the model downward.
                if (not is_test_dataset) and month_start > max_date:
                    skipped_beyond_boundary += 1
                    continue

                # Count events in this calendar month
                def to_date(d):
                    if isinstance(d, datetime):
                        return d.date()
                    elif hasattr(d, 'date'):
                        return d.date()
                    else:
                        return d

                # Count events in target calendar month (this is the target variable)
                count = len([d for d in group_dates if month_start <= to_date(d) < month_end])

                # ====================================================================
                # MONTH-SPECIFIC FEATURES TO IMPROVE MONTH-WISE PATTERN LEARNING
                # ====================================================================
                # These features help the model learn month-specific patterns and seasonality
                # by providing information about the target month being predicted
                
                # Step 1: TARGET MONTH FEATURES (the actual month being predicted)
                target_quarter = ((target_month - 1) // 3) + 1  # Calendar quarter 1-4
                target_fiscal_month = ((target_month - 4) % 12) + 1  # Fiscal month (starts April)
                target_fiscal_quarter = ((target_fiscal_month - 1) // 3) + 1  # Fiscal quarter 1-4

                target_month_features = {
                    'target_month': target_month,  # 1-12
                    # Removed target_fiscal_quarter due to high correlation with target_month
                    'target_is_quarter_end': 1 if target_month in [3, 6, 9, 12] else 0,

                    'target_is_fiscal_year_end': 1 if target_fiscal_month == 12 else 0,
                    'target_days_in_month': (month_end - month_start).days,  # 28-31
                }
                
                # Step 2: MONTH-SPECIFIC HISTORICAL PATTERNS
                # Calculate average events in this specific month historically (year-over-year patterns)
                month_specific_historical = {}
                if len(past_events) > 0:
                    # Count events in same month across all years before cutoff
                    same_month_counts = []
                    for event_date in past_events:
                        if event_date.month == target_month and event_date < cutoff:
                            same_month_counts.append(event_date)
                    
                    # Calculate years of history for volatility calculations
                    first_event = min(past_events)
                    years_of_history = max(1, (cutoff.year - first_event.year) + (1 if cutoff.month >= first_event.month else 0))
                    
                    # Events in same month, previous year
                    prev_year = target_year - 1
                    prev_year_month_start = datetime(prev_year, target_month, 1).date()
                    if target_month == 12:
                        prev_year_month_end = datetime(prev_year + 1, 1, 1).date()
                    else:
                        prev_year_month_end = datetime(prev_year, target_month + 1, 1).date()
                    
                    prev_year_count = len([d for d in past_events if prev_year_month_start <= to_date(d) < prev_year_month_end])
                    # Removed events_same_month_last_year due to high correlation with target_count and event features

                    # Note: events_same_month_2y_ago removed - minimal SHAP importance (0.03%)
                    # Note: target_month_vs_avg_ratio removed - zero SHAP importance

                    # Step 2a: MONTH-SPECIFIC VOLATILITY FEATURES
                    # Calculate volatility across years for this specific month (consistency of demand in this month)
                    if years_of_history >= 2:
                        yearly_counts = []
                        for year_offset in range(years_of_history):
                            year = cutoff.year - year_offset - 1  # Start from last year backwards
                            if year >= first_event.year:
                                year_month_start = datetime(year, target_month, 1).date()
                                if target_month == 12:
                                    year_month_end = datetime(year + 1, 1, 1).date()
                                else:
                                    year_month_end = datetime(year, target_month + 1, 1).date()
                                year_count = len([d for d in past_events if year_month_start <= to_date(d) < year_month_end])
                                # Ensure year_count is always an integer
                                if not isinstance(year_count, int):
                                    year_count = 0
                                yearly_counts.append(year_count)

                        if len(yearly_counts) >= 2:
                            # Filter out any non-numeric values and ensure all are integers
                            yearly_counts_clean = []
                            for x in yearly_counts:
                                if isinstance(x, (int, float, np.integer, np.floating)):
                                    yearly_counts_clean.append(float(x))
                                else:
                                    # If it's a list or other iterable, try to get its length or skip
                                    yearly_counts_clean.append(0)

                else:
                    month_specific_historical = {
                        # Removed events_same_month_last_year, target_month_cv, target_month_stability for consistency
                    }
                
                # Step 3: MONTH TRANSITION FEATURES - REMOVED (zero SHAP importance)
                month_transition_features = {}

                # Step 4: ENHANCED WINDOW_START FEATURES (make it more informative)
                # Categorize forecast horizon to help model learn different patterns for different horizons
                window_start_features = {
                    'window_start': months,  # CRITICAL: Keep this for Excel aggregation in compare_models.py
                    'window_start_category': 'immediate' if months == 0 else ('short' if months <= 2 else ('medium' if months <= 4 else 'long')),  # Categorical
                }
                
                
                # Step 5: TARGET MONTH vs CUTOFF MONTH FEATURES
                month_comparison_features = {
                    # Note: months_between_cutoff_and_target removed - duplicate of window_start
                }

                # Step 5a: PER-HORIZON FEATURES (vary by target month and window_start)
                # These give the model signals that differ across horizons for the same cutoff,
                # addressing the problem of identical features across window_start values.
                horizon_features = {
                    # Continuous temporal distance from cutoff to the target month start
                    'days_to_target_start': (month_start - cutoff).days,
                    # Distance from the last observed event to the target month, combining
                    # group-level recency with horizon distance
                    'target_distance_from_last_event': (month_start - cutoff).days + int(days_since_last),
                }

                # YoY growth rate for the specific target calendar month
                # Compares same-month counts across consecutive years to capture calendar-specific trends
                two_years_ago_year = target_year - 2
                try:
                    two_years_ago_start = datetime(two_years_ago_year, target_month, 1).date()
                    if target_month == 12:
                        two_years_ago_end = datetime(two_years_ago_year + 1, 1, 1).date()
                    else:
                        two_years_ago_end = datetime(two_years_ago_year, target_month + 1, 1).date()
                    two_years_ago_count = len([d for d in past_events if two_years_ago_start <= to_date(d) < two_years_ago_end])
                except ValueError:
                    two_years_ago_count = 0

                one_year_ago_year = target_year - 1
                try:
                    one_year_ago_start = datetime(one_year_ago_year, target_month, 1).date()
                    if target_month == 12:
                        one_year_ago_end = datetime(one_year_ago_year + 1, 1, 1).date()
                    else:
                        one_year_ago_end = datetime(one_year_ago_year, target_month + 1, 1).date()
                    one_year_ago_count = len([d for d in past_events if one_year_ago_start <= to_date(d) < one_year_ago_end])
                except ValueError:
                    one_year_ago_count = 0

                # Require minimum 5 events in the comparison month to avoid noisy percentages
                if two_years_ago_count >= 5:
                    yoy_rate = (one_year_ago_count - two_years_ago_count) / two_years_ago_count * 100.0
                    yoy_rate = min(max(yoy_rate, -300.0), 300.0)
                else:
                    yoy_rate = 0.0

                horizon_features['yoy_growth_rate'] = float(yoy_rate)

                # Step 6: COUNTRY-LEVEL GROWTH FEATURES (from external growth_by_country_uplf_*)
                # Only add quarter growth features if quarter_growth_map is provided
                # This captures country-level economic/business trends
                quarter_growth_features = {}
                if quarter_growth_map is not None:
                    quarter_growth_cutoff = get_quarter_growth_value(quarter_growth_map, country_val, cutoff.year, cutoff.month)
                    quarter_growth_features['country_quarter_growth_cutoff'] = quarter_growth_cutoff
                    

                # Step 7: FTE FEATURES (from group-level aggregated UPLF history)
                # Only add FTE features if UPLF features are enabled
                # These capture planned vs actual FTE trends at the group level
                fte_features = {}
                if not no_uplf:
                    fte_cols = [c for c in group_df.columns if c.startswith('fte_')]
                    if fte_cols and len(group_df) > 0:
                        # FTE features are the same for all rows in a group (aggregated from UPLF)
                        first_row = group_df.head(1)
                        for col in fte_cols:
                            fte_features[col] = first_row[col][0]

                # ================================================================
                # Step 7a: BRIDGE FEATURES for two-phase forecasting
                # ================================================================
                # For M3-M5 horizons the pre-cutoff SMA/WMA/growth features are
                # 3-5 months stale relative to the target.  Bridge features capture
                # recent demand in the M0-M2 gap so the model has a fresh signal.
                #
                # Training M3+: computed from *actual* M0-M2 event counts.
                # M0-M2 (train or test) & test M3+: set equal to the pre-cutoff
                # originals as placeholders (test M3+ overwritten at inference time
                # by _two_phase_predict using Phase 1 predictions).
                _EXP_ALPHA = 0.3       # exponential smoothing factor — must match the alpha
                                       # used in compute_sma_wma_features so bridge and
                                       # pre-cutoff WMA features are on the same scale
                _MIN_GROWTH_BASE = 5   # minimum events in the comparison window before a
                                       # bridge growth % is computed (same gate as
                                       # _MIN_EVENTS_FOR_GROWTH in compute_growth_and_trend_features)
                bridge_features = {}

                if months >= 2 and not is_test_dataset:
                    # --- actual bridge counts already pre-computed above ---
                    bc = bridge_month_counts  # [M0_count, M1_count]
                    pc3 = pre_cutoff_monthly[-3:]  # last 3 pre-cutoff months (chrono)

                    # Bridge SMA / WMA 3m (just the bridge period: M0, M1)
                    bridge_features['bridge_sma_3m'] = max(0.0, float(np.mean(bc)))
                    w3 = np.power(1 - _EXP_ALPHA, np.arange(2))[::-1]
                    w3 = w3 / w3.sum()
                    bridge_features['bridge_wma_3m'] = max(0.0, float(np.sum(w3 * np.array(bc))))

                    # Bridge SMA / WMA 6m (pre-cutoff 3m + bridge 2m = 5m window)
                    ext6 = list(pc3) + list(bc)
                    bridge_features['bridge_sma_6m'] = max(0.0, float(np.mean(ext6)))
                    w6 = np.power(1 - _EXP_ALPHA, np.arange(5))[::-1]
                    w6 = w6 / w6.sum()
                    bridge_features['bridge_wma_6m'] = max(0.0, float(np.sum(w6 * np.array(ext6))))

                    # Bridge demand growth 3m: bridge period vs pre-cutoff 3m
                    sum_bridge = sum(bc)
                    sum_pc3 = sum(pc3)
                    if sum_pc3 >= _MIN_GROWTH_BASE:
                        raw_g = (sum_bridge - sum_pc3) / sum_pc3 * 100.0
                        bridge_features['bridge_demand_growth_3m'] = float(min(max(raw_g, -300.0), 300.0))
                    else:
                        bridge_features['bridge_demand_growth_3m'] = 0.0

                    # Bridge recent-vs-historical ratio (bridge avg / pre-cutoff 6m avg)
                    mean_bridge = np.mean(bc)
                    mean_pc6 = np.mean(pre_cutoff_monthly) if len(pre_cutoff_monthly) > 0 else 0.1
                    ratio_bh = mean_bridge / max(mean_pc6, 0.1)
                    bridge_features['bridge_recent_vs_hist'] = float(min(max(ratio_bh, 0.2), 5.0))

                    # Bridge trend slope 6m: normalized linear slope across ext6 (now 5m window)
                    ext6_arr = np.array(ext6, dtype=float)
                    x_t = np.arange(5, dtype=float)
                    mean_ext = np.mean(ext6_arr)
                    if mean_ext >= 1.0 and np.std(ext6_arr) > 1e-6:
                        n_t = 5
                        num_t = n_t * np.sum(x_t * ext6_arr) - np.sum(x_t) * np.sum(ext6_arr)
                        den_t = n_t * np.sum(x_t ** 2) - np.sum(x_t) ** 2
                        slope_t = num_t / max(den_t, 1e-10)
                        norm_slope_t = slope_t / max(mean_ext, 1.0)
                        bridge_features['bridge_trend_slope_6m'] = float(min(max(norm_slope_t, -2.0), 2.0))
                    else:
                        bridge_features['bridge_trend_slope_6m'] = 0.0

                    # Bridge momentum: M1 relative to bridge average (acceleration signal)
                    avg_bc = np.mean(bc)
                    bridge_features['bridge_momentum'] = float(bc[1] / avg_bc) if avg_bc > 0 else 1.0

                    # Raw bridge counts (Change 2: expose individual month counts as features)
                    bridge_features['bridge_m0_count'] = float(bc[0])
                    bridge_features['bridge_m1_count'] = float(bc[1])

                else:
                    # M0-M2 (train & test) or test M3+: mirror the pre-cutoff originals.
                    # Test M3+ values are placeholders -- overwritten by _two_phase_predict.
                    bridge_features['bridge_sma_3m'] = enriched_features.get('sma_3m', 0.0)
                    bridge_features['bridge_wma_3m'] = enriched_features.get('wma_3m', 0.0)
                    bridge_features['bridge_sma_6m'] = enriched_features.get('sma_6m', 0.0)
                    bridge_features['bridge_wma_6m'] = enriched_features.get('wma_6m', 0.0)
                    bridge_features['bridge_demand_growth_3m'] = enriched_features.get('demand_growth_3m', 0.0)
                    bridge_features['bridge_recent_vs_hist'] = enriched_features.get('recent_vs_historical_ratio', 1.0)
                    bridge_features['bridge_trend_slope_6m'] = enriched_features.get('trend_slope_6m', 0.0)
                    bridge_features['bridge_momentum'] = recency_features.get('momentum_30_vs_90', 1.0)
                    # Raw bridge count placeholders (test rows overwritten by Phase 1 predictions)
                    bridge_features['bridge_m0_count'] = 0.0
                    bridge_features['bridge_m1_count'] = 0.0

                # Step 7f: Create training sample with all features and target
                sample = {
                    'cutoff_date': cutoff,
                    'days_since_last_event': days_since_last,
                    **seasonal_features,
                    **recency_features,
                    **enriched_features,  # Add all enriched features
                    **fte_features,  # FTE-based features from UPLF history
                    **target_month_features,  # Target month identity
                    **month_specific_historical,  # Month-specific historical patterns
                    **month_transition_features,  # Month-to-month transitions
                    **window_start_features,  # Enhanced window_start features
                    **month_comparison_features,  # Target vs cutoff month comparison
                    **horizon_features,  # Per-horizon decay and YoY growth features
                    **bridge_features,  # Two-phase bridge features (recent demand for M3-M5)
                    **quarter_growth_features,  # Quarter growth features (only if quarter_growth_map provided)
                    **{col: key for col, key in zip(group_by_cols, group_keys)},
                    **{col: str(val) for col, val in additional_feature_values.items()},  # Additional features as strings
                    'target_count': count  # Single target column
                }
                all_samples.append(sample)

    # Step 8: Convert samples to DataFrame and sort by cutoff date
    dataset_df = pl.DataFrame(all_samples)

    if len(dataset_df) > 0:
        dataset_df = dataset_df.sort('cutoff_date')

    logging.info(f"  Created {len(dataset_df)} training samples across {group_count} groups")
    if skipped_beyond_boundary > 0:
        logging.info(f"  Skipped {skipped_beyond_boundary} samples where target month exceeded training data boundary (max_date={max_date})")

    # Step 9: Post-processing validation: Check for constant and correlated features
    if len(dataset_df) > 0:
        logging.info("  Post-processing validation: Checking feature quality...")
        
        # Check for constant features (same value across all rows)
        constant_features = []
        numeric_cols = [col for col in dataset_df.columns if dataset_df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8]]
        
        for col in numeric_cols:
            if col in ['target_count']:  # Skip target column
                continue
            unique_count = dataset_df[col].n_unique()
            if unique_count == 1:
                constant_features.append(col)
        
        if constant_features:
            logging.warning(f"  WARNING: Found {len(constant_features)} constant features (same value for all rows):")
            for feat in constant_features:
                sample_val = dataset_df[feat][0]
                logging.warning(f"    - {feat} = {sample_val}")
            logging.warning("  Consider removing these features as they provide no information for the model.")
        
        # Check for highly correlated features (>0.70 correlation)
        logging.info("  Checking for highly correlated features (>0.70)...")
        high_corr_pairs = []
        
        # Convert to pandas for correlation calculation (more efficient)
        numeric_data = dataset_df.select(numeric_cols).to_pandas()
        
        # Calculate correlation matrix
        corr_matrix = numeric_data.corr().abs()
        
        # Find pairs with correlation > 0.70 (excluding diagonal)
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                if corr_val > 0.70:
                    col1 = corr_matrix.columns[i]
                    col2 = corr_matrix.columns[j]
                    high_corr_pairs.append((col1, col2, corr_val))
        
        if high_corr_pairs:
            logging.warning(f"  WARNING: Found {len(high_corr_pairs)} highly correlated feature pairs (>0.70):")
            for col1, col2, corr_val in high_corr_pairs:
                logging.warning(f"    - {col1} <-> {col2}: {corr_val:.4f}")
            logging.warning("  Consider removing one feature from each pair to reduce redundancy.")
        else:
            logging.info("  No highly correlated features found (all pairs < 0.70)")

    # Step 10: Return final training dataset
    return dataset_df


def main(train_file, group_by_cols, additional_features, output_file='data/training_dataset.parquet',
         months_ahead=[0, 1], quarter_growth_file='data/Quarter_growth_by_country_uplf_train.csv',
         no_quarter_growth=False, no_uplf=False, allowed_group_keys=None):
    """
    Build and save training groups dataset.
    
    Step-by-step process:
    1. Log configuration parameters
    2. Handle UPLF feature flags (quarter growth and FTE features)
    3. Load quarter growth map if enabled
    4. Ensure output directory exists
    5. Load processed training data from CSV
    6. Add FTE-based features from UPLF history (if enabled)
    7. Build training dataset with rolling cutoffs and feature engineering
    8. Save dataset in parquet format (efficient)
    9. Save dataset in CSV format (for inspection)
    10. Save summary JSON with statistics
    11. Return output file path
    
    Args:
        train_file: Path to processed training data CSV
        group_by_cols: List of columns to group by
        additional_features: List of additional feature columns
        output_file: Output file path for parquet dataset
        months_ahead: List of months ahead to predict
        quarter_growth_file: Path to quarter growth CSV file
        no_quarter_growth: If True, disable quarter growth features
        no_uplf: If True, disable all UPLF features
        allowed_group_keys: List of group key tuples to include (optional)
    
    Returns:
        str: Path to output file
    """

    logging.info(f"Building training groups from {train_file}...")
    logging.info(f"  Group by: {group_by_cols}")
    logging.info(f"  Additional features: {additional_features}")
    logging.info(f"  Months ahead: {months_ahead}")
    logging.info(f"  Output: {output_file}")
    
    # Step 2: Handle UPLF features: if --no-uplf is set, disable all UPLF features including quarter growth
    if no_uplf:
        logging.info("  UPLF features disabled (--no-uplf flag set): skipping quarter growth and FTE features")
        no_quarter_growth = True
        quarter_growth_file = None
        quarter_growth_map = None
    elif no_quarter_growth:
        logging.info("  Quarter growth features disabled (--no-quarter-growth flag set)")
        quarter_growth_file = None
        quarter_growth_map = None
    else:
        logging.info(f"  Quarter growth file: {quarter_growth_file}")
        # Step 3: Load quarter growth map if enabled
        quarter_growth_map = load_quarter_growth_map(quarter_growth_file)

    # Step 4: Ensure data directory exists
    os.makedirs('data', exist_ok=True)

    # Step 5: Load processed training data
    df_train = pl.read_csv(train_file)
    logging.info(f"Loaded {len(df_train)} training rows")
    
    # Step 5.4: Validate that all group_by_cols exist in the data
    available_columns = set(df_train.columns)
    missing_group_cols = [col for col in group_by_cols if col not in available_columns]
    if missing_group_cols:
        logging.error(f"  ERROR: The following group_by columns are missing from the data: {missing_group_cols}")
        logging.error(f"  Available columns in data: {sorted(available_columns)}")
        logging.error(f"  Requested group_by columns: {group_by_cols}")
        raise ValueError(f"Missing group_by columns in data: {missing_group_cols}. Available columns: {sorted(available_columns)}")
    
    logging.info(f"  Validated group_by columns: {group_by_cols} (all present in data)")

    if allowed_group_keys is not None:
        logging.info(f"  Filtering training data to {len(allowed_group_keys)} eligible groups")
        df_train = _filter_df_by_group_keys(df_train, group_by_cols, allowed_group_keys)
        logging.info(f"  Training rows after group filter: {len(df_train)}")
        if len(df_train) == 0:
            logging.warning("  No training rows after group filter; writing empty dataset.")
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            empty_df = pl.DataFrame([])
            empty_df.write_parquet(output_file)
            csv_output = output_file.replace('.parquet', '.csv') if output_file.endswith('.parquet') else f"{output_file}.csv"
            empty_df.write_csv(csv_output)
            summary = {
                'total_samples': 0,
                'total_features': 0,
                'group_by_cols': group_by_cols,
                'additional_features': additional_features,
                'months_ahead': months_ahead,
                'quarter_growth_file': quarter_growth_file if not no_quarter_growth else None,
                'date_range': {
                    'min_cutoff': None,
                    'max_cutoff': None
                },
                'target_stats': {
                    'mean': None,
                    'median': None,
                    'max': None,
                    'zeros_ratio': None
                }
            }
            with open(output_file.replace('.parquet', '_summary.json'), 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            return output_file

    # Step 5.5: Filter additional_features to only include columns that exist in the data
    # This is important for test datasets that may have had columns removed (e.g., SO Line Status)
    available_columns = set(df_train.columns)
    original_additional_features = additional_features.copy()
    additional_features = [col for col in additional_features if col in available_columns]
    if len(additional_features) < len(original_additional_features):
        removed_features = set(original_additional_features) - set(additional_features)
        logging.info(f"  Filtered additional_features: kept {len(additional_features)}/{len(original_additional_features)} features")
        logging.info(f"  Removed features (not in data): {sorted(removed_features)}")

    # Step 6: Inject FTE-based features from UPLF history before building rolling groups (skip if --no-uplf)
    if not no_uplf:
        df_train = add_fte_features(df_train)
    else:
        logging.info("  Skipping FTE feature engineering (--no-uplf flag set)")

    # Step 7: Build the training dataset (this is the time-consuming part)
    dataset_df = build_count_forecasting_dataset(
        df_train,
        group_by_cols,
        months_ahead=months_ahead,
        additional_features=additional_features,
        quarter_growth_map=quarter_growth_map,
        training_file=train_file,  # Pass train_file to detect test datasets
        no_uplf=no_uplf,
    )

    # Step 8: Save in efficient parquet format
    dataset_df.write_parquet(output_file)
    logging.info(f"Saved training dataset to {output_file} ({len(dataset_df)} samples, {len(dataset_df.columns)} features)")

    # Step 9: Also save a CSV version for easier inspection
    csv_output = output_file.replace('.parquet', '.csv') if output_file.endswith('.parquet') else f"{output_file}.csv"
    dataset_df.write_csv(csv_output)
    logging.info(f"Saved training dataset CSV to {csv_output}")

    # Step 10: Also save a summary with statistics
    summary = {
        'total_samples': len(dataset_df),
        'total_features': len(dataset_df.columns),
        'group_by_cols': group_by_cols,
        'additional_features': additional_features,
        'months_ahead': months_ahead,
        'quarter_growth_file': quarter_growth_file if not no_quarter_growth else None,
        'date_range': {
            'min_cutoff': str(dataset_df['cutoff_date'].min()),
            'max_cutoff': str(dataset_df['cutoff_date'].max())
        },
        'target_stats': {
            'mean': float(dataset_df['target_count'].mean()),
            'median': float(dataset_df['target_count'].median()),
            'max': int(dataset_df['target_count'].max()),
            'zeros_ratio': float((dataset_df['target_count'] == 0).mean())
        }
    }

    with open(output_file.replace('.parquet', '_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    logging.info("Training dataset building completed successfully!")
    # Step 11: Return output file path
    return output_file


if __name__ == "__main__":
    # Default grouping and features
    # DEFAULT_GROUP_BY = ['Country', 'Off/ On', 'SO GRADE', 'Project Billability Type', 'BU', 'Skill Cluster']
    DEFAULT_GROUP_BY = ['BU', 'Skill Cluster']

    DEFAULT_ADDITIONAL_FEATURES = [
        'Vertical', 'Practice', 'SubVertical', 'SubPractice', 'Parent Customer',
        'Account Name', 'Parent Customer', 'Geography', 'Market', 'ServiceLine',
        'Practice Area', 'Project Type', 'SO TYPE', 'Requirement type',
        'Revenue potential', 'SO Line Status', 'City', 'Country', 'SBU1', 'SO GRADE'
    ]

    # -----------------------------------------------------------------------
    # Argument parsing
    # Each argument is described in-line below.  Key design decisions:
    #   --train-file None  -> AUTO-MODE: discovers train_data.csv / test_data.csv
    #                         under the canonical data_<SUFFIX>/ directory and
    #                         processes both in the correct order (test first, then
    #                         train, so that eligible groups are known before training).
    #   --months-ahead     -> controls which forecast horizons are in the dataset.
    #                         M0 = same calendar month as cutoff; M1 = one month later.
    #   --max-groups /
    #   --cv-threshold     -> joint gate: pick the top N stable (CV < threshold) groups
    #                         for individual models; all others use global model.
    #   --no-uplf          -> disables ALL UPLF-derived features (FTE columns +
    #                         quarter growth); useful when UPLF data is unavailable.
    # -----------------------------------------------------------------------
    parser = argparse.ArgumentParser(description='Build training groups dataset for time series forecasting')
    parser.add_argument('--train-file', default=None,
                       help='Path to processed training data CSV file (if not provided, processes both train and test automatically)')
    parser.add_argument('--group-by', nargs='*', default=DEFAULT_GROUP_BY,
                       help='Columns to group by for training groups')
    parser.add_argument('--additional-features', nargs='*', default=DEFAULT_ADDITIONAL_FEATURES,
                       help='Additional feature columns to include')
    parser.add_argument('--months-ahead', type=int, nargs='+', default=[0, 1],
                       help='Number of months ahead to predict (e.g., 0 1). Default: 0 1')
    parser.add_argument('--output-file', default=None,
                       help='Output file path for training dataset (parquet format). If not provided, auto-generates based on input file')
    parser.add_argument('--quarter-growth-file', default='data/Quarter_growth_by_country_uplf_train.csv',
                       help='Path to Quarter_growth_by_country_uplf_train.csv (Country, year, month, quarter_growth_pct_from_sum)')
    parser.add_argument('--no-quarter-growth', action='store_true',
                       help='Disable quarter growth features (enabled by default)')
    parser.add_argument('--no-uplf', action='store_true',
                       help='Disable all UPLF features (quarter growth, FTE features, industry_segment_encoded, customer_classification_encoded, normalized_demand_per_fte)')
    parser.add_argument('--individual-groups', action='store_true', default=False,
                       help='Write per-group train/test datasets into data_<GROUP> directory (requires auto-mode)')
    parser.add_argument('--data-dir', default=None,
                       help='Override data directory (e.g. results_S_m6_per_group/data_S). When set, train/test paths are under this dir; used by run_pipeline when --individual-groups is set.')
    parser.add_argument('--max-groups', type=int, default=10,
                       help='Maximum number of individual group models. Groups beyond this limit (by demand, '
                            'CV < cv-threshold) use the global model only; no separate remainder model. Default: 10')
    parser.add_argument('--cv-threshold', type=float, default=0.5,
                       help='CV (coefficient of variation) threshold for individual group eligibility. '
                            'Groups with CV >= this value are ineligible for individual models. Default: 0.5')
    parser.add_argument('--global-train-all', action='store_true', default=True,
                       help='Also build global training/test parquets from unfiltered data '
                            '(train_data_global.csv / test_data_global.csv) for the global model.')
    parser.add_argument('--publishing', action='store_true', default=False,
                       help='Publishing mode: use Jan 1 2026 as the single test cutoff (M0=Jan 2026) '
                            'instead of the default Jul 1 2025. Must be used together with '
                            'data_split --publishing so the test CSV covers the correct period.')

    args = parser.parse_args()

    # ── Derive the single test cutoff date from publishing flag ───────────────
    # This date is used in three places:
    #   1. build_count_forecasting_dataset – single_cutoff for test dataset
    #   2. Target-month filtering of test parquet (post-build)
    #   3. 90-day recency filter applied to the test CSV
    _FORECAST_CUTOFF = (
        datetime(2026, 1, 1).date() if args.publishing
        else datetime(2025, 7, 1).date()
    )
    _IS_PUBLISHING = args.publishing

    # --- Validate / normalise the --group-by argument ---
    # nargs='*' means argparse returns [] (not the default) when the flag is present
    # with no values, so we need extra guards here.
    logging.info(f"Parsed arguments - group_by: {args.group_by}, type: {type(args.group_by)}")

    # Guard 1: empty list supplied (e.g. `--group-by` with no values)
    if args.group_by == []:
        args.group_by = DEFAULT_GROUP_BY
        logging.warning(f"  --group-by was provided with no values, using default: {args.group_by}")

    # Guard 2: argparse returned None (should not happen with nargs='*' but defensive)
    if args.group_by is None:
        args.group_by = DEFAULT_GROUP_BY
        logging.info(f"  --group-by was None, using default: {args.group_by}")

    # Guard 3: final sanity check — list must be non-empty before proceeding
    if not args.group_by or len(args.group_by) == 0:
        args.group_by = DEFAULT_GROUP_BY
        logging.warning(f"  --group-by was empty, using default: {args.group_by}")

    logging.info(f"Final group_by_cols to use: {args.group_by}")

    # -----------------------------------------------------------------------
    # Execution mode selection
    # -----------------------------------------------------------------------
    # AUTO-MODE  (--train-file not provided): discovers train_data.csv and
    #   test_data.csv under the canonical data_<SUFFIX>/ folder.  Processes
    #   the test dataset first (so eligible groups are known), then the training
    #   dataset filtered to those same groups.  Optionally writes per-group files
    #   (--individual-groups) and global parquets (--global-train-all).
    #
    # SINGLE-FILE MODE  (--train-file provided): processes one CSV directly and
    #   writes the parquet to --output-file (or the default path).
    if args.train_file is None:
        logging.info("="*80)
        logging.info("AUTO-MODE: Processing both train and test datasets")
        logging.info("="*80)

        # TRUE FORECASTING: never include raw business categorical columns in parquets.
        # At inference time only the group identifiers are known; everything else is
        # COMPUTED from historical demand date records (SMA, WMA, growth rates, etc.).
        # Using [] here ensures training_dataset.parquet and test_dataset.parquet have
        # identical feature sets — only demand-history features + group keys.
        # (Quarter-growth from the external macro file is still included via quarter_growth_map.)
        _ADDITIONAL_FEATURES_FOR_BUILD = []

        # Resolve the canonical data directory and file paths.
        # data_dir is overridden by --data-dir when run_pipeline.sh sets up a
        # per-group sub-directory (e.g., results_RLC_m6_per_group/data_RLC/).
        suffix = build_group_suffix(args.group_by)
        data_dir = args.data_dir if args.data_dir else os.path.join(resolve_results_dir(f"results_{suffix}"), f"data_{suffix}")
        train_file = os.path.join(data_dir, 'train_data.csv')
        test_file = os.path.join(data_dir, 'test_data.csv')
        train_output = os.path.join(data_dir, 'training_dataset.parquet')
        test_output = os.path.join(data_dir, 'test_dataset.parquet')
        # Quarter-growth files are split by period; fall back to the train file when
        # the test-specific one has not been generated yet.
        quarter_growth_train = os.path.join(data_dir, 'Quarter_growth_by_country_uplf_train.csv')
        quarter_growth_test = os.path.join(data_dir, 'Quarter_growth_by_country_uplf_test.csv')
        os.makedirs(data_dir, exist_ok=True)
        eligible_group_keys = None  # Populated after test dataset is processed

        # Process test dataset first (before training) so that eligible_group_keys
        # can be derived from the test set and used to filter the training build.
        if os.path.exists(test_file):
            logging.info(f"\n{'='*80}")
            logging.info(f"1. Processing TEST dataset: {test_file}")
            logging.info(f"{'='*80}")
            if not os.path.exists(quarter_growth_test):
                test_quarter_growth_file = args.quarter_growth_file
                logging.info(f"  Using train quarter growth file for test: {test_quarter_growth_file}")
            else:
                test_quarter_growth_file = quarter_growth_test
                logging.info(f"  Using test quarter growth file: {test_quarter_growth_file}")

            main(
                train_file=test_file,
                group_by_cols=args.group_by,
                additional_features=_ADDITIONAL_FEATURES_FOR_BUILD,  # true forecasting: no raw business cols
                months_ahead=args.months_ahead,
                output_file=test_output,
                quarter_growth_file=test_quarter_growth_file,
                no_quarter_growth=args.no_quarter_growth,
                no_uplf=args.no_uplf,
            )

            # FILTER TEST DATASET TO ONLY TARGET MONTHS BASED ON months_ahead
            # Calculate target months from forecast cutoff date (Jul 2025 normal / Jan 2026 publishing) + months_ahead
            cutoff_date = _FORECAST_CUTOFF
            target_months = []
            for months in args.months_ahead:
                target_year = cutoff_date.year
                target_month = cutoff_date.month + months
                while target_month > 12:
                    target_year += 1
                    target_month -= 12
                target_months.append(target_month)
            
            min_target_month = min(target_months)
            max_target_month = max(target_months)
            
            logging.info(f"\nFiltering test dataset to target months {min_target_month}-{max_target_month} (based on months_ahead={args.months_ahead})...")
            test_df = pl.read_parquet(test_output)
            logging.info(f"  Test dataset before filtering: {len(test_df)} samples")

            # Filter to only target months based on months_ahead
            test_df_filtered = test_df.filter(
                (pl.col('target_month') >= min_target_month) &
                (pl.col('target_month') <= max_target_month)
            )

            logging.info(f"  Test dataset after filtering to months {min_target_month}-{max_target_month}: {len(test_df_filtered)} samples")

            # Save filtered test dataset
            test_df_filtered.write_parquet(test_output)
            test_csv_output = test_output.replace('.parquet', '.csv')
            test_df_filtered.write_csv(test_csv_output)

            logging.info(f"  Saved filtered test dataset to: {test_output}")
            logging.info(f"  Saved filtered test dataset CSV to: {test_csv_output}")

            # Update summary with filtered stats
            summary_file = test_output.replace('.parquet', '_summary.json')
            if os.path.exists(summary_file):
                with open(summary_file, 'r') as f:
                    summary = json.load(f)

                summary.update({
                    'filtered_samples': len(test_df_filtered),
                    'filtered_date_range': f'Months {min_target_month}-{max_target_month} {_FORECAST_CUTOFF.year}',
                    'target_months': target_months,
                    'filter_reason': f'Keep only target months {min_target_month}-{max_target_month} based on months_ahead={args.months_ahead}'
                })

                with open(summary_file, 'w') as f:
                    json.dump(summary, f, indent=2, default=str)

                logging.info(f"  Updated summary file: {summary_file}")

        # ======================================================================
        # 90-DAY RECENCY FILTER: only keep groups active in last 90 days before cutoff
        # ======================================================================
        # True forecasting: at inference time we only predict for groups that had
        # at least 1 demand occurrence in the 90 days before the forecast cutoff
        # (2025-07-01).  Dormant / extinct groups are excluded.
        #
        # _TEST_CUTOFF_DATE : global single cutoff used for all test predictions.
        #                     Derived from --publishing flag (2025-07-01 normal / 2026-01-01 publishing).
        #                     Must match the single_cutoff used in build_count_forecasting_dataset.
        # _RECENCY_DAYS     : look-back window for the "active group" gate.
        #                     Increase to be more permissive (keep more groups);
        #                     decrease to be more aggressive (fewer, fresher groups).
        _TEST_CUTOFF_DATE = _FORECAST_CUTOFF
        _RECENCY_DAYS = 90   # days before cutoff that constitute "recent activity"
        _recency_start = _TEST_CUTOFF_DATE - timedelta(days=_RECENCY_DAYS)

        if os.path.exists(test_file) and os.path.exists(test_output):
            logging.info(
                f"\n90-day recency filter (cutoff={_TEST_CUTOFF_DATE}, window={_recency_start} … {_TEST_CUTOFF_DATE}): "
                f"keeping only groups with ≥1 demand event in the last {_RECENCY_DAYS} days."
            )
            try:
                _raw_test = pl.read_csv(test_file)
                # Parse the demand date column
                if 'Requirement Start Date' in _raw_test.columns:
                    _raw_test = _raw_test.with_columns(
                        parse_date_flexible(pl.col('Requirement Start Date')).alias('_Req_Date')
                    ).filter(pl.col('_Req_Date').is_not_null())
                elif 'Req_Date' in _raw_test.columns:
                    _raw_test = _raw_test.rename({'Req_Date': '_Req_Date'})
                else:
                    raise ValueError("Neither 'Requirement Start Date' nor 'Req_Date' found in test CSV.")

                # Events within [recency_start, cutoff)
                _recent = _raw_test.filter(
                    (pl.col('_Req_Date') >= pl.lit(_recency_start)) &
                    (pl.col('_Req_Date') < pl.lit(_TEST_CUTOFF_DATE))
                )

                _missing_group_cols = [c for c in args.group_by if c not in _recent.columns]
                if _missing_group_cols:
                    logging.warning(f"  90-day filter: group columns missing from raw test CSV: {_missing_group_cols}; skipping filter.")
                elif len(_recent) == 0:
                    logging.warning("  90-day filter: no events found in recency window; all groups would be dropped — skipping filter.")
                else:
                    _active_groups = _recent.select(args.group_by).unique()
                    n_active = len(_active_groups)
                    logging.info(f"  Active groups (≥1 event in last {_RECENCY_DAYS} days): {n_active}")

                    _test_pq = pl.read_parquet(test_output)
                    n_before = len(_test_pq)
                    n_groups_before = _test_pq.select(args.group_by).unique().height

                    _test_pq_active = _test_pq.join(_active_groups, on=args.group_by, how='inner')
                    n_after = len(_test_pq_active)
                    n_groups_after = _test_pq_active.select(args.group_by).unique().height

                    logging.info(
                        f"  Test dataset after 90-day recency filter: "
                        f"{n_after} rows / {n_groups_after} groups "
                        f"(was {n_before} rows / {n_groups_before} groups; "
                        f"removed {n_groups_before - n_groups_after} dormant groups)"
                    )
                    _test_pq_active.write_parquet(test_output)
                    _test_pq_active.write_csv(test_output.replace('.parquet', '.csv'))

                    # Update summary with recency filter info
                    _sf = test_output.replace('.parquet', '_summary.json')
                    if os.path.exists(_sf):
                        with open(_sf, 'r') as _f:
                            _s = json.load(_f)
                        _s.update({
                            'recency_filter_days': _RECENCY_DAYS,
                            'recency_filter_cutoff': str(_TEST_CUTOFF_DATE),
                            'recency_filter_start': str(_recency_start),
                            'groups_before_recency_filter': n_groups_before,
                            'groups_after_recency_filter': n_groups_after,
                        })
                        with open(_sf, 'w') as _f:
                            json.dump(_s, _f, indent=2, default=str)
            except Exception as _e:
                logging.warning(f"  90-day recency filter failed (non-fatal): {_e}")

        else:
            logging.warning(f"Test file not found: {test_file}, skipping...")

        skip_training = False
        eligibility_metric = None
        if args.individual_groups and os.path.exists(test_output):
            test_df_for_eligibility = pl.read_parquet(test_output)
            valid_counts, eligibility_metric = _compute_group_eligibility(
                test_df=test_df_for_eligibility,
                group_by_cols=args.group_by
            )
            eligible_group_keys = [
                tuple(row[col] for col in args.group_by)
                for row in valid_counts.iter_rows(named=True)
            ]
            logging.info(f"  Groups in test data: {len(eligible_group_keys)} (metric={eligibility_metric})")

            if len(eligible_group_keys) == 0:
                group_output_dir = data_dir
                logging.warning("  No groups in test data; skipping training dataset build.")
                write_empty_group_manifest(
                    output_dir=group_output_dir,
                    group_by_cols=args.group_by,
                    eligibility_metric=eligibility_metric,
                    eligibility_source=test_output
                )
                skip_training = True

            if not skip_training:
                test_df_filtered = _filter_df_by_group_keys(
                    test_df_for_eligibility,
                    args.group_by,
                    eligible_group_keys
                )
                test_df_filtered.write_parquet(test_output)
                test_csv_output = test_output.replace('.parquet', '.csv')
                test_df_filtered.write_csv(test_csv_output)

                summary_file = test_output.replace('.parquet', '_summary.json')
                if os.path.exists(summary_file):
                    with open(summary_file, 'r') as f:
                        summary = json.load(f)
                    summary.update({
                        'eligible_groups': len(eligible_group_keys),
                        'eligible_samples': len(test_df_filtered),
                        'eligibility_metric': eligibility_metric
                    })
                    with open(summary_file, 'w') as f:
                        json.dump(summary, f, indent=2, default=str)

        # Process training dataset
        if not skip_training and os.path.exists(train_file):
            logging.info(f"\n{'='*80}")
            logging.info(f"2. Processing TRAINING dataset: {train_file}")
            logging.info(f"{'='*80}")
            quarter_train_for_build = quarter_growth_train if os.path.exists(quarter_growth_train) else args.quarter_growth_file
            main(
                train_file=train_file,
                group_by_cols=args.group_by,
                additional_features=_ADDITIONAL_FEATURES_FOR_BUILD,  # true forecasting: no raw business cols
                months_ahead=args.months_ahead,
                output_file=train_output,
                quarter_growth_file=quarter_train_for_build,
                no_quarter_growth=args.no_quarter_growth,
                no_uplf=args.no_uplf,
                allowed_group_keys=eligible_group_keys
            )
        elif not skip_training:
            logging.warning(f"Training file not found: {train_file}, skipping...")

        if not skip_training and args.individual_groups and os.path.exists(train_output) and os.path.exists(test_output):
            group_output_dir = data_dir
            logging.info(f"\nBuilding per-group datasets in {group_output_dir}...")
            write_individual_group_datasets(
                train_dataset_path=train_output,
                test_dataset_path=test_output,
                group_by_cols=args.group_by,
                output_dir=group_output_dir,
                max_groups=args.max_groups,
                cv_threshold=args.cv_threshold,
                raw_train_csv_path=train_file,
            )

        # Build global parquets from unfiltered data when --global-train-all is set
        if args.global_train_all:
            global_train_csv = os.path.join(data_dir, 'train_data_global.csv')
            global_test_csv = os.path.join(data_dir, 'test_data_global.csv')
            global_train_output = os.path.join(data_dir, 'training_dataset_global.parquet')
            global_test_output = os.path.join(data_dir, 'test_dataset_global.parquet')

            if os.path.exists(global_train_csv):
                logging.info(f"\n{'='*80}")
                logging.info("Processing GLOBAL TRAINING dataset (all data, no demand filter)")
                logging.info(f"{'='*80}")
                quarter_train_for_build = quarter_growth_train if os.path.exists(quarter_growth_train) else args.quarter_growth_file
                main(
                    train_file=global_train_csv,
                    group_by_cols=args.group_by,
                    additional_features=_ADDITIONAL_FEATURES_FOR_BUILD,  # true forecasting: no raw business cols
                    months_ahead=args.months_ahead,
                    output_file=global_train_output,
                    quarter_growth_file=quarter_train_for_build,
                    no_quarter_growth=args.no_quarter_growth,
                    no_uplf=args.no_uplf,
                )
            else:
                logging.warning(f"  Global train CSV not found: {global_train_csv}. "
                                "Run data_split.py with --global-train-all first.")

            if os.path.exists(global_test_csv):
                logging.info(f"\n{'='*80}")
                logging.info("Processing GLOBAL TEST dataset (all data, no demand filter)")
                logging.info(f"{'='*80}")
                if not os.path.exists(quarter_growth_test):
                    test_quarter_growth_file = args.quarter_growth_file
                else:
                    test_quarter_growth_file = quarter_growth_test
                main(
                    train_file=global_test_csv,
                    group_by_cols=args.group_by,
                    additional_features=_ADDITIONAL_FEATURES_FOR_BUILD,  # true forecasting: no raw business cols
                    months_ahead=args.months_ahead,
                    output_file=global_test_output,
                    quarter_growth_file=test_quarter_growth_file,
                    no_quarter_growth=args.no_quarter_growth,
                    no_uplf=args.no_uplf,
                )

                # Apply same target month filtering to global test dataset
                cutoff_date_g = _FORECAST_CUTOFF
                target_months_g = []
                for months in args.months_ahead:
                    target_year_g = cutoff_date_g.year
                    target_month_g = cutoff_date_g.month + months
                    while target_month_g > 12:
                        target_year_g += 1
                        target_month_g -= 12
                    target_months_g.append(target_month_g)

                min_target_month_g = min(target_months_g)
                max_target_month_g = max(target_months_g)

                logging.info(f"\nFiltering global test dataset to target months {min_target_month_g}-{max_target_month_g}...")
                global_test_df = pl.read_parquet(global_test_output)
                global_test_df_filtered = global_test_df.filter(
                    (pl.col('target_month') >= min_target_month_g) &
                    (pl.col('target_month') <= max_target_month_g)
                )
                global_test_df_filtered.write_parquet(global_test_output)
                logging.info(f"  Global test dataset: {len(global_test_df)} -> {len(global_test_df_filtered)} rows after month filter")

                # Apply 90-day recency filter to global test dataset (same logic as per-group test)
                if os.path.exists(global_test_csv):
                    try:
                        _raw_g = pl.read_csv(global_test_csv)
                        if 'Requirement Start Date' in _raw_g.columns:
                            _raw_g = _raw_g.with_columns(
                                parse_date_flexible(pl.col('Requirement Start Date')).alias('_Req_Date')
                            ).filter(pl.col('_Req_Date').is_not_null())
                        elif 'Req_Date' in _raw_g.columns:
                            _raw_g = _raw_g.rename({'Req_Date': '_Req_Date'})
                        else:
                            raise ValueError("Date column not found in global test CSV.")

                        _recent_g = _raw_g.filter(
                            (pl.col('_Req_Date') >= pl.lit(_recency_start)) &
                            (pl.col('_Req_Date') < pl.lit(_TEST_CUTOFF_DATE))
                        )
                        _missing_g = [c for c in args.group_by if c not in _recent_g.columns]
                        if _missing_g:
                            logging.warning(f"  Global 90-day filter: missing group cols {_missing_g}; skipping.")
                        elif len(_recent_g) == 0:
                            logging.warning("  Global 90-day filter: no events in recency window; skipping.")
                        else:
                            _active_g = _recent_g.select(args.group_by).unique()
                            _gtdf = pl.read_parquet(global_test_output)
                            _gtdf_active = _gtdf.join(_active_g, on=args.group_by, how='inner')
                            logging.info(
                                f"  Global test after 90-day recency filter: "
                                f"{len(_gtdf_active)} rows / {_gtdf_active.select(args.group_by).unique().height} groups "
                                f"(was {len(_gtdf)} rows)"
                            )
                            _gtdf_active.write_parquet(global_test_output)
                            _gtdf_active.write_csv(global_test_output.replace('.parquet', '.csv'))
                    except Exception as _eg:
                        logging.warning(f"  Global 90-day recency filter failed (non-fatal): {_eg}")
            else:
                logging.warning(f"  Global test CSV not found: {global_test_csv}. "
                                "Run data_split.py with --global-train-all first.")

        logging.info(f"\n{'='*80}")
        logging.info("AUTO-MODE COMPLETE: Both datasets processed")
        logging.info(f"{'='*80}")
    else:
        # Single file mode (original behavior)
        output_file = args.output_file if args.output_file else 'data/training_dataset.parquet'
        main(
            train_file=args.train_file,
            group_by_cols=args.group_by,
            additional_features=[],  # true forecasting: no raw business cols in either train or test
            months_ahead=args.months_ahead,
            output_file=output_file,
            quarter_growth_file=args.quarter_growth_file,
            no_quarter_growth=args.no_quarter_growth,
            no_uplf=args.no_uplf,
        )
        if args.individual_groups:
            logging.warning("  --individual-groups is only supported in auto-mode; skipping per-group export.")
