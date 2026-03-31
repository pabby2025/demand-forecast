"""
Data Processing and Splitting Script for DFC demand-forecasting pipeline.

Loads the raw DFC (Demand Fulfilment Cycle) CSV, applies grade normalisation,
optionally merges UPLF FTE data, then performs a strict chronological train/test
split to prevent any future data leakage into the training set.

Chronological split logic:
  The split date is fixed at 2025-06-30.  All rows with Requirement Start Date
  on or before that date form the training set; rows with RSD from 2025-07-01
  onwards form the test set.  This mirrors the real production scenario where
  the model is trained on historical data and evaluated on future demand.

Leakage prevention:
  * Time-to-next-order targets are computed ONLY on the training set.
    Computing them on the test set would require looking at future order dates.
  * UPLF FTE and business-detail columns (Account Name, Market, Revenue
    potential, etc.) are stripped from both train and test sets before saving,
    because those fields are not available at real inference time (only group
    identifiers + historical date records are known when forecasting).
  * The demand-percentage filter (--demand-pct) identifies top-N groups using
    training-set demand only; the same groups are then applied to filter the
    test set.

Optional pre-computation:
  * SSD (SO Submission Date) floors are computed from the raw CSV and saved as
    ssd_floors.csv alongside the train/test files so that ssd_guardrail.py can
    apply the guardrail without re-reading the full raw CSV at prediction time.

Outputs (written under results_<SUFFIX>/data_<SUFFIX>/):
  train_data.csv            -- Training rows (earliest Req_Date to 2025-06-30).
  test_data.csv             -- Test rows (full history for CAGR, filtered to
                               Jul-Dec 2025 during build_training_groups.py).
  train_data_global.csv     -- Unfiltered training rows (all groups, no
                               demand_pct filter); only when --global-train-all.
  test_data_global.csv      -- Same for test.
  ssd_floors.csv            -- Pre-computed SSD floor counts per group x window.
  Quarter_growth_by_country_uplf_train.csv / _test.csv  -- Quarterly growth
                               features split at Jun 2025.
  excluded_groups.json      -- Groups with demand <= min_demand_threshold.
  plots/demand_vs_group_distribution_*.png  -- Cumulative demand vs group plots.

Usage:
    python data_split.py \\
        --input-file   DFC_YTD_2023-2025_v1_corrected_skill.csv \\
        --group-by     BU "Skill Cluster" \\
        --demand-pct   80 \\
        --ssd-cutoff   2025-06-30
"""

import polars as pl
import numpy as np
from datetime import datetime
import warnings
import argparse
import os
import logging
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import coloredlogs

warnings.filterwarnings('ignore')

coloredlogs.install(level="INFO", fmt="%(asctime)s - %(levelname)s - %(message)s", isatty=True)


def parse_date_flexible(date_col):
    """Parse a Polars string column into pl.Date using cascading format fallbacks.

    Tries formats in priority order:
      1. ISO 8601:   YYYY-MM-DD  (e.g. "2025-07-01")
      2. US slash:   MM/DD/YYYY  (e.g. "07/01/2025")
      3. US dash:    MM-DD-YYYY  (e.g. "07-01-2025")

    The cascading pl.when/otherwise pattern means:
      - If the ISO parse succeeds, that result is used.
      - If ISO fails but slash succeeds, the slash result is used.
      - Otherwise the dash parse result (which may be null) is returned.

    Using strict=False on each strptime call makes it return null for rows
    that don't match the format rather than raising an error, which is what
    allows the cascade to work across mixed-format columns.

    Args:
        date_col: A Polars String/Utf8 column expression.

    Returns:
        Polars expression that evaluates to a pl.Date (or null where all
        formats fail).
    """
    parsed_iso   = date_col.str.strptime(pl.Date, '%Y-%m-%d', strict=False)
    parsed_slash = date_col.str.strptime(pl.Date, '%m/%d/%Y', strict=False)
    parsed_dash  = date_col.str.strptime(pl.Date, '%m-%d-%Y', strict=False)
    return (
        pl.when(parsed_iso.is_not_null()).then(parsed_iso)
        .when(parsed_slash.is_not_null()).then(parsed_slash)
        .otherwise(parsed_dash)
    )


def _compute_ssd_floors(input_file: str, group_cols: list,
                         ssd_cutoff_date, forecast_year: int, forecast_month: int,
                         m_windows: list = None) -> pl.DataFrame:
    """
    Pre-compute SSD (SO Submission Date) floor counts from the raw input CSV.

    The guardrail concept: any SO that was already submitted before the training
    cutoff (SSD < ssd_cutoff_date) with a Requirement Start Date falling in a
    future forecast window IS CONFIRMED demand.  The model cannot predict fewer
    SOs than are already confirmed in the system, so this count acts as a
    minimum floor for each (group x window) prediction.

    The floor table is saved as ssd_floors.csv by data_split.main() so that
    ssd_guardrail.py can apply the correction without re-reading the full raw
    CSV at guardrail time.

    Pipeline steps (Polars operations):
      Step 1 -- Read the raw CSV (schema-inferred over first 10,000 rows).
      Step 2 -- Parse SSD and RSD columns using parse_date_flexible (both must
                be non-null).
      Step 3 -- Filter to SSD < ssd_cutoff_date  (keep only confirmed SOs).
      Step 4 -- Compute _window = months offset from (forecast_year, forecast_month).
                _window = (rsd.year - forecast_year)*12 + (rsd.month - forecast_month)
                So _window=0 means M0 (July 2025 if forecast_month=7), etc.
      Step 5 -- Keep only rows whose _window is in the requested m_windows list.
      Step 6 -- Stringify group columns for consistent join keys downstream.
      Step 7 -- Group by (group_cols + _window) and count rows -> ssd_floor_count.

    Args:
        input_file:     Path to the raw DFC CSV.
        group_cols:     List of column names that define each group (e.g.
                        ['BU', 'Skill Cluster']).
        ssd_cutoff_date: date object; SOs with SSD < this date are confirmed.
        forecast_year:  Year of M0 (the first forecast month).
        forecast_month: Month of M0.
        m_windows:      List of integer window offsets to include; defaults to
                        [0, 1, 2, 3, 4, 5] (M0-M5).

    Returns:
        pl.DataFrame with columns [*group_cols, 'window', 'ssd_floor_count'],
        or an empty DataFrame if the required columns are missing.
    """
    if m_windows is None:
        m_windows = list(range(6))   # default M0-M5

    _SSD_COL = 'SO Submission Date'
    _RSD_COL = 'Requirement Start Date'

    # Step 1: Read raw CSV
    try:
        raw = pl.read_csv(input_file, infer_schema_length=10000)
    except Exception as e:
        print(f"  Warning: could not read input file for SSD floors: {e}")
        return pl.DataFrame()

    if _SSD_COL not in raw.columns or _RSD_COL not in raw.columns:
        print(f"  Warning: '{_SSD_COL}' or '{_RSD_COL}' not found; skipping SSD floor computation.")
        return pl.DataFrame()

    valid_group_cols = [c for c in group_cols if c in raw.columns]
    if not valid_group_cols:
        print(f"  Warning: none of {group_cols} found in CSV; skipping SSD floor computation.")
        return pl.DataFrame()

    # Steps 2-5: Parse dates, apply cutoff filter, compute window offset, filter to requested windows
    raw = (
        raw
        .with_columns([
            parse_date_flexible(pl.col(_SSD_COL)).alias('_ssd'),  # Step 2a: parse SSD
            parse_date_flexible(pl.col(_RSD_COL)).alias('_rsd'),  # Step 2b: parse RSD
        ])
        .filter(pl.col('_ssd').is_not_null() & pl.col('_rsd').is_not_null())  # Step 2c: drop unparseable
        .filter(pl.col('_ssd') < pl.lit(ssd_cutoff_date))  # Step 3: confirmed SOs only
        .with_columns([
            # Step 4: compute how many months after forecast start each RSD falls
            ((pl.col('_rsd').dt.year() - forecast_year) * 12
             + (pl.col('_rsd').dt.month() - forecast_month)).alias('_window')
        ])
        .filter(pl.col('_window').is_in(m_windows))  # Step 5: keep relevant windows
    )

    if raw.is_empty():
        print(f"  No confirmed SOs found in M-windows {m_windows}; SSD floors will all be 0.")
        return pl.DataFrame()

    # Step 6: Stringify group columns so join keys are consistent (strips leading/trailing whitespace)
    raw = raw.with_columns([pl.col(c).cast(pl.Utf8).str.strip_chars() for c in valid_group_cols])

    # Step 7: Count confirmed SOs per (group x window) -> ssd_floor_count
    floors = (
        raw.group_by(valid_group_cols + ['_window'])
        .agg(pl.len().alias('ssd_floor_count'))
        .rename({'_window': 'window'})
        .sort(valid_group_cols + ['window'])
    )
    return floors


def split_quarter_growth_file(
    input_path: str = "data/Quarter_growth_by_country_uplf.csv",
    train_output_path: str = "data/Quarter_growth_by_country_uplf_train.csv",
    test_output_path: str = "data/Quarter_growth_by_country_uplf_test.csv",
    publishing: bool = False,
) -> None:
    """Split a master Quarter_growth_by_country_uplf.csv into train and test CSVs.

    Normal mode:      split at Jun 2025 (train ≤ Jun 2025, test starts May 2025).
    Publishing mode:  split at Dec 2025 (train ≤ Dec 2025, test starts Nov 2025).

    Automatically fills null values in quarter_growth_pct_from_sum column with 0.0 to ensure clean data.
    """
    if not os.path.exists(input_path):
        logging.error(f"Input growth file not found: {input_path}")
        return

    logging.info(f"Loading growth data from {input_path}")
    df = pl.read_csv(input_path)

    expected_cols = {"Country", "year", "month", "quarter_growth_pct_from_sum"}
    missing = expected_cols.difference(set(df.columns))
    if missing:
        logging.error(f"Input file is missing expected columns: {missing}")
        return

    # Convert month names to numbers and ensure numeric types
    month_mapping = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    # Fill null values first, then convert month names to numbers
    null_count_before = df["quarter_growth_pct_from_sum"].null_count()
    df = df.with_columns([
        pl.col("quarter_growth_pct_from_sum").fill_null(0.0),
        pl.col("month").replace(month_mapping, default=None).cast(pl.Int32, strict=False),
        pl.col("year").cast(pl.Int32, strict=False)
    ])
    if null_count_before > 0:
        logging.info(f"Replaced {null_count_before} null values in quarter_growth_pct_from_sum with 0.0")

    if publishing:
        # Publishing mode: train through Dec 2025; test buffer starts Nov 2025 (2 months before Jan 2026 M0)
        train_mask = (pl.col("year") < 2025) | ((pl.col("year") == 2025) & (pl.col("month") <= 12))
        test_mask = (pl.col("year") > 2025) | ((pl.col("year") == 2025) & (pl.col("month") >= 11))
    else:
        # Normal mode: train through Jun 2025; test buffer starts May 2025 (2 months before Jul 2025)
        train_mask = (pl.col("year") < 2025) | ((pl.col("year") == 2025) & (pl.col("month") <= 6))
        test_mask = (pl.col("year") > 2025) | ((pl.col("year") == 2025) & (pl.col("month") >= 5))
    df_train = df.filter(train_mask)
    df_test = df.filter(test_mask)

    logging.info(f"Saving train quarter growth data ({len(df_train)} rows) to {train_output_path}")
    df_train.write_csv(train_output_path)

    logging.info(f"Saving test quarter growth data ({len(df_test)} rows) to {test_output_path}")
    df_test.write_csv(test_output_path)

    logging.info("Quarter growth file split completed successfully")


def create_time_features(df, date_format='%d/%m/%y'):
    """Create time-based features from Requirement Start Date only.

    Supports both ISO format (YYYY-MM-DD) and custom format (default: DD/MM/YY).
    Only Req_Date features are created since forecasting is based on Requirement Start Date.
    """
    print("Creating time features from Requirement Start Date...")

    # Parse Requirement Start Date only
    df = df.with_columns([
        parse_date_flexible(pl.col('Requirement Start Date')).alias('Req_Date')
    ])

    # Extract only essential Req_Date features for forecasting
    df = df.with_columns([
        pl.col('Req_Date').dt.year().fill_null(0).alias('Req_Year'),
        pl.col('Req_Date').dt.month().fill_null(0).alias('Req_Month'),
        pl.col('Req_Date').dt.quarter().fill_null(0).alias('Req_Quarter'),
        pl.col('Req_Date').dt.weekday().fill_null(0).alias('Req_Weekday'),
    ])

    print("  Created time features from Req_Date only")
    return df


def combine_grades(df, col='SO GRADE', exclude_vals=('cont',)):
    """Merge grade categories and exclude specified values"""
    # Prepare case-insensitive matching
    src = pl.col(col).cast(pl.Utf8)
    upper = src.str.to_uppercase()

    # Map (PT, PAT, PA, P, Admin Staff) -> GenC
    lm_set = ['PT', 'PAT', 'PA', 'P', 'ADMIN STAFF']
    combined = pl.when(upper.is_in(lm_set)).then(pl.lit('GenC')).otherwise(src)

    # Exclude rows matching exclude_vals (case-insensitive)
    excl_upper = [v.upper() for v in exclude_vals]
    df = df.filter(~upper.is_in(excl_upper))

    # Write merged grades
    df = df.with_columns(combined.alias(col))

    return df


def calculate_time_to_next_order(df, group_by=None, date_format='%d/%m/%y'):
    """For each order, calculate days until next order within specified groups or overall, with censor flag."""
    # Determine grouping strategy for time-to-next calculations
    if group_by is None or len(group_by) == 0:
        print("Calculating time-to-next-order (overall, no grouping)...")
        group_by = []
    else:
        print(f"Calculating time-to-next-order grouped by: {group_by}")

    # Ensure we have parsed dates for calculation using flexible parsing
    df = df.with_columns([
        parse_date_flexible(pl.col('Requirement Start Date')).alias('Req_Date')
    ])
    df = df.filter(pl.col('Req_Date').is_not_null())

    # Handle overall calculation (no grouping)
    if len(group_by) == 0:
        # Overall calculation (original logic)
        # Sort by date to ensure chronological order for time-to-next calculations
        df = df.sort('Req_Date')
        time_to_next = []
        is_censored = []
        rows = df.to_dicts()

        # Calculate time-to-next for each row, handling censoring for the last observation
        for i, row in enumerate(rows):
            if i == len(rows) - 1:
                # Last observed point: censored to max horizon (365 days) as we don't know when next event occurs
                time_to_next.append(365.0)
                is_censored.append(1)
                continue
            cur = row['Req_Date']
            nxt = rows[i + 1].get('Req_Date')
            if cur is None or nxt is None:
                # Handle missing dates with censoring
                time_to_next.append(365.0)
                is_censored.append(1)
                continue
            dt = (nxt - cur).days
            if dt <= 0:
                dt = 1  # Ensure minimum 1 day gap
            time_to_next.append(float(dt))
            is_censored.append(0)  # Uncensored observation

        # Add calculated time-to-next and censoring columns to dataframe
        df = df.with_columns([
            pl.Series('Time_To_Next_Order', time_to_next),
            pl.Series('Is_Censored', is_censored)
        ])

        print(f"  Mean time-to-next: {np.mean(time_to_next):.2f} days; censored: {sum(is_censored)}")
    else:
        # Grouped calculation: calculate time-to-next within each group separately
        time_to_next_all = []
        is_censored_all = []

        # Process each group separately to maintain within-group temporal relationships
        grouped = df.group_by(group_by)
        groups_list = list(grouped)
        total_groups = len(groups_list)

        # Process each group individually for time-to-next calculations
        for group_idx, (group_keys, group_df) in enumerate(groups_list):
            group_name = "_".join(str(k) for k in group_keys) if len(group_keys) > 1 else str(group_keys[0])
            print(f"  Processing group {group_idx + 1}/{total_groups}: {group_name} ({len(group_df)} rows)")

            # Sort group by date to ensure chronological order
            group_df = group_df.sort('Req_Date')
            rows = group_df.to_dicts()

            time_to_next_group = []
            is_censored_group = []

            # Calculate time-to-next within this specific group
            for i, row in enumerate(rows):
                if i == len(rows) - 1:
                    # Last observed point in group: censored to max horizon (365 days)
                    time_to_next_group.append(365.0)
                    is_censored_group.append(1)
                    continue
                cur = row['Req_Date']
                nxt = rows[i + 1].get('Req_Date')
                if cur is None or nxt is None:
                    # Handle missing dates within group with censoring
                    time_to_next_group.append(365.0)
                    is_censored_group.append(1)
                    continue
                dt = (nxt - cur).days
                if dt <= 0:
                    dt = 1  # Ensure minimum 1 day gap within group
                time_to_next_group.append(float(dt))
                is_censored_group.append(0)  # Uncensored observation within group

            # Accumulate results from all groups
            time_to_next_all.extend(time_to_next_group)
            is_censored_all.extend(is_censored_group)

        df = df.with_columns([
            pl.Series('Time_To_Next_Order', time_to_next_all),
            pl.Series('Is_Censored', is_censored_all)
        ])

        print(f"  Processed {total_groups} groups, mean time-to-next: {np.mean(time_to_next_all):.2f} days; censored: {sum(is_censored_all)}")

    return df


# Canonical suffix for results folder: must match run_pipeline.sh GROUP_INITIALS (results_S, results_RLC, results_BS, results_BRLC).
# Notation: S = Skill Cluster only; RLC = Role Location Cluster (SO GRADE, Country, Skill Cluster);
# BS = BU + Skill Cluster; BRLC = BU + RLC (BU, SO GRADE, Country, Skill Cluster).
_CANONICAL_GROUP_SUFFIX = {
    frozenset(['Skill Cluster']): 'S',
    frozenset(['Country', 'SO GRADE', 'Skill Cluster']): 'RLC',
    frozenset(['BU', 'Skill Cluster']): 'BS',
    frozenset(['BU', 'SO GRADE', 'Country', 'Skill Cluster']): 'BRLC',
}


def get_data_dir_for_group_by(group_by_cols: list) -> str:
    """Return path to data dir inside results (e.g. results_BS/data_BS). Uses same naming as run_pipeline (capital suffix)."""
    if not group_by_cols:
        return 'results_ALL/data_ALL'
    normalized = [c.strip() for c in group_by_cols if c and str(c).strip()]
    if not normalized:
        return 'results_ALL/data_ALL'
    key = frozenset(normalized)
    suffix = _CANONICAL_GROUP_SUFFIX.get(key)
    if suffix is None:
        # Fallback: first letter of each col in canonical order (match build_training_groups ordering)
        order = ['BU', 'SO GRADE', 'Country', 'Skill Cluster']
        seen = set()
        initials = []
        for col in order:
            if col in key and col not in seen:
                seen.add(col)
                initials.append(col[0].upper())
        for col in normalized:
            if col not in seen:
                initials.append(col[0].upper())
        suffix = ''.join(initials) if initials else 'ALL'
    return os.path.join(f"results_{suffix}", f"data_{suffix}")


def get_data_subdir_for_group_by(group_by_cols: list) -> str:
    """Return data subdir name only (e.g. data_S) for use under a custom results base dir."""
    return os.path.basename(get_data_dir_for_group_by(group_by_cols))


def resolve_results_dir(base_name: str) -> str:
    """Return a unique results directory name that does not yet exist on disk.

    Prevents accidental overwriting of previous runs by appending an
    incrementing numeric suffix when the base name is already taken.
    For example:
      - First run:  base_name          (if this doesn't exist yet)
      - Second run: base_name_1
      - Third run:  base_name_2
      ... and so on until an unused name is found.

    Args:
        base_name: Desired directory name (e.g. 'results_BS').

    Returns:
        str: A directory path guaranteed not to exist yet.
    """
    if not os.path.exists(base_name):
        return base_name
    i = 1
    while os.path.exists(f"{base_name}_{i}"):
        i += 1
    return f"{base_name}_{i}"


# Grouping configs used by pipeline (RLC, S, BS, BRLC); used to build demand-vs-group plots for all.
ALL_GROUP_CONFIGS = {
    'S': ['Skill Cluster'],
    'RLC': ['Country', 'SO GRADE', 'Skill Cluster'],
    'BS': ['BU', 'Skill Cluster'],
    'BRLC': ['BU', 'SO GRADE', 'Country', 'Skill Cluster'],
}


def plot_demand_vs_group_distribution_all(df_train: pl.DataFrame, output_dir: str = 'data') -> None:
    """Plot cumulative demand vs number of groups for each pipeline grouping; save under output_dir with vertical lines at 80% and 90%."""
    if df_train is None or df_train.is_empty():
        return
    os.makedirs(output_dir, exist_ok=True)
    for name, group_by_cols in ALL_GROUP_CONFIGS.items():
        missing = [c for c in group_by_cols if c not in df_train.columns]
        if missing:
            logging.info(f"  Skipping demand plot for {name}: missing columns {missing}")
            continue
        counts_df = (
            df_train.group_by(group_by_cols)
            .agg(pl.len().alias('train_rows'))
            .sort('train_rows', descending=True)
        )
        if counts_df.is_empty():
            continue
        train_rows = counts_df['train_rows'].to_numpy()
        total = float(train_rows.sum())
        if total <= 0:
            continue
        n = len(train_rows)
        cumul_pct = 100.0 * np.cumsum(train_rows) / total
        n_80 = int(np.argmax(cumul_pct >= 80) + 1) if np.any(cumul_pct >= 80) else n
        n_90 = int(np.argmax(cumul_pct >= 90) + 1) if np.any(cumul_pct >= 90) else n
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(np.arange(1, n + 1), cumul_pct, color='#2e86ab', linewidth=2, label='Cumulative % demand')
        ax.axvline(x=n_80, color='#e94f37', linestyle='--', linewidth=1.5, label=f'80% demand ({n_80} groups)')
        ax.axvline(x=n_90, color='#44af69', linestyle=':', linewidth=1.5, label=f'90% demand ({n_90} groups)')
        ax.set_xlabel('Number of groups (sorted by demand descending)')
        ax.set_ylabel('Cumulative % of total demand (train rows)')
        ax.set_title(f'Demand vs group distribution ({name})')
        ax.legend(loc='lower right')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, n)
        ax.set_ylim(0, 105)
        out_path = os.path.join(output_dir, f'demand_vs_group_distribution_{name}.png')
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        logging.info(f"  Demand vs group distribution ({name}): {out_path} (80%%: {n_80} groups, 90%%: {n_90} groups)")


def _strip_test_columns(df: pl.DataFrame, group_by: list) -> pl.DataFrame:
    """Strip dataframe to only essential columns for true forecasting.

    True forecasting means: at inference time we only know the group identifiers;
    ALL other features are COMPUTED from historical date records (SMA, WMA, growth
    rates, lagged counts, trajectory, etc.).  Raw business columns (Account Name,
    Practice, Market, Revenue potential, SO TYPE, etc.) AND UPLF/FTE data are NOT
    available at inference time and must be removed so the model never learns to
    depend on them.

    Columns retained:
      - group_by columns ONLY (the exact prediction keys passed in, e.g. ['Skill Cluster']
        or ['BU', 'Skill Cluster']) — nothing extra like Country / SO GRADE that is not
        part of the grouping level being used.
      - 'Requirement Start Date' + parsed Req_Date/Req_Year/Req_Month/Req_Quarter/Req_Weekday
        (needed so build_training_groups.py can compute SMA, WMA, growth, lagged counts …)
      - Quantity (for quantity-based features if present)

    Everything else — UPLF FTE columns, has_uplf_data, Industry Segment,
    Customer Classification, all raw business string fields, and any hierarchy cols
    not part of the current group_by — is dropped.
    """
    keep = set(group_by) | {
        # Date columns for feature engineering
        'Requirement Start Date', 'Req_Date',
        'Req_Year', 'Req_Month', 'Req_Quarter', 'Req_Weekday',
        # Quantity-based features
        'Quantity',
    }
    keep_ordered = [c for c in df.columns if c in keep]
    removed = [c for c in df.columns if c not in keep]
    if removed:
        print(f"  True-forecasting strip: removed {len(removed)} column(s) from test data "
              f"(raw business fields + UPLF/FTE — not available at inference time).")
        print(f"    Removed: {removed}")
        print(f"    Kept {len(keep_ordered)} essential columns: {keep_ordered}")
    return df.select(keep_ordered)


def main(input_file='data/DFC_YTD_2023-2025_skill.csv', uplf_file='data/UPLF_full.csv', group_by=None, date_format='%d/%m/%y',
         split_mode='train,test', no_open=True, quarter_growth_file='data/Quarter_growth_by_country_uplf.csv', exclude_grades=None,
         min_demand_threshold=36, no_uplf=False, demand_pct=80.0, results_dir=None, global_train_all=False,
         ssd_cutoff='2025-06-30', train_min_year=2023, publishing=False):
    """Process raw DFC data and produce chronological train/test splits with no leakage.

    Execution phases (annotated with Step comments below):
      Step 1 -- Argument normalisation and output directory setup.
      Step 2 -- Quarter-growth file split (train / test at Jun 2025).
      Step 3 -- Determine chronological date boundaries for the split.
      Step 4 -- Load raw DFC CSV; apply Skill Cluster null drop; grade exclusions.
      Step 5 -- Optionally merge UPLF FTE data (skip if --no-uplf).
      Step 6 -- Optionally filter out rows with SO Line Status = 'OPEN'.
      Step 7 -- Drop rows with null/empty group_by column values.
      Step 8 -- Sort chronologically by Requirement Start Date.
      Step 9 -- Create time features (Req_Year, Req_Month, Req_Quarter, Req_Weekday).
      Step 10 -- Filter out rows before train_min_year.
      Step 11 -- Remove low-demand groups (demand <= min_demand_threshold).
      Step 12 -- Chronological train/test split at 2025-06-30.
                 The split date is FIXED to prevent leakage: no future data can
                 influence the model since all rows after 2025-06-30 are test-only.
      Step 13 -- Compute time-to-next-order targets on training data ONLY.
      Step 14 -- Demand-percentage filter (keep groups covering demand_pct % of train).
      Step 15 -- Strip train and test to essential columns (true forecasting mode).
      Step 16 -- Save train, test (and optional global) CSVs.
      Step 17 -- Pre-compute SSD floors and save ssd_floors.csv.

    Args:
        input_file:           Path to raw DFC CSV.
        uplf_file:            Path to UPLF FTE CSV (optional).
        group_by:             List of group column names (e.g. ['BU', 'Skill Cluster']).
        date_format:          Legacy date format string (used only in time_to_next_order).
        split_mode:           'train,test' (dev merged into train) or 'train,dev,test'.
        no_open:              If True, filter out OPEN status rows before splitting.
        quarter_growth_file:  Path to quarterly growth CSV; split alongside main data.
        exclude_grades:       List of SO GRADE values to remove (case-insensitive).
        min_demand_threshold: Groups with <= this many rows in full dataset are dropped.
        no_uplf:              If True, skip UPLF load/merge entirely.
        demand_pct:           Retain only top groups covering this % of train demand.
        results_dir:          Custom base results dir; overrides the canonical naming.
        global_train_all:     If True, also save unfiltered global train/test CSVs.
        ssd_cutoff:           ISO date string; SOs with SSD < this are confirmed for floors.
        train_min_year:       Drop rows with Req_Date before Jan 1 of this year.
        publishing:           If True, shift all date boundaries for a 2026 forecast:
                              train_end=2025-12-31, feature buffer=Nov-Dec 2025, M0=Jan 2026.

    Returns:
        dict with keys 'train_file', 'test_file', 'train_rows', 'test_rows',
        'cutoff_date', and optionally 'dev_file'/'dev_rows'.
    """
    print("Starting data processing and splitting pipeline...")

    # Step 1a: Normalise group_by parameter so we always have a list
    if group_by is None:
        group_by = ['Skill Cluster']
    elif isinstance(group_by, str):
        group_by = [group_by]

    # Step 1b: Resolve output directory using canonical naming (results_<SUFFIX>/data_<SUFFIX>)
    # or the user-provided --results-dir.  resolve_results_dir appends a numeric
    # suffix (e.g. _1, _2) to avoid overwriting previous runs.
    if results_dir:
        output_dir = os.path.join(results_dir, get_data_subdir_for_group_by(group_by))
    else:
        base_dir = get_data_dir_for_group_by(group_by)
        results_base = os.path.dirname(base_dir)
        data_subdir = os.path.basename(base_dir)
        resolved_base = resolve_results_dir(results_base)
        output_dir = os.path.join(resolved_base, data_subdir)
    plots_dir = os.path.join(output_dir, 'plots')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(plots_dir, exist_ok=True)

    # Step 2: Split the quarter-growth file at Jun 2025 (same boundary as train/test)
    # so quarterly growth features are also leakage-free for the test period.
    if os.path.exists(quarter_growth_file):
        print(f"\nProcessing quarter growth file: {quarter_growth_file}")
        split_quarter_growth_file(
            input_path=quarter_growth_file,
            train_output_path=os.path.join(output_dir, 'Quarter_growth_by_country_uplf_train.csv'),
            test_output_path=os.path.join(output_dir, 'Quarter_growth_by_country_uplf_test.csv'),
            publishing=publishing,
        )
    else:
        print(f"\nQuarter growth file not found: {quarter_growth_file}, skipping quarter growth split")

    group_desc = f"grouped by {group_by}" if group_by else "overall (no grouping)"

    # Determine split strategy based on mode
    if split_mode == 'train,dev,test':
        # Create separate train/dev/test splits
        create_dev_split = True
        train_end = datetime(2025, 6, 30).date()
        dev_start = datetime(2025, 7, 1).date()
        dev_end = datetime(2025, 12, 31).date()
        # Test period: Start 2 months before July 1st (May 1st) to have history for July predictions
        # Actual test predictions start from July 1st, but we need May-June data for feature engineering
        test_start = datetime(2025, 5, 1).date()  # 2 months before July 1st
        test_end = datetime(2025, 12, 31).date()
        actual_test_start = datetime(2025, 7, 1).date()  # First actual test prediction cutoff
        print(f"Processing configuration: {group_desc} (train/dev/test split)")
        print("  Training period: [earliest Req_Date] to 2025-06-30")
        print("  Dev period:      2025-07-01 to 2025-12-31")
        print(f"  Testing period:  [earliest Req_Date] to {test_end} (full data for CAGR calculations, will filter to July-Dec during build)")
        print(f"  Actual test predictions start from: {actual_test_start}")
    else:
        # Default: train + dev combined, test separate
        create_dev_split = False
        train_end = datetime(2025, 6, 30).date()  # Include dev period in training
        # Test period: Start 2 months before July 1st (May 1st) to have history for July predictions
        # Actual test predictions start from July 1st, but we need May-June data for feature engineering
        test_start = datetime(2025, 5, 1).date()  # 2 months before July 1st
        test_end = datetime(2025, 12, 31).date()
        actual_test_start = datetime(2025, 7, 1).date()  # First actual test prediction cutoff
        print(f"Processing configuration: {group_desc} (train/test only - dev added to train)")
        print("  Training period: [earliest Req_Date] to 2025-06-30 (includes dev period)")
        print(f"  Testing period:  [earliest Req_Date] to {test_end} (full data for CAGR calculations, will filter to July-Dec during build)")
        print(f"  Actual test predictions start from: {actual_test_start}")
        print("  Dev period:      None (added to training)")

    # ── Publishing mode: override date boundaries for a Jan-Jun 2026 forecast ──
    # Training extends through all of 2025; forecast starts Jan 2026 (M0).
    # The test CSV includes historical data for feature engineering (CAGR, SMA, etc.)
    # starting 2 months before M0 (i.e., Nov 2025) and extending to Jun 2026.
    # Actual 2026 rows will be absent from the input file (they don't exist yet),
    # so the test set contains only the Nov-Dec 2025 history buffer, which is
    # sufficient for build_training_groups.py to engineer features at a Jan-2026 cutoff.
    if publishing:
        create_dev_split = False          # publishing never needs a separate dev split
        train_end          = datetime(2025, 12, 31).date()   # Train through Dec 31
        test_start         = datetime(2025, 11,  1).date()   # Feature buffer: Nov-Dec 2025 history for features
        test_end           = datetime(2026,  6, 30).date()   # M5 = Jun 2026
        actual_test_start  = datetime(2026,  1,  1).date()   # M0 = Jan 2026
        ssd_cutoff         = '2025-12-31'                    # SSD floor: confirmed SOs up to end of 2025
        print(f"\n[Publishing mode] Date boundaries overridden:")
        print(f"  Training period: [earliest Req_Date] to 2025-12-31")
        print(f"  Feature buffer:  2025-11-01 to 2025-12-31 (Nov-Dec 2025 history for features)")
        print(f"  SSD cutoff:      2025-12-31 (confirmed SOs for floor computation)")
        print(f"  Actual forecast starts from: {actual_test_start} (M0 = Jan 2026)")

    # Step 4a: Load raw DFC CSV
    print(f"\nLoading data from {input_file}...")
    df = pl.read_csv(input_file)
    print(f"  Loaded {df.shape[0]} rows, {df.shape[1]} columns")

    # Step 4b: Drop rows with null Skill Cluster (cannot assign to any group)
    if 'Skill Cluster' in df.columns:
        null_count = df.filter(pl.col('Skill Cluster').is_null()).height
        if null_count > 0:
            print(f"  Removing {null_count} rows with null/None Skill Cluster values...")
            df = df.filter(pl.col('Skill Cluster').is_not_null())

    # Step 4c: Filter out rows with excluded SO GRADE values if specified
    if exclude_grades and len(exclude_grades) > 0:
        print(f"\nFiltering out rows with SO GRADE in: {exclude_grades}...")
        if 'SO GRADE' not in df.columns:
            print(f"  Warning: 'SO GRADE' column not found. Available columns: {list(df.columns)}")
        else:
            initial_count = len(df)
            # Case-insensitive matching
            exclude_upper = [g.upper() for g in exclude_grades]
            df = df.filter(
                ~pl.col('SO GRADE').cast(pl.Utf8).str.to_uppercase().is_in(exclude_upper)
            )
            filtered_count = initial_count - len(df)
            print(f"  Filtered out {filtered_count} rows with excluded SO GRADE values")
            print(f"  Remaining rows: {len(df)} ({len(df)/initial_count*100:.1f}% of original)")

    # Load UPLF data and merge (skip if --no-uplf flag is set)
    if no_uplf:
        print(f"\n--no-uplf flag is set: Skipping UPLF data loading and merging")
        print(f"  UPLF columns will not be appended to DFC data")
    elif os.path.exists(uplf_file):
        print(f"\nLoading UPLF data from {uplf_file}...")
        # Handle UPLF CSV with mixed dtypes (FTE columns have decimals)
        # Use pandas to read first, then convert to polars to handle mixed types
        import pandas as pd
        df_uplf_pd = pd.read_csv(uplf_file, low_memory=False)
        df_uplf = pl.from_pandas(df_uplf_pd)
        print(f"  Loaded UPLF {df_uplf.shape[0]} rows, {df_uplf.shape[1]} columns")

        # STEP 1: Align UPLF column names to match DFC naming conventions
        print("\nSTEP 1: Aligning UPLF column names to match DFC naming conventions...")
        column_mapping = {
            'Off/On': 'Off/ On',  # Add space after slash to match DFC
            'SO Grade': 'SO GRADE',  # Capitalize to match DFC
            'Sub Vertical': 'SubVertical',  # Remove space to match DFC
        }

        # Apply column renaming
        columns_to_rename = {old: new for old, new in column_mapping.items() if old in df_uplf.columns}
        if columns_to_rename:
            df_uplf = df_uplf.rename(columns_to_rename)
            print(f"  Renamed UPLF columns: {columns_to_rename}")

        # STEP 2: Apply the same grade combining logic to UPLF data BEFORE merging
        print("\nSTEP 2: Applying grade combining to UPLF data...")
        if 'SO GRADE' in df_uplf.columns:
            grade_count_before = df_uplf.select(pl.col('SO GRADE').n_unique()).item()
            print(f"  SO GRADE categories in UPLF before combining: {grade_count_before}")
            df_uplf = combine_grades(df_uplf, col='SO GRADE', exclude_vals=('cont',))
            grade_count_after = df_uplf.select(pl.col('SO GRADE').n_unique()).item()
            print(f"  SO GRADE categories in UPLF after combining: {grade_count_after}")
            print(f"  UPLF SO GRADES after combining: {df_uplf.select(pl.col('SO GRADE').unique().sort()).to_series().to_list()}")
        
        # Also apply to DFC data now (before matching) for consistency
        print("\nApplying grade combining to DFC data (before matching)...")
        grade_count_before_dfc = df.select(pl.col('SO GRADE').n_unique()).item()
        print(f"  SO GRADE categories in DFC before combining: {grade_count_before_dfc}")
        df = combine_grades(df)
        grade_count_after_dfc = df.select(pl.col('SO GRADE').n_unique()).item()
        print(f"  SO GRADE categories in DFC after combining: {grade_count_after_dfc}")
        print(f"  DFC SO GRADES: {df.select(pl.col('SO GRADE').unique().sort()).to_series().to_list()}")
        
        # Save the normalized UPLF data for reference
        normalized_uplf_file = uplf_file.replace('.csv', '_normalized.csv')
        print(f"\nSaving normalized UPLF data to: {normalized_uplf_file}")
        df_uplf.write_csv(normalized_uplf_file)
        print(f"  Saved {len(df_uplf)} rows, {len(df_uplf.columns)} columns")

        # STEP 3: Identify matching and new columns
        print("\nSTEP 3: Analyzing column alignment...")
        common_cols = set(df.columns) & set(df_uplf.columns)
        print(f"  ✓ Matched columns: {len(common_cols)}")
        print(f"    {sorted(common_cols)}")

        # Identify FTE columns (time-based data) on UPLF side
        fte_cols = [
            col
            for col in df_uplf.columns
            if col.endswith('_Actuals') or col.endswith('_Forecast') or col.endswith('_Adjustments')
        ]
        print(f"  ✓ FTE columns found in UPLF: {len(fte_cols)}")
        print(f"    Sample: {fte_cols[:5] if fte_cols else 'None'}")

        # Identify new non-FTE columns from UPLF
        new_uplf_cols = set(df_uplf.columns) - set(df.columns)
        new_non_fte_cols = [
            col for col in new_uplf_cols if not col.endswith(('_Actuals', '_Forecast', '_Adjustments'))
        ]
        print(f"  ✓ New non-FTE columns from UPLF: {len(new_non_fte_cols)}")
        print(f"    {sorted(new_non_fte_cols)}")

        # STEP 4: Define explicit matching columns from UPLF (normalized names)
        print("\nSTEP 4: Defining matching keys from UPLF data...")
        
        # Explicit matching columns to use (normalized UPLF column names that should exist in DFC)
        # These are the core business identifiers that should be consistent between systems
        matching_key_cols = [
            'Account ID',           # Unique account identifier
            'SO GRADE',            # Seniority level (normalized)
            'Country',             # Location
            'Parent Customer ID',  # Parent company identifier
            'Market',              # Market segment
            'BU',                  # Business unit
            'Practice',            # Practice
            'Off/ On',             # Offshore/Onshore (normalized with space)
            'Project Billability Type',  # BTM/BFD/etc
            'SBU1',
        ]
        
        # Verify all keys exist in both datasets
        missing_in_dfc = [k for k in matching_key_cols if k not in df.columns]
        missing_in_uplf = [k for k in matching_key_cols if k not in df_uplf.columns]
        
        if missing_in_dfc:
            print(f"  ⚠ Keys missing in DFC: {missing_in_dfc}")
            matching_key_cols = [k for k in matching_key_cols if k not in missing_in_dfc]
        
        if missing_in_uplf:
            print(f"  ⚠ Keys missing in UPLF: {missing_in_uplf}")
            matching_key_cols = [k for k in matching_key_cols if k not in missing_in_uplf]
        
        if len(matching_key_cols) == 0:
            print("  ✗ No valid matching columns found between DFC and UPLF")
            print("  Cannot proceed with merge")
            return
        
        print(f"  ✓ Using {len(matching_key_cols)} matching columns:")
        for key in matching_key_cols:
            print(f"    - {key}")
        
        # Normalize matching column values for consistent matching (case-insensitive, trimmed)
        print("\n  Normalizing matching column values for consistent matching...")
        
        # Normalize DFC matching columns
        for col in matching_key_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars().alias(col)
                )
        
        # Normalize UPLF matching columns
        for col in matching_key_cols:
            if col in df_uplf.columns:
                df_uplf = df_uplf.with_columns(
                    pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars().alias(col)
                )
        
        print(f"    Normalized {len(matching_key_cols)} columns to UPPERCASE and trimmed whitespace")
        
        # Count potential exact matches
        df_keys = df.select(matching_key_cols).unique()
        uplf_keys = df_uplf.select(matching_key_cols).unique()
        test_join = df_keys.join(uplf_keys, on=matching_key_cols, how='inner')
        match_count = len(test_join)
        
        print(f"\n  ✓ Potential exact matches: {match_count} row combinations")
        
        if match_count == 0:
            print("  ⚠ WARNING: No rows will match between DFC and UPLF!")
            print("  ✗ All FTE data will be zeros - check data quality")
            print("\n  Debugging info:")
            print(f"    DFC unique key combinations: {len(df_keys)}")
            print(f"    UPLF unique key combinations: {len(uplf_keys)}")
            
            # Show sample values from each dataset for comparison
            print("\n  Sample DFC keys (first 3):")
            for row in df_keys.head(3).to_dicts():
                print(f"    {row}")
            
            print("\n  Sample UPLF keys (first 3):")
            for row in uplf_keys.head(3).to_dicts():
                print(f"    {row}")
        
        best_key_cols = matching_key_cols

        # -------------------------------------------------------------------------------------
        # STEP 5: GROUP-BASED UPLF → DFC MAPPING
        # -------------------------------------------------------------------------------------
        print("\nSTEP 5: Group-based UPLF → DFC mapping (aggregate, distribute, aggregate strings)...")

        try:
            # A. Build DFC group sizes for equal distribution
            print("  Building DFC group sizes...")
            df_group_sizes = (
                df.group_by(best_key_cols)
                .agg(pl.len().alias("_dfc_group_size"))
            )

            # Attach group size to every DFC row
            df = df.join(df_group_sizes, on=best_key_cols, how="left")

            # B. Aggregate UPLF FTE columns by group (vertical sum)
            if fte_cols:
                print("  Aggregating UPLF FTE columns by group (vertical sum)...")
                fte_agg_exprs = [pl.col(c).cast(pl.Float64, strict=False).sum().alias(c) for c in fte_cols]
            else:
                fte_agg_exprs = []

            # String columns from UPLF we want to aggregate/deduplicate
            string_cols = []
            for c in ["Industry Segment", "Customer Classification"]:
                if c in df_uplf.columns:
                    string_cols.append(c)

            string_agg_exprs = []
            for c in string_cols:
                # Collect values per group; we'll dedupe/sort/join after aggregation
                string_agg_exprs.append(
                    pl.col(c)
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .drop_nulls()
                    .alias(c)
                )

            if fte_agg_exprs or string_agg_exprs:
                df_uplf_grouped = df_uplf.group_by(best_key_cols).agg(fte_agg_exprs + string_agg_exprs)

                # Turn aggregated lists into single, deduped, sorted strings per group
                for c in string_cols:
                    if c in df_uplf_grouped.columns:
                        df_uplf_grouped = df_uplf_grouped.with_columns(
                            pl.col(c)
                            .list.unique()
                            .list.sort()
                            .list.join(" | ")
                            .alias(c)
                        )
            else:
                df_uplf_grouped = df_uplf.select(best_key_cols).unique()

            print(f"  UPLF groups: {len(df_uplf_grouped)}")

            # C. Join aggregated UPLF data to DFC groups
            df_original_count = len(df)
            df = df.join(df_uplf_grouped, on=best_key_cols, how="left", suffix="_uplf")

            # D. Equal distribution of FTE totals across DFC rows in the same group
            if fte_cols:
                print("  Distributing aggregated FTE totals equally across DFC rows in each group...")
                for col in fte_cols:
                    # Use the aggregated value from UPLF side if present
                    source_col = col if col in df.columns else f"{col}_uplf"
                    if source_col not in df.columns:
                        # Ensure column exists with nulls so downstream logic is consistent
                        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(source_col))

                    df = df.with_columns(
                        (
                            pl.col(source_col).cast(pl.Float64, strict=False)
                            / pl.col("_dfc_group_size").cast(pl.Float64, strict=False)
                        ).alias(col)
                    )

                # Apply missing data rule for FTEs:
                # - If no matching UPLF group -> all FTE columns = 0 + add has_uplf_data flag
                # - Use 0 instead of -1 for CatBoost compatibility
                print("  Applying missing-data rules for FTE columns (no UPLF match → 0 + has_uplf_data flag)...")
                has_any_fte = None
                for col in fte_cols:
                    current = pl.col(col).is_not_null()
                    has_any_fte = current if has_any_fte is None else (has_any_fte | current)

                if has_any_fte is not None:
                    df = df.with_columns(
                        pl.when(has_any_fte)
                        .then(pl.lit(0))  # Temporary marker; will be overridden per-column
                        .otherwise(pl.lit(1))
                        .alias("_no_uplf_flag")
                    )

                    # Add explicit indicator flag for CatBoost to distinguish missing from actual zeros
                    # 1 = missing UPLF data, 0 = has UPLF data
                    df = df.with_columns([
                        pl.when(pl.col("_no_uplf_flag") == 1)
                        .then(pl.lit(1))  # 1 = missing UPLF data
                        .otherwise(pl.lit(0))  # 0 = has UPLF data
                        .alias("has_uplf_data")
                    ])

                    # Where _no_uplf_flag == 1, set all FTE cols to 0 (instead of -1); else fill nulls with 0
                    for col in fte_cols:
                        df = df.with_columns(
                            pl.when(pl.col("_no_uplf_flag") == 1)
                            .then(pl.lit(0.0))  # Use 0 instead of -1 for CatBoost
                            .otherwise(
                                pl.col(col)
                                .cast(pl.Float64, strict=False)
                                .fill_null(0.0)
                            )
                            .alias(col)
                        )

            # E. Handle aggregated string columns (deduplicated, sorted)
            if string_cols:
                print("  Assigning aggregated string columns from UPLF to all DFC rows in the group...")
                for c in string_cols:
                    source_col = c if c in df.columns else f"{c}_uplf"
                    if source_col in df.columns:
                        df = df.with_columns(
                            pl.col(source_col)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .alias(c)
                        )
                        # Drop suffix version if present
                        if f"{c}_uplf" in df.columns:
                            df = df.drop(f"{c}_uplf")

            # Drop helper columns
            if "_no_uplf_flag" in df.columns:
                df = df.drop("_no_uplf_flag")

            print(f"  ✓ Group-based UPLF mapping completed: {df_original_count} rows maintained, {len(df.columns)} columns")

            # STEP 6: Verify FTE distribution results
            print("\nSTEP 6: Verifying group-based FTE distribution...")
            if fte_cols:
                # Check using has_uplf_data flag instead of -1.0 values
                if 'has_uplf_data' in df.columns:
                    rows_with_uplf = df.filter(pl.col('has_uplf_data') == 0)
                    rows_missing_uplf = df.filter(pl.col('has_uplf_data') == 1)
                    print(
                        f"  ✓ Rows with valid UPLF data: "
                        f"{len(rows_with_uplf)} ({len(rows_with_uplf) / len(df) * 100:.1f}%)"
                    )
                    print(
                        f"  ✓ Rows missing UPLF data: "
                        f"{len(rows_missing_uplf)} ({len(rows_missing_uplf) / len(df) * 100:.1f}%)"
                    )
                else:
                    sample_fte_col = next((c for c in fte_cols if c in df.columns), None)
                    if sample_fte_col:
                        # Fallback: check for all-zero FTE columns (might indicate missing)
                        non_missing = df.filter(pl.col(sample_fte_col) != 0.0)
                        print(
                            f"  ✓ Rows with non-zero FTE data (sample col '{sample_fte_col}'): "
                            f"{len(non_missing)} ({len(non_missing) / len(df) * 100:.1f}%)"
                        )
                    else:
                        print("  ⚠ FTE columns found in UPLF but not present after mapping.")

        except Exception as e:
            print(f"  ✗ Group-based UPLF mapping failed: {e}")
            import traceback
            traceback.print_exc()
            # Ensure columns exist with conservative defaults
            print("  Falling back to conservative defaults for UPLF-derived columns...")
            # Add has_uplf_data flag set to 1 (missing) when mapping fails
            if 'has_uplf_data' not in df.columns:
                df = df.with_columns(pl.lit(1).alias('has_uplf_data'))
            for col in fte_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(0.0).alias(col))  # Use 0 instead of -1
            for c in ["Industry Segment", "Customer Classification"]:
                if c not in df.columns:
                    df = df.with_columns(pl.lit("").alias(c))

        # Ensure there are no nested list columns before downstream CSV writes
        list_cols = [
            name
            for name, dtype in zip(df.columns, df.dtypes)
            if isinstance(dtype, pl.List)
        ]
        if list_cols:
            print(f"  Converting list-typed columns to delimited strings for CSV compatibility: {list_cols}")
            df = df.with_columns(
                [pl.col(c).list.join(" | ").alias(c) for c in list_cols]
            )

        print(f"\n✓ UPLF mapping complete: {df.shape[0]} rows, {df.shape[1]} columns")
    else:
        print(f"UPLF file not found: {uplf_file}, proceeding with DFC data only")

    # Handle --no-open flag: filter out OPEN status rows and save original copy
    if no_open:
        print(f"\nFiltering out rows with SO Line Status = 'OPEN'...")

        # Save original file copy with _no_open suffix
        original_file_path = input_file.replace('.csv', '_no_open.csv')
        df.write_csv(original_file_path)
        print(f"  Original data saved to: {original_file_path}")

        # Check if 'SO Line Status' column exists
        if 'SO Line Status' not in df.columns:
            print(f"  Warning: 'SO Line Status' column not found. Available columns: {list(df.columns)}")
        else:
            # Filter out rows where SO Line Status contains 'OPEN' (case-insensitive)
            initial_count = len(df)
            df = df.filter(
                ~pl.col('SO Line Status').cast(pl.Utf8).str.to_uppercase().str.contains('OPEN')
            )
            filtered_count = initial_count - len(df)
            print(f"  Filtered out {filtered_count} rows with SO Line Status containing 'OPEN'")
            print(f"  Remaining rows: {len(df)} ({len(df)/initial_count*100:.1f}% of original)")

    # Grade combining already done before merging (STEP 2), skip duplicate
    print(f"\n✓ Grade combining already applied before merge")
    print(f"  Current SO GRADES: {df.select(pl.col('SO GRADE').unique().sort()).to_series().to_list()}")

    # Inspect and then DROP rows with null/empty values in group_by columns.
    # First we log diagnostics so you see exactly which columns have nulls/empties.
    if group_by and len(group_by) > 0:
        print(f"\nInspecting null/empty values in group_by columns: {group_by}...")
        total_rows = len(df)
        cols_with_missing = []
        filter_conditions = []
        for col in group_by:
            if col in df.columns:
                null_count = df.filter(pl.col(col).is_null()).height
                empty_count = df.filter(
                    pl.col(col).cast(pl.Utf8).str.strip_chars() == ''
                ).height
                if null_count > 0 or empty_count > 0:
                    cols_with_missing.append(col)
                # Condition to KEEP rows that are non-null and non-empty
                filter_conditions.append(
                    pl.col(col).is_not_null()
                    & (pl.col(col).cast(pl.Utf8).str.strip_chars() != '')
                )
                print(
                    f"  Column '{col}': nulls={null_count}, empty_strings={empty_count}, "
                    f"non_empty={total_rows - null_count - empty_count}"
                )
            else:
                print(f"  Column '{col}' not present in dataframe")

        if cols_with_missing:
            print(f"  Will drop rows with null/empty values in these group_by columns: {cols_with_missing}")
        else:
            print("  No null/empty values found in group_by columns.")

        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = combined_filter & condition

            initial_count = len(df)
            df = df.filter(combined_filter)
            filtered_count = initial_count - len(df)
            print(
                f"  Dropped {filtered_count} rows with null/empty group_by values; "
                f"remaining rows: {len(df)} ({len(df)/initial_count*100:.1f}% of original)"
            )
        else:
            print(f"  Warning: None of the group_by columns exist in the dataset")

    # Sort data chronologically by Requirement Start Date
    print("\nSorting by Requirement Start Date...")
    df = df.with_columns([
        parse_date_flexible(pl.col('Requirement Start Date')).alias('_sort_date')
    ])
    df = df.sort('_sort_date', nulls_last=True).drop('_sort_date')
    print(f"  Sorted {len(df)} rows by Requirement Start Date")

    # Create time-based features
    print("\nCreating time features...")
    df = create_time_features(df, date_format)

    # Filter out rows with Requirement Start Date before January of train_min_year
    print(f"\nFiltering out rows with Requirement Start Date before January {train_min_year}...")
    cutoff_date = datetime(train_min_year, 1, 1).date()
    initial_count = len(df)
    df = df.filter(pl.col('Req_Date') >= pl.lit(cutoff_date))
    filtered_count = initial_count - len(df)
    print(f"  Filtered out {filtered_count} rows with Req_Date before {train_min_year}-01-01")
    print(f"  Remaining rows: {len(df)} ({len(df)/initial_count*100:.1f}% of original)")

    # Quick year-wise row counts for Req_Date (after parsing)
    print("\nRow counts by Req_Year (after date parsing and filtering):")
    if 'Req_Year' in df.columns:
        df_nonnull = df.filter(pl.col('Req_Date').is_not_null())
        year_counts = (
            df_nonnull
            .group_by('Req_Year')
            .agg(pl.len().alias('row_count'))
            .sort('Req_Year')
        )
        for row in year_counts.to_dicts():
            print(f"  Year {int(row['Req_Year'])}: {row['row_count']} rows")
    else:
        print("  Req_Year column not available for summary.")

    # Step 11: Filter out groups with Total_Demand <= min_demand_threshold BEFORE splitting.
    # Filtering on the FULL dataset (before the train/test split) ensures consistency:
    # the same groups are removed from both train and test, preventing evaluation
    # on groups that were never seen during training.
    print(f"\nFiltering out groups with Total_Demand <= {min_demand_threshold} from FULL dataset (before split)...")
    demand_group_cols = ['Skill Cluster']
    
    # Check which grouping columns exist in the dataframe
    available_group_cols = [col for col in demand_group_cols if col in df.columns]
    excluded_groups = []
    
    if len(available_group_cols) == 0:
        print(f"  Warning: None of the demand grouping columns {demand_group_cols} found in dataset")
        print(f"  Skipping demand-based filtering")
    else:
        print(f"  Using grouping columns: {available_group_cols}")
        
        # Calculate Total_Demand per group on FULL dataset (before split)
        # This matches the logic in analyze_demand_tiers.py: each row = 1 demand unit
        group_demand = (
            df.group_by(available_group_cols)
            .agg(pl.len().alias('Total_Demand'))
        )
        
        # Identify excluded groups (Total_Demand <= threshold)
        excluded_groups_df = group_demand.filter(pl.col('Total_Demand') <= min_demand_threshold)
        excluded_groups = excluded_groups_df.select(available_group_cols).to_dicts()
        
        # Join back to tag each row with its group's Total_Demand
        df = df.join(group_demand, on=available_group_cols, how='left')
        
        # Filter out rows where Total_Demand <= min_demand_threshold
        initial_count = len(df)
        df = df.filter(pl.col('Total_Demand') > min_demand_threshold)
        filtered_count = initial_count - len(df)
        
        # Drop the temporary Total_Demand column
        df = df.drop('Total_Demand')
        
        print(f"  Filtered out {filtered_count} rows from {len(excluded_groups)} groups with Total_Demand <= {min_demand_threshold}")
        print(f"  Remaining rows: {len(df)} ({len(df)/initial_count*100:.1f}% of original)")
        
        # Save excluded groups to JSON file in data directory
        excluded_groups_file = 'data/excluded_groups.json'
        with open(excluded_groups_file, 'w') as f:
            json.dump(excluded_groups, f, indent=2)
        print(f"  Saved excluded groups list to: {excluded_groups_file} ({len(excluded_groups)} groups)")

    # Step 12: Chronological train/test split.
    # The split date (train_end = 2025-06-30) is FIXED so that the model only
    # ever sees historical demand.  The test set starts from the earliest
    # available date (for CAGR feature calculation) but build_training_groups.py
    # later restricts predictions to Jul-Dec 2025 cutoff dates.
    # Using a fixed date (rather than a random fraction) is critical for
    # time-series data: a random split would leak future patterns into training.
    print(f"\nCreating chronological split...")
    earliest_req_date = df['Req_Date'].min()
    train_start = earliest_req_date

    # Training: [earliest] -> train_end (includes all historical demand up to Jun 2025)
    df_train_raw = df.filter(
        (pl.col('Req_Date') >= pl.lit(train_start)) & (pl.col('Req_Date') <= pl.lit(train_end))
    )
    # Include FULL DATA for test set so CAGR and other historical metrics can be calculated
    # Will be filtered to July-December period during build/prediction phase
    df_test = df.filter(
        (pl.col('Req_Date') >= pl.lit(train_start)) & (pl.col('Req_Date') <= pl.lit(test_end))
    )

    if create_dev_split:
        # Separate dev split
        df_dev = df.filter(
            (pl.col('Req_Date') >= pl.lit(dev_start)) & (pl.col('Req_Date') <= pl.lit(dev_end))
        )
        print("\nData split (train/dev/test):")
        print(f"  Train period: {df_train_raw['Req_Date'].min()} -> {df_train_raw['Req_Date'].max()}  rows={len(df_train_raw)}")
        print(f"  Dev period:   {df_dev['Req_Date'].min()} -> {df_dev['Req_Date'].max()}  rows={len(df_dev)}")
        print(f"  Test period:  {df_test['Req_Date'].min()} -> {df_test['Req_Date'].max()}  rows={len(df_test)} (full data for CAGR calculations)")
    else:
        print("\nData split (train/test only):")
        print(f"  Train period: {df_train_raw['Req_Date'].min()} -> {df_train_raw['Req_Date'].max()}  rows={len(df_train_raw)}")
        print(f"  Test period:  {df_test['Req_Date'].min()} -> {df_test['Req_Date'].max()}  rows={len(df_test)} (full data for CAGR calculations)")

    # Step 13: Compute time-to-next-order targets ONLY on training data.
    # Computing this on test data would require knowing future order dates
    # (data leakage), so the feature is restricted to the training set only.
    # The function sorts rows by date within each group and computes the
    # number of days to the next demand event (censored at 365 days for the
    # last observation in each group).
    print(f"\nComputing time-to-next-order targets on TRAIN DATA ONLY ({group_desc})...")
    df_train = calculate_time_to_next_order(df_train_raw, group_by=group_by, date_format=date_format)

    # Safety guard: re-filter training rows to ensure no dates crept outside bounds
    # (e.g. from group-sorted rows reintroducing boundary rows)
    df_train = df_train.filter(
        (pl.col('Req_Date') >= pl.lit(train_start)) & (pl.col('Req_Date') <= pl.lit(train_end))
    )

    # Dev and test data retain raw features only; time-to-next-order is not applicable
    # because it would need future demand events that are not yet observed.
    if create_dev_split:
        print(f"\nDev and test data: keeping raw features only (no time-to-next-order targets computed)")
    else:
        print(f"\nTest data: keeping raw features only (no time-to-next-order targets computed)")

    # Plot demand vs group distribution on full train data (before any demand_pct limit) for clarity
    print("\nBuilding demand vs group distribution plots for all pipeline groupings (full train data, before demand filter)...")
    plot_demand_vs_group_distribution_all(df_train, output_dir=plots_dir)

    # Preserve unfiltered copies for the global model when --global-train-all is set
    # These will be saved alongside the demand-filtered versions
    df_train_global = df_train.clone() if global_train_all else None
    df_test_global = df_test.clone() if global_train_all else None

    # Filter train and test to groups covering demand_pct of train demand (sorted by demand desc)
    if group_by and demand_pct < 100.0 and all(c in df_train.columns for c in group_by):
        demand_per_group = (
            df_train.group_by(group_by)
            .agg(pl.len().alias('demand'))
            .sort('demand', descending=True)
        )
        if not demand_per_group.is_empty():
            total_demand = demand_per_group['demand'].sum()
            if total_demand > 0:
                cumsum = demand_per_group['demand'].cum_sum()
                threshold = total_demand * (demand_pct / 100.0)
                mask = cumsum >= threshold
                n_keep = (mask.arg_max() + 1) if mask.any() else len(demand_per_group)
                n_keep = min(n_keep, len(demand_per_group))
                top_keys_df = demand_per_group.head(n_keep).select(group_by)
                df_train = df_train.join(top_keys_df, on=group_by, how='inner')
                if all(c in df_test.columns for c in group_by):
                    df_test = df_test.join(top_keys_df, on=group_by, how='inner')
                print(f"\nDemand filter ({demand_pct}%): kept {n_keep} groups covering ~{demand_pct}% of train demand; train rows={len(df_train)}, test rows={len(df_test)}")

    # Save processed datasets under output_dir (e.g. data_BS)
    train_file = os.path.join(output_dir, 'train_data.csv')
    test_file = os.path.join(output_dir, 'test_data.csv')

    # Remove SO Line Status column from test data (already captured in train period)
    if 'SO Line Status' in df_test.columns:
        print(f"\nRemoving 'SO Line Status' column from test data...")
        df_test = df_test.drop('SO Line Status')

    # Strip both train and test to only essential columns for true forecasting.
    # Raw business fields + UPLF/FTE data are not available at real inference time;
    # only group identifiers + historical date records are known.
    print("\nStripping train data to essential columns (true forecasting mode)...")
    df_train = _strip_test_columns(df_train, group_by)
    print("\nStripping test data to essential columns (true forecasting mode)...")
    df_test = _strip_test_columns(df_test, group_by)

    print(f"\nSaving processed datasets...")
    df_train.write_csv(train_file)
    df_test.write_csv(test_file)

    print(f"  Train data saved to: {train_file} ({len(df_train)} rows)")
    print(f"  Test data saved to:  {test_file} ({len(df_test)} rows)")

    # Save unfiltered global data for the global model (all groups, no demand_pct filter)
    if global_train_all and df_train_global is not None and df_test_global is not None:
        train_global_file = os.path.join(output_dir, 'train_data_global.csv')
        test_global_file = os.path.join(output_dir, 'test_data_global.csv')
        # Same true-forecasting column strip for global train and test data
        print("\nStripping global train data to essential columns (true forecasting mode)...")
        df_train_global = _strip_test_columns(df_train_global, group_by)
        df_train_global.write_csv(train_global_file)
        print("\nStripping global test data to essential columns (true forecasting mode)...")
        df_test_global = _strip_test_columns(df_test_global, group_by)
        df_test_global.write_csv(test_global_file)
        print(f"  Global train data saved to: {train_global_file} ({len(df_train_global)} rows) [unfiltered, all groups]")
        print(f"  Global test data saved to:  {test_global_file} ({len(df_test_global)} rows) [unfiltered, all groups]")

    if create_dev_split:
        dev_file = os.path.join(output_dir, 'dev_data.csv')
        df_dev.write_csv(dev_file)
        print(f"  Dev data saved to:   {dev_file} ({len(df_dev)} rows)")

    # ── Pre-compute SSD floors ────────────────────────────────────────────────
    # Saves ssd_floors.csv alongside the train/test files so ssd_guardrail.py can
    # load it directly without re-reading the full raw CSV each time.
    print("\nPre-computing SSD floors for guardrail use...")
    try:
        ssd_cutoff_date = datetime.strptime(ssd_cutoff, '%Y-%m-%d').date()
    except ValueError:
        print(f"  Warning: could not parse --ssd-cutoff '{ssd_cutoff}'; defaulting to 2025-06-30.")
        ssd_cutoff_date = datetime(2025, 6, 30).date()
    floors_df = _compute_ssd_floors(
        input_file=input_file,
        group_cols=group_by,
        ssd_cutoff_date=ssd_cutoff_date,
        forecast_year=actual_test_start.year,
        forecast_month=actual_test_start.month,
    )
    if not floors_df.is_empty():
        floors_file = os.path.join(output_dir, 'ssd_floors.csv')
        floors_df.write_csv(floors_file)
        print(f"  SSD floors saved to: {floors_file} ({len(floors_df)} group x window rows)")
    else:
        print("  No SSD floors saved (no confirmed SOs found or missing date columns).")

    # Summary statistics
    print("\nProcessing complete!")
    print("Summary:")
    print(f"  Total rows processed: {len(df)}")
    print(f"  Training rows: {len(df_train)} ({len(df_train)/len(df)*100:.1f}%)")
    print(f"  Test rows: {len(df_test)} ({len(df_test)/len(df)*100:.1f}%) - full dataset for CAGR calculations")
    print(f"  Train period: {df_train['Req_Date'].min()} to {df_train['Req_Date'].max()}")
    print(f"  Test period:  {df_test['Req_Date'].min()} to {df_test['Req_Date'].max()} (full historical data)")
    
    # Print month-wise quantity breakdown for test data
    print("\nTest data month-wise quantity breakdown:")
    df_test_with_month = df_test.with_columns([
        pl.col('Req_Date').dt.year().alias('year'),
        pl.col('Req_Date').dt.month().alias('month')
    ])
    month_wise = df_test_with_month.group_by(['year', 'month']).agg([
        pl.count().alias('quantity')
    ]).sort(['year', 'month'])
    
    # Print month names for readability
    month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                   7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    total_test_qty = 0
    for row in month_wise.iter_rows(named=True):
        year = row['year']
        month = row['month']
        qty = row['quantity']
        total_test_qty += qty
        month_name = month_names.get(month, f'M{month}')
        print(f"  {month_name} {year}: {qty:,}")
    print(f"  Total: {total_test_qty:,}")
    
    # Also print for training data
    print("\nTraining data month-wise quantity breakdown:")
    df_train_with_month = df_train.with_columns([
        pl.col('Req_Date').dt.year().alias('year'),
        pl.col('Req_Date').dt.month().alias('month')
    ])
    train_month_wise = df_train_with_month.group_by(['year', 'month']).agg([
        pl.count().alias('quantity')
    ]).sort(['year', 'month'])
    
    total_train_qty = 0
    for row in train_month_wise.iter_rows(named=True):
        year = row['year']
        month = row['month']
        qty = row['quantity']
        total_train_qty += qty
        month_name = month_names.get(month, f'M{month}')
        print(f"  {month_name} {year}: {qty:,}")
    print(f"  Total: {total_train_qty:,}")

    if create_dev_split:
        dev_file = os.path.join(output_dir, 'dev_data.csv')
        print(f"  Dev rows: {len(df_dev)} ({len(df_dev)/len(df)*100:.1f}%)")
        print(f"  Dev period:   {df_dev['Req_Date'].min()} to {df_dev['Req_Date'].max()}")
        print("  Note: Test data includes full historical dataset for CAGR calculations")

        return {
            'train_file': train_file,
            'dev_file': dev_file,
            'test_file': test_file,
            'train_rows': len(df_train),
            'dev_rows': len(df_dev),
            'test_rows': len(df_test),
            'cutoff_date': test_start
        }
    else:
        return {
            'train_file': train_file,
            'test_file': test_file,
            'train_rows': len(df_train),
            'test_rows': len(df_test),
            'cutoff_date': test_start
        }


if __name__ == "__main__":
    # ── Argument parsing ──────────────────────────────────────────────────────
    # DEFAULT_ADDITIONAL_FEATURES lists all raw business columns that are available
    # in the DFC CSV and were historically passed to build_training_groups.py as
    # optional learning features.  These are NOT grouping columns; they are used
    # only for model training and are stripped from the output CSV by
    # _strip_test_columns (since they're unavailable at real inference time).
    # The list is curated from SHAP importance analysis across prior runs.
    DEFAULT_ADDITIONAL_FEATURES = [
        'Off/ On',
        'SO GRADE',
        'Project Billability Type',
        'BU',
        'SBU1',
        'Skill Cluster',
        'Vertical',
        'Practice',
        'SubVertical',
        'SubPractice',
        'Parent Customer',
        'Account Name',
        'Parent Customer',
        'Geography',
        'Market',
        'ServiceLine',
        'Practice Area',
        'Project Type',
        'SO TYPE',
        'Requirement type',
        'Revenue potential',
        'SO Line Status',
        'City'
    ]
    
    parser = argparse.ArgumentParser(description='Process and split data into train/test files with no leakage. '
                                                     'Use --no-open to filter out OPEN status rows.')

    # ── Input data paths ──────────────────────────────────────────────────────
    parser.add_argument('--input-file', default='DFC_YTD_2023-2025_v1_corrected_skill.csv',
                        help='Path to input CSV file (should include Skill Cluster column from skill_clustering.py)')
    parser.add_argument('--uplf-file', default='data/UPLF_full.csv',
                        help='Path to UPLF CSV file to merge with DFC data')
    # ── Grouping and features ─────────────────────────────────────────────────
    parser.add_argument('--group-by', nargs='*',
                        default=['BU', 'Skill Cluster'],
                        help='Column(s) to group by for time-to-next calculations. '
                             'Default: "Skill Cluster"')
    parser.add_argument('--additional-features', nargs='*',
                        default=DEFAULT_ADDITIONAL_FEATURES,
                        help='Additional feature columns to include for model learning (NOT grouping columns). '
                             f'Default: {DEFAULT_ADDITIONAL_FEATURES}')
    parser.add_argument('--date-format', default='%d/%m/%y',
                        help='Date format for parsing dates (default: %%d/%%m/%%y)')
    # ── Split strategy ────────────────────────────────────────────────────────
    parser.add_argument('--split-mode', choices=['train,test', 'train,dev,test'], default='train,test',
                        help='Split mode: "train,test" (default, dev data added to train) or '
                             '"train,dev,test" (separate train/dev/test splits)')
    # ── Filtering options ─────────────────────────────────────────────────────
    parser.add_argument('--no-open', action='store_true', default=False,
                        help='Filter out rows where SO Line Status is OPEN (case-insensitive) and save original copy (default: False)')
    parser.add_argument('--no-uplf', action='store_true', default=False,
                        help='Skip loading and merging UPLF data. UPLF columns will not be appended to DFC data (default: False)')
    parser.add_argument('--quarter-growth-file', default='data/Quarter_growth_by_country_uplf.csv',
                        help='Path to quarter growth CSV file to split into train/test (default: data/Quarter_growth_by_country_uplf.csv)')
    parser.add_argument('--exclude-grades', nargs='*', default=['D', "SR. DIR.", 'VP', 'AVP'],
                        help='SO GRADE values to exclude from the dataset (case-insensitive). '
                             'If empty, no grades are excluded. Example: --exclude-grades D "SR. DIR." VP AVP')
    parser.add_argument('--min-demand-threshold', type=int, default=36,
                        help='Minimum total demand threshold for keeping groups in training data. '
                             'Groups with Total_Demand <= this value will be filtered out (default: 36)')
    parser.add_argument('--demand-pct', type=float, default=80.0,
                        help='Keep groups that cumulatively cover this percentage of train demand; filter train and test to these groups (default: 80)')
    # ── Output directory ──────────────────────────────────────────────────────
    parser.add_argument('--results-dir', default=None,
                        help='Base results directory (e.g. results_S_m6_per_group). When set, data is written under <results-dir>/data_<GROUP> to align with run_pipeline.')
    parser.add_argument('--global-train-all', action='store_true', default=True,
                        help='Save additional unfiltered train/test CSVs (train_data_global.csv, test_data_global.csv) '
                             'for the global model to train on ALL data, not just demand_pct filtered groups.')
    # ── SSD floor pre-computation ─────────────────────────────────────────────
    parser.add_argument('--ssd-cutoff', default='2025-06-30',
                        help='SOs with SO Submission Date < this date are treated as confirmed for the SSD '
                             'floor pre-computation. Format: YYYY-MM-DD. Default: 2025-06-30')
    # ── Training window ───────────────────────────────────────────────────────
    parser.add_argument('--train-min-year', type=int, default=2023,
                        help='Exclude rows with Req_Date before January 1 of this year (default: 2023). '
                             'Set to 2024 to train only on 2024+ data.')
    # ── Publishing mode ───────────────────────────────────────────────────────
    parser.add_argument('--publishing', action='store_true', default=False,
                        help='Publishing mode: shift date boundaries so training covers 2024-Dec 2025 '
                             'and the forecast starts Jan 2026 (M0). Overrides train_end, test_start, '
                             'test_end, and actual_test_start regardless of --split-mode.')

    args = parser.parse_args()

    # Extract and echo the key configuration choices
    group_by = args.group_by
    additional_features = args.additional_features
    split_mode = args.split_mode

    print(f"Data split configuration: group_by={group_by}")
    print(f"  Split mode: {split_mode}")
    print(f"  Additional features (for learning): {additional_features}")

    results = main(
        input_file=args.input_file,
        uplf_file=args.uplf_file,
        group_by=group_by,
        date_format=args.date_format,
        split_mode=split_mode,
        no_open=args.no_open,
        quarter_growth_file=args.quarter_growth_file,
        exclude_grades=args.exclude_grades,
        min_demand_threshold=args.min_demand_threshold,
        no_uplf=args.no_uplf,
        demand_pct=args.demand_pct,
        results_dir=args.results_dir,
        global_train_all=args.global_train_all,
        ssd_cutoff=args.ssd_cutoff,
        train_min_year=args.train_min_year,
        publishing=args.publishing,
    )
