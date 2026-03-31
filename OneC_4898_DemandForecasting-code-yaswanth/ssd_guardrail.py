"""
SSD Guardrail - Post-correction script for AutoGluon predictions.

Applies the SSD (SO Submission Date) guardrail:
  For every group x month, count how many confirmed SOs already exist in the
  training-data window (SSD < ssd_cutoff) with their RSD in that forecast month.
  If our model predicted LESS than that confirmed count, bump the prediction up
  to the confirmed count (it's impossible to get fewer than what's already in
  the system).

Reads:
  - Combined Excel output from train_and_predict.py  (All_Predictions sheet)
  - Processed raw-data CSV  (has BU, Skill Cluster, Country, SO GRADE,
                              SO Submission Date, Requirement Start Date)
  - [Optional] test parquet to auto-read the forecast cutoff date

Writes a NEW Excel file with:
  - All_Predictions_SSD  : original rows + SSD floor + corrected predictions
                           + per-M accuracy columns (original and corrected)
  - SSD_Accuracy_Summary : one row per group, before/after accuracy side-by-side
  - SSD_Floors_Detail    : the confirmed floor counts used (transparency)

Usage:
    # Zero-arg: auto-detects latest results_BS_DE_*_Americas/ dir, Excel, and floors CSV
    py ssd_guardrail.py

    # Different market / practice area
    py ssd_guardrail.py --market EMEA --practice-area DA

    # Explicit paths (fully manual)
    py ssd_guardrail.py \\
        --input-excel  results_BS_DE_m6_per_group_Americas/combined_group_results_*.xlsx \\
        --floors-csv   results_BS_DE_m6_per_group_Americas/data_BS/ssd_floors.csv \\
        --ssd-cutoff   2025-06-30 \\
        --output-dir   results_BS_DE_m6_per_group_Americas
"""

import sys
import os
import re
import logging
import argparse
import warnings
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd
import coloredlogs

warnings.filterwarnings('ignore')
coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s - %(message)s', isatty=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# _META_COLS: Excel columns that carry metadata (model name, source tag, group
# label) rather than group-dimension values.  They must be excluded when
# detecting which columns define the prediction group key so that the guardrail
# join logic does not try to match on these administrative fields.
_META_COLS = {'Group_Label', 'Model_Name', 'Prediction_Source'}

# Raw-data column names as they appear in the processed skill_clusters CSV.
# Defined here once so any future column rename only needs a single update.
_RAW_SSD_COL   = 'SO Submission Date'       # when the SO was submitted to the system
_RAW_RSD_COL   = 'Requirement Start Date'   # when the resource need starts
_RAW_BU_COL    = 'BU'                       # Business Unit
_RAW_SC_COL    = 'Skill Cluster'            # skill cluster taxonomy
_RAW_CTRY_COL  = 'Country'                  # delivery country
_RAW_GRADE_COL  = 'SO GRADE'               # seniority grade
_RAW_STATUS_COL = 'SO Line Status'          # used for --exclude-open filter

# _GROUP_COL_MAP maps the Excel prediction column names (as they appear in the
# All_Predictions sheet) to the corresponding raw CSV column names.  In the
# current data they are identical, but this mapping makes the code resilient to
# any future renaming in the Excel output layer.
_GROUP_COL_MAP = {
    'BU':            _RAW_BU_COL,
    'Skill Cluster': _RAW_SC_COL,
    'Country':       _RAW_CTRY_COL,
    'SO GRADE':      _RAW_GRADE_COL,
}

# ─────────────────────────────────────────────────────────────────────────────
# ARG PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    """Parse command-line arguments for the SSD guardrail.

    Arguments are grouped into three categories:

    Auto-detect shortcuts:
        --mode, --practice-area, --market
          Used to construct the glob pattern results_{mode}_{pa}_*_{market}/
          for auto-detecting the latest results directory.  If you always run
          with --results-dir, these three can be ignored.

    Explicit paths (override auto-detection):
        --results-dir     Pinpoint the results directory directly.
        --input-excel     Combined Excel from train_and_predict.py.
        --floors-csv      Pre-computed ssd_floors.csv from data_split.py.
        --raw-data        Raw DFC CSV (only needed if ssd_floors.csv is absent).
        --test-parquet    Test parquet used to auto-read the forecast cutoff date.
        --output-dir      Where to write the guardrail Excel and JSON.

    Behaviour options:
        --ssd-cutoff      Date boundary: SOs submitted before this are confirmed.
        --cutoff-year/month  M0 of the forecast (overridden by --test-parquet).
        --exclude-open    Drop OPEN/Cancelled SOs from the floor count.
        --sheet-name      Sheet in the input Excel that holds predictions.
    """
    p = argparse.ArgumentParser(
        description='Apply SSD guardrail to AutoGluon prediction Excel outputs. '
                    'Run with no arguments to auto-detect the latest results (mode=BS).'
    )

    # ── Auto-detect shortcuts ──────────────────────────────────────────────
    # These three together form the glob: results_{mode}_{practice_area}_*_{market}/
    p.add_argument('--mode', default='S',
                   help='Pipeline mode key used in directory names (e.g. BS, UPLF). '
                        'Used for auto-detecting results dir and data subdir. Default: BS')
    p.add_argument('--practice-area', default='DE',
                   help='Practice-area code used in directory names (e.g. DE, DA). '
                        'Used for auto-detecting results dir. Default: DE')
    p.add_argument('--market', default='Americas',
                   help='Market label used in directory names. '
                        'Used for auto-detecting results dir. Default: Americas')
    p.add_argument('--results-dir', default=None,
                   help='Explicit results directory. If omitted, auto-detected as the '
                        'latest results_{mode}_{pa}_*_{market}/ folder in the current dir.')

    # ── Core paths ────────────────────────────────────────────────────────
    # Floor source priority: --floors-csv > auto-detect ssd_floors.csv > --raw-data
    p.add_argument('--input-excel', default=None,
                   help='Path to combined Excel from train_and_predict.py. '
                        'Auto-detected as the latest combined_group_results_*.xlsx '
                        '(excluding *_SSD_guardrail_*.xlsx) in the results dir if omitted.')
    p.add_argument('--raw-data', default=None,
                   help='Processed CSV with BU, Skill Cluster, SO Submission Date, '
                        'Requirement Start Date. Used to compute SSD floors on the fly. '
                        'Not needed when --floors-csv is provided or auto-detected.')
    p.add_argument('--floors-csv', default=None,
                   help='Pre-computed SSD floors CSV (produced by data_split.py and saved as '
                        'ssd_floors.csv in the data dir). When provided, skips re-reading '
                        '--raw-data entirely. Auto-detected from data_{mode}/ssd_floors.csv '
                        'in the results dir if neither --floors-csv nor --raw-data is given.')

    # ── Forecast / cutoff settings ────────────────────────────────────────
    p.add_argument('--ssd-cutoff', default='2025-06-30',
                   help='SOs with SO Submission Date < this date count as confirmed '
                        '(training-window SOs). Format: YYYY-MM-DD. Default: 2025-06-30')
    p.add_argument('--cutoff-year', type=int, default=2025,
                   help='Year of the forecast cutoff (M0 = cutoff_year/cutoff_month). '
                        'Default: 2025')
    p.add_argument('--cutoff-month', type=int, default=7,
                   help='Month of the forecast cutoff (1-12). M0=July -> 7. Default: 7')
    p.add_argument('--test-parquet', default=None,
                   help='[Optional] Test parquet from build_training_groups.py. '
                        'If provided, cutoff_date is read from it automatically '
                        '(overrides --cutoff-year / --cutoff-month). '
                        'Auto-detected from data_{mode}/test_dataset.parquet if omitted.')

    # ── Behaviour options ─────────────────────────────────────────────────
    p.add_argument('--exclude-open', action='store_true', default=False,
                   help='Exclude SOs with SO Line Status containing "OPEN" or "Cancelled" '
                        'from the floor calculation. Use this if your test actuals were also '
                        'built without OPEN/Cancelled SOs (prevents floor > actual artefacts).')
    p.add_argument('--sheet-name', default='All_Predictions',
                   help='Sheet in input Excel containing predictions. Default: All_Predictions')
    p.add_argument('--output-dir', default=None,
                   help='Directory for the output Excel. Defaults to the results dir.')
    p.add_argument('--publishing', action='store_true', default=False,
                   help='Publishing mode: overrides ssd-cutoff to 2025-12-31, '
                        'cutoff-year to 2026, cutoff-month to 1 (M0 = Jan 2026).')
    args = p.parse_args()

    # ── Publishing mode: override date settings ────────────────────────────
    if args.publishing:
        args.ssd_cutoff = '2025-12-31'
        args.cutoff_year = 2026
        args.cutoff_month = 1
        logging.info('[Publishing mode] ssd_cutoff=2025-12-31, cutoff=2026-01 (M0=Jan 2026)')

    return args


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-DETECT PATHS
# ─────────────────────────────────────────────────────────────────────────────

def _auto_detect_paths(args) -> None:
    """Fill in missing path args from the results directory structure.

    Resolves (in order):
      1. results_dir  — from --results-dir or glob results_{mode}_{pa}_*_{market}/ in cwd
      2. data_dir     — {results_dir}/data_{mode}/
      3. test_parquet — data_dir/test_dataset.parquet
      4. floors_csv   — data_dir/ssd_floors.csv  (only if raw_data also absent)
      5. input_excel  — latest combined_group_results_*.xlsx (not _SSD_guardrail_)
      6. output_dir   — results_dir

    Mutates *args* in place; logs each auto-detection.
    """
    import glob as _glob

    # 1 ── Resolve results_dir ─────────────────────────────────────────────
    results_dir = args.results_dir
    if not results_dir:
        pattern = f'results_{args.mode}_{args.practice_area}_*_{args.market}'
        candidates = sorted(
            [d for d in _glob.glob(pattern) if os.path.isdir(d)],
            key=os.path.getmtime,
            reverse=True,
        )
        if candidates:
            results_dir = candidates[0]
            logging.info('Auto-detected results dir: %s', results_dir)

    if not results_dir:
        # Nothing to auto-detect — main() will validate required args later
        return

    # 2 ── Data subdir ─────────────────────────────────────────────────────
    data_dir = os.path.join(results_dir, f'data_{args.mode}')

    # 3 ── test_parquet ────────────────────────────────────────────────────
    if not args.test_parquet:
        tp = os.path.join(data_dir, 'test_dataset.parquet')
        if os.path.exists(tp):
            args.test_parquet = tp
            logging.info('Auto-detected test parquet: %s', tp)

    # 4 ── floors_csv ──────────────────────────────────────────────────────
    if not args.floors_csv and not args.raw_data:
        fc = os.path.join(data_dir, 'ssd_floors.csv')
        if os.path.exists(fc):
            args.floors_csv = fc
            logging.info('Auto-detected floors CSV: %s', fc)

    # 4b ── raw_data fallback (when ssd_floors.csv absent) ─────────────────
    # If neither floors_csv nor raw_data is resolved, look for the DFC raw CSV.
    # Prefer skill_clusters files; exclude unmapped files (they lack Skill Cluster col).
    if not args.floors_csv and not args.raw_data:
        def _rank_raw(p):
            name = os.path.basename(p).lower()
            if 'skill_cluster' in name:
                return 0   # best: has skill clusters
            if 'unmapped' in name:
                return 2   # worst: missing skill cluster column
            return 1

        all_candidates = _glob.glob(
            os.path.join('data', args.practice_area, f'DFC_*_{args.market}.csv')
        )
        if not all_candidates:
            all_candidates = _glob.glob(
                os.path.join('data', '**', 'DFC_*.csv'), recursive=True
            )
        ranked = sorted(all_candidates, key=lambda p: (_rank_raw(p), -os.path.getmtime(p)))
        if ranked:
            args.raw_data = ranked[0]
            logging.info('Auto-detected raw data CSV (no ssd_floors.csv found): %s', args.raw_data)

    # 5 ── input_excel ─────────────────────────────────────────────────────
    if not args.input_excel:
        xl_pattern = os.path.join(results_dir, 'combined_group_results_*.xlsx')
        excels = sorted(
            [f for f in _glob.glob(xl_pattern)
             if '_SSD_guardrail_' not in os.path.basename(f)],
            key=os.path.getmtime,
            reverse=True,
        )
        if excels:
            args.input_excel = excels[0]
            logging.info('Auto-detected input Excel: %s', args.input_excel)

    # 6 ── output_dir ──────────────────────────────────────────────────────
    if not args.output_dir:
        args.output_dir = results_dir
        logging.info('Auto-set output dir: %s', results_dir)


# ─────────────────────────────────────────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_flexible(val) -> date | None:
    """Parse a date from a mixed-format string or pd.Timestamp."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.date() if hasattr(val, 'date') else val
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'nat', 'none', ''):
        return None
    # Only two accepted formats: ISO (YYYY-MM-DD) and Month/Day/Year (MM/DD/YYYY or MM-DD-YYYY).
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _months_diff(d: date, cutoff_year: int, cutoff_month: int) -> int:
    """How many months is d ahead of (cutoff_year, cutoff_month)?
    Returns 0 if d is in the same month, negative if before cutoff."""
    return (d.year - cutoff_year) * 12 + (d.month - cutoff_month)


# ─────────────────────────────────────────────────────────────────────────────
# CUTOFF DETECTION FROM PARQUET
# ─────────────────────────────────────────────────────────────────────────────

def _read_cutoff_from_parquet(parquet_path: str, cutoff_year: int, cutoff_month: int):
    """Try to read cutoff_date from test parquet. Returns (year, month) or the
    passed defaults on failure."""
    try:
        import polars as pl
        df = pl.read_parquet(parquet_path)
        if 'cutoff_date' not in df.columns:
            logging.warning('  Parquet has no cutoff_date column; using --cutoff-year/month.')
            return cutoff_year, cutoff_month
        # test set may have a single cutoff or a few near-identical ones; take max
        cutoff_series = df['cutoff_date'].drop_nulls()
        if cutoff_series.is_empty():
            return cutoff_year, cutoff_month
        latest = cutoff_series.cast(pl.Utf8).to_list()
        # parse the latest date
        parsed = [_parse_date_flexible(v) for v in latest if _parse_date_flexible(v)]
        if not parsed:
            return cutoff_year, cutoff_month
        latest_date = max(parsed)
        logging.info('  Auto-detected cutoff from parquet: %s -> M0=%s-%02d',
                     latest_date, latest_date.year, latest_date.month)
        return latest_date.year, latest_date.month
    except Exception as e:
        logging.warning('  Could not read cutoff from parquet (%s); using defaults.', e)
        return cutoff_year, cutoff_month


# ─────────────────────────────────────────────────────────────────────────────
# LOAD PREDICTIONS EXCEL
# ─────────────────────────────────────────────────────────────────────────────

def _load_predictions(excel_path: str, sheet_name: str) -> pd.DataFrame:
    """Load the All_Predictions sheet from the combined AutoGluon results Excel.

    Args:
        excel_path: Path to the combined Excel file produced by train_and_predict.py.
        sheet_name: Name of the sheet containing predictions (default: 'All_Predictions').

    Returns:
        pandas DataFrame with one row per group x model combination, containing
        M{w}_Actual and M{w}_Predicted columns for each forecast window.
    """
    logging.info('Loading predictions from %s [sheet=%s]', excel_path, sheet_name)
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    logging.info('  Loaded %d rows x %d cols', len(df), len(df.columns))
    return df


def _detect_m_columns(df: pd.DataFrame) -> list[int]:
    """Return sorted list of M-window indices found in columns (e.g. [0,1,2,3,4,5])."""
    windows = set()
    for col in df.columns:
        m = re.match(r'^M(\d+)_(Actual|Predicted)$', str(col))
        if m:
            windows.add(int(m.group(1)))
    return sorted(windows)


def _detect_group_cols(df: pd.DataFrame, m_windows: list[int]) -> list[str]:
    """Return all columns that are NOT M*_Actual/Predicted and NOT meta cols."""
    m_pattern = re.compile(r'^M\d+_(Actual|Predicted)$')
    skip = _META_COLS.copy()
    group_cols = [
        c for c in df.columns
        if c not in skip and not m_pattern.match(str(c))
    ]
    return group_cols


# ─────────────────────────────────────────────────────────────────────────────
# BUILD SSD FLOORS
# ─────────────────────────────────────────────────────────────────────────────

def build_ssd_floors(raw_csv: str, ssd_cutoff_date: date,
                     cutoff_year: int, cutoff_month: int,
                     group_cols_excel: list[str],
                     m_windows: list[int],
                     exclude_open: bool = False,
                     output_dir: str = None) -> pd.DataFrame:
    """
    Load processed CSV, filter SSD < ssd_cutoff_date, compute confirmed SO counts
    per (group_cols x M-window).

    When exclude_open=True, rows with SO Line Status containing 'OPEN' or 'Cancelled'
    are dropped — use this when your test actuals were also built without those rows,
    to avoid SSD floor > Actual artefacts.

    The final filtered rows (used to derive the floors) are saved as a CSV to output_dir
    for full transparency/auditing.

    Returns a DataFrame with columns = group_cols + ['window', 'ssd_floor_count'].
    """
    logging.info('Loading raw data from %s', raw_csv)
    raw = pd.read_csv(raw_csv, low_memory=False)
    logging.info('  Raw data: %d rows x %d cols', len(raw), len(raw.columns))

    # Map Excel group-col names to raw CSV col names (usually identical)
    raw_col_for = {}
    for gc in group_cols_excel:
        mapped = _GROUP_COL_MAP.get(gc, gc)
        if mapped in raw.columns:
            raw_col_for[gc] = mapped
        elif gc in raw.columns:
            raw_col_for[gc] = gc
        else:
            logging.warning('  Group col "%s" not found in raw CSV; will be NaN.', gc)
            raw_col_for[gc] = None

    # ── Optional: exclude OPEN / Cancelled SOs ────────────────────────────
    # Why this matters: OPEN/Cancelled SOs were submitted before the cutoff
    # (so they count in the SSD floor) but they are typically NOT counted in
    # the test actuals (which reflect fulfilled demand). Without this filter
    # the floor can exceed the actual, making the correction hurt accuracy.
    if exclude_open and _RAW_STATUS_COL in raw.columns:
        before = len(raw)
        status_upper = raw[_RAW_STATUS_COL].astype(str).str.upper()
        mask_exclude = status_upper.str.contains('OPEN') | status_upper.str.contains('CANCEL')
        raw = raw[~mask_exclude].copy()
        logging.info('  --exclude-open: removed %d OPEN/Cancelled rows; %d remain',
                     before - len(raw), len(raw))
    elif exclude_open:
        logging.warning('  --exclude-open set but column "%s" not found; skipping filter.',
                        _RAW_STATUS_COL)

    # Parse SSD
    raw['_ssd_parsed'] = raw[_RAW_SSD_COL].apply(_parse_date_flexible)
    n_before = len(raw)
    raw = raw[raw['_ssd_parsed'].notna()]
    logging.info('  %d rows have parseable SSD (dropped %d)', len(raw), n_before - len(raw))

    # Filter to SSD < ssd_cutoff_date  (confirmed = already in system)
    raw = raw[raw['_ssd_parsed'] < ssd_cutoff_date].copy()
    logging.info('  %d rows with SSD < %s (confirmed training-window SOs)', len(raw), ssd_cutoff_date)

    # Parse RSD
    raw['_rsd_parsed'] = raw[_RAW_RSD_COL].apply(_parse_date_flexible)
    raw = raw[raw['_rsd_parsed'].notna()]
    logging.info('  %d rows with parseable RSD', len(raw))

    # Compute which M-window each RSD belongs to
    raw['_window'] = raw['_rsd_parsed'].apply(
        lambda d: _months_diff(d, cutoff_year, cutoff_month)
    )

    # Keep only windows we care about
    raw = raw[raw['_window'].isin(m_windows)].copy()
    logging.info('  %d rows whose RSD falls in forecast windows %s', len(raw), m_windows)

    if raw.empty:
        logging.warning('  No confirmed SOs found in forecast windows; floors will all be 0.')
        return pd.DataFrame(columns=group_cols_excel + ['window', 'ssd_floor_count'])

    # Build group key columns
    for gc in group_cols_excel:
        rc = raw_col_for.get(gc)
        if rc:
            raw[gc] = raw[rc].astype(str).str.strip()
        else:
            raw[gc] = 'UNKNOWN'

    # ── Save the filtered rows used for floor computation ─────────────────
    # Provides full auditability: you can open this CSV and count the exact
    # SOs that drove each floor value.
    if output_dir:
        try:
            os.makedirs(output_dir, exist_ok=True)
            ts = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            save_cols = group_cols_excel + [_RAW_SSD_COL, _RAW_RSD_COL, '_window']
            if _RAW_STATUS_COL in raw.columns:
                save_cols = [_RAW_STATUS_COL] + save_cols
            # keep only cols that exist
            save_cols = [c for c in save_cols if c in raw.columns]
            csv_path = os.path.join(output_dir, f'ssd_floor_source_rows_{ts}.csv')
            raw[save_cols].rename(columns={'_window': 'M_Window'}).to_csv(csv_path, index=False)
            logging.info('  Floor source rows saved to: %s', csv_path)
        except Exception as e:
            logging.warning('  Could not save floor source CSV: %s', e)

    grp = group_cols_excel + ['_window']
    floors_df = (
        raw.groupby(grp, dropna=False)
        .size()
        .reset_index(name='ssd_floor_count')
        .rename(columns={'_window': 'window'})
    )
    logging.info('  Computed SSD floors for %d group x window combinations', len(floors_df))
    return floors_df


# ─────────────────────────────────────────────────────────────────────────────
# ACCURACY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _row_accuracy(actual, predicted) -> float | None:
    """Single-row MAPE-accuracy (%). Returns None if actual == 0."""
    a, p = float(actual or 0), float(predicted or 0)
    if a <= 0:
        return None
    ape = min(1.0, abs(p - a) / a)
    return round((1.0 - ape) * 100, 2)


def _overall_accuracy(actuals, predicteds) -> float | None:
    """MAPE accuracy across multiple (actual, predicted) pairs, non-zero actuals only."""
    pairs = [(float(a), float(p)) for a, p in zip(actuals, predicteds)
             if a is not None and p is not None and float(a or 0) > 0]
    if not pairs:
        return None
    apes = [min(1.0, abs(p - a) / a) for a, p in pairs]
    return round((1.0 - np.mean(apes)) * 100, 2)


# ─────────────────────────────────────────────────────────────────────────────
# APPLY GUARDRAIL
# ─────────────────────────────────────────────────────────────────────────────

def apply_ssd_guardrail(pred_df: pd.DataFrame,
                        floors_df: pd.DataFrame,
                        group_cols: list[str],
                        m_windows: list[int]) -> pd.DataFrame:
    """Apply the SSD floor guardrail to every row and window in the predictions.

    Guardrail concept:
        Confirmed SOs (SO Submission Date before the training cutoff) with
        Requirement Start Date in a given forecast window are already in the
        system.  The model cannot predict fewer than this confirmed count.
        Therefore: corrected_prediction = max(original_prediction, ssd_floor).

    For each row in pred_df, for each M-window, the following columns are added:
      M{w}_SSD_Floor             -- the confirmed floor count for that group x window
      M{w}_Predicted_Corrected   -- max(original, floor)
      M{w}_Correction_Applied    -- True if the prediction was bumped upward
      M{w}_Accuracy_%_Original   -- row-level MAPE accuracy with original prediction
      M{w}_Accuracy_%_Corrected  -- row-level MAPE accuracy with corrected prediction

    Summary flags added per row:
      Any_SSD_Correction   -- True if at least one month was corrected
      N_Months_Corrected   -- count of months where a correction was applied

    Args:
        pred_df:    pandas DataFrame loaded from the All_Predictions sheet.
        floors_df:  pandas DataFrame with columns [*group_cols, 'window',
                    'ssd_floor_count'] (from ssd_floors.csv or build_ssd_floors).
        group_cols: List of column names that form the prediction group key.
        m_windows:  List of integer M-window offsets to process.

    Returns:
        pandas DataFrame: pred_df with the additional guardrail columns appended.
    """
    # Step 1: Build a fast O(1) lookup dict: (group_key_tuple, window) -> floor_count.
    # Using a dict keyed by tuple avoids repeated DataFrame filtering inside the loop.
    floor_lookup: dict = {}
    if not floors_df.empty:
        for _, row in floors_df.iterrows():
            key = tuple(str(row[gc]).strip() for gc in group_cols)
            floor_lookup[(key, int(row['window']))] = int(row['ssd_floor_count'])

    # Step 2: Iterate over every prediction row and apply the guardrail per window.
    out_rows = []
    for _, row in pred_df.iterrows():
        new_row = dict(row)
        # Step 3: Build the group key tuple for this row so we can look up the floor.
        # Normalise to strings and strip whitespace to match how the floor dict was keyed.
        group_key = tuple(
            str(row[gc]).strip() if gc in row.index and pd.notna(row[gc]) else 'nan'
            for gc in group_cols
        )

        any_correction = False
        n_corrected = 0

        for w in m_windows:
            pred_col   = f'M{w}_Predicted'
            actual_col = f'M{w}_Actual'
            floor_col  = f'M{w}_SSD_Floor'
            corr_col   = f'M{w}_Predicted_Corrected'
            flag_col   = f'M{w}_Correction_Applied'
            acc_orig   = f'M{w}_Accuracy_%_Original'
            acc_corr   = f'M{w}_Accuracy_%_Corrected'

            predicted = float(row.get(pred_col, 0) or 0)
            actual    = float(row.get(actual_col, 0) or 0)
            # Step 4: Look up the confirmed floor for this (group, window) pair.
            # If no confirmed SOs exist for this combination, the floor defaults to 0
            # (no guardrail adjustment needed).
            floor     = floor_lookup.get((group_key, w), 0)

            # Step 5: Apply the guardrail — the corrected prediction is the larger of
            # the model output and the confirmed floor count.
            corrected  = max(predicted, floor)
            corrected  = round(corrected, 4)
            correction = corrected > predicted  # True if we bumped it up

            new_row[floor_col] = floor
            new_row[corr_col]  = corrected
            new_row[flag_col]  = correction
            new_row[acc_orig]  = _row_accuracy(actual, predicted)
            new_row[acc_corr]  = _row_accuracy(actual, corrected)

            if correction:
                any_correction = True
                n_corrected   += 1

        new_row['Any_SSD_Correction'] = any_correction
        new_row['N_Months_Corrected'] = n_corrected
        out_rows.append(new_row)

    return pd.DataFrame(out_rows)


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY SHEET
# ─────────────────────────────────────────────────────────────────────────────

def _build_group_rows(df: pd.DataFrame,
                      group_cols: list[str],
                      m_windows: list[int]) -> list[dict]:
    """Aggregate one summary dict per group-key. Core computation only."""
    rows = []
    grouped = df.groupby(group_cols, dropna=False) if group_cols else [((), df)]
    for group_vals, grp in grouped:
        rec = {}
        if group_cols:
            if not isinstance(group_vals, tuple):
                group_vals = (group_vals,)
            for gc, gv in zip(group_cols, group_vals):
                rec[gc] = gv

        all_actuals, all_preds_orig, all_preds_corr = [], [], []

        for w in m_windows:
            actual_col = f'M{w}_Actual'
            pred_col   = f'M{w}_Predicted'
            corr_col   = f'M{w}_Predicted_Corrected'

            acts  = grp[actual_col].fillna(0).tolist() if actual_col in grp else []
            preds = grp[pred_col].fillna(0).tolist()   if pred_col  in grp else []
            corrs = grp[corr_col].fillna(0).tolist()   if corr_col  in grp else []

            all_actuals.extend(acts)
            all_preds_orig.extend(preds)
            all_preds_corr.extend(corrs)

            rec[f'M{w}_Total_Actual']             = round(sum(acts),  2)
            rec[f'M{w}_Pred_Original']            = round(sum(preds), 2)
            rec[f'M{w}_Pred_Corrected']           = round(sum(corrs), 2)
            rec[f'M{w}_Accuracy_%_Original']      = _overall_accuracy(acts, preds)
            rec[f'M{w}_Accuracy_%_Corrected']     = _overall_accuracy(acts, corrs)

        rec['Total_Actual']                 = round(sum(all_actuals), 2)
        rec['Total_Pred_Original']          = round(sum(all_preds_orig), 2)
        rec['Total_Pred_Corrected']         = round(sum(all_preds_corr), 2)
        rec['Overall_Accuracy_Original_%']  = _overall_accuracy(all_actuals, all_preds_orig)
        rec['Overall_Accuracy_Corrected_%'] = _overall_accuracy(all_actuals, all_preds_corr)
        rec['Any_SSD_Correction']           = bool(grp['Any_SSD_Correction'].any()) \
                                              if 'Any_SSD_Correction' in grp else False
        rec['N_Months_Corrected_Max']       = int(grp['N_Months_Corrected'].max()) \
                                              if 'N_Months_Corrected' in grp else 0
        rows.append(rec)
    return rows


def _compute_weighted_avg_row(summary: pd.DataFrame,
                               group_cols: list[str],
                               label: str = 'Weighted Avg') -> dict:
    """Weighted avg over summary rows (weight = Total_Actual). Returns a dict."""
    if summary.empty or 'Total_Actual' not in summary.columns:
        return {}
    weights = summary['Total_Actual'].fillna(0)
    wavg: dict = {gc: (label if i == 0 else '') for i, gc in enumerate(group_cols)}
    numeric_cols = summary.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        if col == 'Total_Actual':
            wavg[col] = round(float(summary[col].sum()), 2)
        elif col == 'N_Months_Corrected_Max':
            wavg[col] = np.nan
        elif (col.endswith('_Accuracy_%_Original') or col.endswith('_Accuracy_%_Corrected') or
              col in ('Overall_Accuracy_Original_%', 'Overall_Accuracy_Corrected_%')):
            valid = summary[col].notna() & (weights > 0)
            if valid.any() and weights[valid].sum() > 0:
                wavg[col] = round(
                    float((summary.loc[valid, col] * weights[valid]).sum() / weights[valid].sum()), 2
                )
            else:
                wavg[col] = np.nan
        else:
            wavg[col] = round(float(summary[col].fillna(0).sum()), 2)
    if 'Any_SSD_Correction' in summary.columns:
        wavg['Any_SSD_Correction'] = bool(summary['Any_SSD_Correction'].any())
    return wavg


def _reorder_summary_cols(df: pd.DataFrame, group_cols: list[str], m_windows: list[int]) -> pd.DataFrame:
    """
    Reorder SSD_Accuracy_Summary columns so overall accuracy appears first:
      group_cols | Overall_Accuracy_Original_% | Overall_Accuracy_Corrected_%
                 | Total_Actual | Total_Pred_Original | Total_Pred_Corrected
                 | per-window metrics (M0, M1, ...) | flags
    """
    ordered = list(group_cols)
    for col in ['Overall_Accuracy_Original_%', 'Overall_Accuracy_Corrected_%']:
        if col in df.columns:
            ordered.append(col)
    for col in ['Total_Actual', 'Total_Pred_Original', 'Total_Pred_Corrected']:
        if col in df.columns:
            ordered.append(col)
    for w in m_windows:
        for suffix in ['_Total_Actual', '_Pred_Original', '_Pred_Corrected',
                       '_Accuracy_%_Original', '_Accuracy_%_Corrected']:
            c = f'M{w}{suffix}'
            if c in df.columns:
                ordered.append(c)
    for col in ['Any_SSD_Correction', 'N_Months_Corrected_Max']:
        if col in df.columns:
            ordered.append(col)
    remaining = [c for c in df.columns if c not in ordered]
    ordered += remaining
    return df[[c for c in ordered if c in df.columns]]


def build_summary(corrected_df: pd.DataFrame,
                  group_cols: list[str],
                  m_windows: list[int]) -> tuple:
    """
    Build the SSD_Accuracy_Summary sheet.

    Individual-model rows form the main section (non-REMAINDER groups first,
    REMAINDER groups after a blank-row separator).  Global-model rows are
    appended after two additional blank rows so all groups are represented.

    Overall_Accuracy columns are placed first (after group_cols) via
    _reorder_summary_cols().

    Returns:
        (full_summary_df, main_individual_df, main_wavg_dict)
        - full_summary_df    : assembled sheet (blank separators + weighted avg rows)
        - main_individual_df : raw per-group rows for non-REMAINDER Individual groups
        - main_wavg_dict     : weighted avg dict for those groups (used in JSON export)
    """
    # ── Separate Individual from Global rows ─────────────────────────────────
    if 'Prediction_Source' in corrected_df.columns:
        ind_df  = corrected_df[corrected_df['Prediction_Source'] == 'Individual'].copy()
        glob_df = corrected_df[corrected_df['Prediction_Source'] != 'Individual'].copy()
        logging.info('  Summary: %d Individual rows, %d Global rows.',
                     len(ind_df), len(glob_df))
        if ind_df.empty:
            logging.warning('  No Individual rows found; falling back to all rows for main section.')
            ind_df  = corrected_df.copy()
            glob_df = pd.DataFrame()
    else:
        ind_df  = corrected_df.copy()
        glob_df = pd.DataFrame()

    # ── Separate REMAINDER groups within Individual ───────────────────────────
    if 'Group_Label' in ind_df.columns:
        is_rem = ind_df['Group_Label'].astype(str).str.strip().str.upper().str.startswith('REMAINDER')
        main_df = ind_df[~is_rem].copy()
        rem_df  = ind_df[is_rem].copy()
    else:
        main_df = ind_df.copy()
        rem_df  = pd.DataFrame()

    # ── Main Individual section ───────────────────────────────────────────────
    main_rows = _build_group_rows(main_df, group_cols, m_windows)
    main_summary = pd.DataFrame(main_rows)
    if not main_summary.empty and 'Total_Actual' in main_summary.columns:
        main_summary = main_summary.sort_values(
            'Total_Actual', ascending=False, na_position='last'
        ).reset_index(drop=True)

    main_wavg = _compute_weighted_avg_row(main_summary, group_cols, 'Weighted Avg')

    if not main_summary.empty and main_wavg:
        blank = {c: np.nan for c in main_summary.columns}
        main_with_avg = pd.concat(
            [main_summary, pd.DataFrame([blank]), pd.DataFrame([main_wavg])],
            ignore_index=True
        )
    else:
        main_with_avg = main_summary.copy()

    # ── REMAINDER Individual section (if any) ────────────────────────────────
    if not rem_df.empty:
        rem_rows = _build_group_rows(rem_df, group_cols, m_windows)
        rem_summary = pd.DataFrame(rem_rows)
        sep_cols = main_with_avg.columns.tolist() if not main_with_avg.empty \
                   else rem_summary.columns.tolist()
        blank_rows = pd.DataFrame([{c: np.nan for c in sep_cols},
                                   {c: np.nan for c in sep_cols}])
        full = pd.concat([main_with_avg, blank_rows, rem_summary], ignore_index=True)
    else:
        full = main_with_avg

    # ── Global section (if any) ───────────────────────────────────────────────
    if not glob_df.empty:
        glob_rows = _build_group_rows(glob_df, group_cols, m_windows)
        glob_summary = pd.DataFrame(glob_rows)
        glob_wavg = _compute_weighted_avg_row(glob_summary, group_cols, 'Global Weighted Avg')
        if not glob_summary.empty and glob_wavg:
            blank_g = {c: np.nan for c in glob_summary.columns}
            glob_with_avg = pd.concat(
                [glob_summary, pd.DataFrame([blank_g]), pd.DataFrame([glob_wavg])],
                ignore_index=True
            )
        else:
            glob_with_avg = glob_summary.copy()

        sep_cols = full.columns.tolist() if not full.empty else glob_with_avg.columns.tolist()
        blank_rows = pd.DataFrame([{c: np.nan for c in sep_cols},
                                   {c: np.nan for c in sep_cols}])
        section_label = {c: np.nan for c in sep_cols}
        if group_cols:
            section_label[group_cols[0]] = '--- Global Model Groups ---'
        full = pd.concat(
            [full, blank_rows, pd.DataFrame([section_label]), glob_with_avg],
            ignore_index=True
        )

    # ── Reorder columns so Overall_Accuracy comes first ──────────────────────
    full = _reorder_summary_cols(full, group_cols, m_windows)

    return full, main_summary, main_wavg


# ─────────────────────────────────────────────────────────────────────────────
# WRITE OUTPUT EXCEL
# ─────────────────────────────────────────────────────────────────────────────

def _round_df(df: pd.DataFrame) -> pd.DataFrame:
    """Round floats to 4 dp for cleaner Excel output."""
    out = df.copy()
    for col in out.select_dtypes(include=[float]).columns:
        out[col] = out[col].round(4)
    return out


def write_json_summary(summary_df: pd.DataFrame,
                       m_windows: list[int],
                       output_dir: str,
                       input_excel: str,
                       ssd_cutoff: str,
                       cutoff_year: int,
                       cutoff_month: int,
                       group_cols: list[str] | None = None,
                       main_individual_df: pd.DataFrame | None = None,
                       main_wavg: dict | None = None) -> str:
    """
    Write a JSON file with weighted accuracy numbers (original vs corrected)
    for quick comparison across runs.

    Uses main_wavg (pre-computed weighted avg over non-REMAINDER Individual groups)
    if provided; otherwise falls back to finding the 'Weighted Avg' row in summary_df.
    Group counts come from main_individual_df (non-REMAINDER Individual rows).
    """
    import json

    def _fval(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return round(float(v), 4)

    # ── Find the weighted avg values ─────────────────────────────────────────
    if main_wavg:
        wavg_src = main_wavg
    elif group_cols and group_cols[0] in summary_df.columns:
        mask = summary_df[group_cols[0]].astype(str) == 'Weighted Avg'
        wavg_src = summary_df[mask].iloc[0].to_dict() if mask.any() else {}
    else:
        # fallback: last non-blank row with Total_Actual > 0
        wavg_src = {}
        for _, row in summary_df.iloc[::-1].iterrows():
            if pd.notna(row.get('Total_Actual')) and row.get('Total_Actual', 0) > 0:
                wavg_src = row.to_dict()
                break

    orig_acc = _fval(wavg_src.get('Overall_Accuracy_Original_%'))
    corr_acc = _fval(wavg_src.get('Overall_Accuracy_Corrected_%'))
    improvement = round(corr_acc - orig_acc, 4) if (orig_acc is not None and corr_acc is not None) else None

    overall = {
        'weighted_accuracy_original_%':  orig_acc,
        'weighted_accuracy_corrected_%': corr_acc,
        'improvement_%':                 improvement,
        'total_actual':         _fval(wavg_src.get('Total_Actual')),
        'total_pred_original':  _fval(wavg_src.get('Total_Pred_Original')),
        'total_pred_corrected': _fval(wavg_src.get('Total_Pred_Corrected')),
    }

    per_window = {}
    for w in m_windows:
        wo = _fval(wavg_src.get(f'M{w}_Accuracy_%_Original'))
        wc = _fval(wavg_src.get(f'M{w}_Accuracy_%_Corrected'))
        per_window[f'M{w}'] = {
            'accuracy_original_%':  wo,
            'accuracy_corrected_%': wc,
            'improvement_%': round(wc - wo, 4) if (wo is not None and wc is not None) else None,
            'total_actual':   _fval(wavg_src.get(f'M{w}_Total_Actual')),
            'pred_original':  _fval(wavg_src.get(f'M{w}_Pred_Original')),
            'pred_corrected': _fval(wavg_src.get(f'M{w}_Pred_Corrected')),
        }

    # ── Group counts from main Individual df ────────────────────────────────
    if main_individual_df is not None and not main_individual_df.empty:
        n_total     = len(main_individual_df)
        n_corrected = int(main_individual_df['Any_SSD_Correction'].sum()) \
                      if 'Any_SSD_Correction' in main_individual_df.columns else None
    else:
        data_rows = summary_df[summary_df['Total_Actual'].notna() & (summary_df['Total_Actual'] > 0)]
        if group_cols and group_cols[0] in summary_df.columns:
            data_rows = data_rows[data_rows[group_cols[0]].astype(str) != 'Weighted Avg']
        n_total     = len(data_rows)
        n_corrected = int(data_rows['Any_SSD_Correction'].sum()) \
                      if 'Any_SSD_Correction' in data_rows.columns else None

    payload = {
        'generated_at':    datetime.now().isoformat(timespec='seconds'),
        'ssd_cutoff':      str(ssd_cutoff),
        'forecast_cutoff': f'{cutoff_year}-{cutoff_month:02d}',
        'note':            'Weighted averages cover non-REMAINDER Individual-model groups only.',
        'overall':         overall,
        'per_window':      per_window,
        'n_groups_total':     n_total,
        'n_groups_corrected': n_corrected,
    }

    os.makedirs(output_dir, exist_ok=True)
    base = Path(input_excel).stem
    ts   = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    json_path = os.path.join(output_dir, f'{base}_SSD_guardrail_summary_{ts}.json')
    with open(json_path, 'w') as f:
        json.dump(payload, f, indent=2)
    return json_path


def _rebuild_split_sheet_from_ssd(
    corrected_df: pd.DataFrame,
    source_excel: str,
    group_cols: list[str],
    m_windows: list[int],
) -> tuple:
    """
    Rebuild Backfill and New_demand sheets by scaling original split predictions
    by the SSD correction factor per group per month.

    Scale factor = M{m}_Predicted_Corrected / M{m}_Predicted (from corrected_df).
    Actuals are unchanged.  Accuracy is recomputed row-by-row.

    Returns (backfill_df, new_demand_df) — either may be None if the sheet is
    missing from the source Excel.
    """
    # ── Build scale-factor lookup: key -> {m: scale} ─────────────────────────
    merge_key_cols = list(group_cols)
    if 'Prediction_Source' in corrected_df.columns:
        merge_key_cols = list(group_cols) + ['Prediction_Source']

    scale_lookup: dict = {}
    for _, row in corrected_df.iterrows():
        key = tuple(str(row.get(k, '')) for k in merge_key_cols)
        scales: dict = {}
        for m in m_windows:
            orig = float(row.get(f'M{m}_Predicted', 0) or 0)
            corr = float(row.get(f'M{m}_Predicted_Corrected', 0) or 0)
            scales[m] = corr / orig if orig > 0 else 1.0
        scale_lookup[key] = scales

    try:
        src_xl = pd.ExcelFile(source_excel)
    except Exception as e:
        logging.warning('  Could not open source Excel for split-sheet rebuild: %s', e)
        return None, None

    acc_pattern = re.compile(r'^M\d+_Accuracy_%$')
    results: dict = {}

    for sheet_name in ('Backfill', 'New_demand'):
        if sheet_name not in src_xl.sheet_names:
            logging.warning('  Sheet %r not in source Excel — skipping split rebuild.', sheet_name)
            results[sheet_name] = None
            continue

        src_df = src_xl.parse(sheet_name)

        # Drop weighted-avg footer row(s)
        if 'Group_Label' in src_df.columns:
            footer_mask = src_df['Group_Label'].astype(str).str.strip().str.lower().isin(
                ['weighted avg', 'weighted average']
            )
            src_data = src_df[~footer_mask].copy()
        else:
            src_data = src_df.copy()

        rows = []
        for _, row in src_data.iterrows():
            # Build the lookup key — fall back gracefully if a key col is absent
            key = tuple(str(row.get(k, '')) if k in src_data.columns else ''
                        for k in merge_key_cols)
            scales = scale_lookup.get(key, {m: 1.0 for m in m_windows})

            new_row = {c: row[c] for c in src_data.columns if not acc_pattern.match(c)}

            for m in m_windows:
                pred_col   = f'M{m}_Predicted'
                actual_col = f'M{m}_Actual'
                acc_col    = f'M{m}_Accuracy_%'

                if pred_col not in src_data.columns:
                    continue

                orig_pred = float(row.get(pred_col, 0) or 0)
                new_pred  = round(orig_pred * scales.get(m, 1.0), 2)
                new_row[pred_col] = new_pred

                if actual_col in src_data.columns:
                    actual = float(row.get(actual_col, 0) or 0)
                    if actual > 0:
                        ape = min(1.0, abs(new_pred - actual) / actual)
                        new_row[acc_col] = round((1.0 - ape) * 100, 2)
                    else:
                        new_row[acc_col] = np.nan

            rows.append(new_row)

        out_df = pd.DataFrame(rows)

        # Restore column order from source (accuracy cols inserted after each predicted col)
        ordered: list = []
        for c in src_data.columns:
            if acc_pattern.match(c):
                continue
            ordered.append(c)
            m_match = re.match(r'^M(\d+)_Predicted$', c)
            if m_match:
                acc_col = f'M{m_match.group(1)}_Accuracy_%'
                if acc_col in out_df.columns:
                    ordered.append(acc_col)
        remaining = [c for c in out_df.columns if c not in ordered]
        ordered += remaining
        out_df = out_df[[c for c in ordered if c in out_df.columns]]

        # Recompute weighted-avg footer row (weight = first M_Actual)
        m0_actual = f'M{m_windows[0]}_Actual'
        if m0_actual in out_df.columns and out_df[m0_actual].notna().any():
            w = out_df[m0_actual].fillna(0)
            if w.sum() > 0:
                avg_row: dict = {c: np.nan for c in out_df.columns}
                if 'Group_Label' in avg_row:
                    avg_row['Group_Label'] = 'Weighted avg'
                for m in m_windows:
                    pred_col   = f'M{m}_Predicted'
                    actual_col = f'M{m}_Actual'
                    acc_col    = f'M{m}_Accuracy_%'
                    if actual_col in out_df.columns:
                        avg_row[actual_col] = round(float(out_df[actual_col].fillna(0).sum()), 2)
                    if pred_col in out_df.columns:
                        avg_row[pred_col] = round(float(out_df[pred_col].fillna(0).sum()), 2)
                    if acc_col in out_df.columns:
                        valid = (w > 0) & out_df[acc_col].notna()
                        if valid.any() and w[valid].sum() > 0:
                            avg_row[acc_col] = round(
                                float((out_df.loc[valid, acc_col] * w[valid]).sum()
                                      / w[valid].sum()), 2
                            )
                out_df = pd.concat([out_df, pd.DataFrame([avg_row])], ignore_index=True)

        results[sheet_name] = out_df
        logging.info('  Rebuilt %s sheet: %d data rows.', sheet_name, len(out_df) - 1)

    return results.get('Backfill'), results.get('New_demand')


def write_output(corrected_df: pd.DataFrame,
                 summary_df: pd.DataFrame,
                 floors_df: pd.DataFrame,
                 output_dir: str,
                 input_excel: str,
                 group_cols: list[str] | None = None,
                 m_windows: list[int] | None = None) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    base = Path(input_excel).stem
    out_path = os.path.join(output_dir, f'{base}_SSD_guardrail_{ts}.xlsx')

    # Auto-detect m_windows / group_cols from corrected_df if not supplied
    if m_windows is None:
        m_windows = _detect_m_columns(corrected_df)
    if group_cols is None:
        group_cols = _detect_group_cols(corrected_df, m_windows)

    # Rebuild Backfill and New_demand from SSD-corrected predictions
    backfill_df, new_demand_df = _rebuild_split_sheet_from_ssd(
        corrected_df, input_excel, group_cols, m_windows
    )

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        _round_df(corrected_df).to_excel(writer, sheet_name='All_Predictions_SSD', index=False)
        if not summary_df.empty:
            _round_df(summary_df).to_excel(writer, sheet_name='SSD_Accuracy_Summary', index=False)
        if not floors_df.empty:
            floors_df.to_excel(writer, sheet_name='SSD_Floors_Detail', index=False)
        if backfill_df is not None and not backfill_df.empty:
            _round_df(backfill_df).to_excel(writer, sheet_name='Backfill', index=False)
        if new_demand_df is not None and not new_demand_df.empty:
            _round_df(new_demand_df).to_excel(writer, sheet_name='New_demand', index=False)

    logging.info('Output written to %s', out_path)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN ORDERING HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _reorder_columns(df: pd.DataFrame, group_cols: list[str], m_windows: list[int]) -> pd.DataFrame:
    """
    Order columns as:
      group_cols
      [for each M: Actual, Predicted, SSD_Floor, Predicted_Corrected, Correction_Applied,
                   Accuracy_%_Original, Accuracy_%_Corrected]
      Any_SSD_Correction, N_Months_Corrected
      meta cols (Group_Label, Model_Name, Prediction_Source)
    """
    ordered = list(group_cols)
    for w in m_windows:
        for suffix in ['_Actual', '_Predicted', '_SSD_Floor', '_Predicted_Corrected',
                       '_Correction_Applied', '_Accuracy_%_Original', '_Accuracy_%_Corrected']:
            c = f'M{w}{suffix}'
            if c in df.columns:
                ordered.append(c)
    for extra in ['Any_SSD_Correction', 'N_Months_Corrected']:
        if extra in df.columns:
            ordered.append(extra)
    for meta in sorted(_META_COLS):
        if meta in df.columns:
            ordered.append(meta)
    # Any remaining cols not yet added
    remaining = [c for c in df.columns if c not in ordered]
    ordered += remaining
    return df[ordered]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Orchestrate the full SSD guardrail pipeline end-to-end.

    Execution phases:
        Step 1  - Parse CLI arguments.
        Step 2  - Auto-detect missing paths (results dir, test parquet, floors CSV,
                  input Excel) from the on-disk directory structure.
        Step 3  - Resolve forecast cutoff year/month from the test parquet
                  (overrides --cutoff-year / --cutoff-month if parquet is found).
        Step 4  - Validate the SSD cutoff date string.
        Step 5  - Load the All_Predictions sheet from the input Excel.
        Step 6  - Detect M-window indices (e.g. [0,1,2,3,4,5]) from column names.
        Step 7  - Detect group columns (all non-meta, non-M columns).
        Step 8  - Load or build SSD floors:
                    a. Load from pre-computed ssd_floors.csv (fast path), OR
                    b. Build from raw DFC CSV via build_ssd_floors() (slow path).
        Step 9  - Apply the guardrail to every prediction row.
        Step 10 - Reorder columns into a clean, readable layout.
        Step 11 - Build the SSD_Accuracy_Summary sheet (Individual rows only).
        Step 12 - Write the output Excel (All_Predictions_SSD, SSD_Accuracy_Summary,
                  SSD_Floors_Detail sheets).
        Step 13 - Write the improvement JSON for quick cross-run comparison.
    """
    # Step 1: Parse CLI arguments.
    args = parse_args()

    # Step 2: Auto-detect missing paths ──────────────────────────────────────
    _auto_detect_paths(args)

    # Default output_dir fallback (in case results_dir was never found).
    if not args.output_dir:
        args.output_dir = '.'

    # Validate that we have an input Excel (required, just may be auto-detected).
    if not args.input_excel:
        logging.error(
            'No --input-excel provided and none could be auto-detected.\n'
            '  Hint: make sure a results_%s_%s_*_%s/ directory exists in the current folder,\n'
            '  or pass --input-excel explicitly.',
            args.mode, args.practice_area, args.market,
        )
        sys.exit(1)

    # Step 3: Detect forecast cutoff year/month — try to read from test parquet first.
    # If the parquet contains a cutoff_date column, that value takes precedence over
    # the --cutoff-year / --cutoff-month CLI defaults.
    cutoff_year  = args.cutoff_year
    cutoff_month = args.cutoff_month

    if args.test_parquet and os.path.exists(args.test_parquet) and not args.publishing:
        # In publishing mode, cutoff is already set correctly (2026-01) — skip parquet detection
        # because the parquet stores the last training day (2025-12-31) not the M0 month.
        cutoff_year, cutoff_month = _read_cutoff_from_parquet(
            args.test_parquet, cutoff_year, cutoff_month
        )

    logging.info('Forecast cutoff: %d-%02d  (M0 = %s-%02d)',
                 cutoff_year, cutoff_month, cutoff_year, cutoff_month)

    # Step 4: Parse and validate the SSD guardrail cutoff date.
    # SOs with SO Submission Date < ssd_cutoff_date are considered "confirmed"
    # and contribute to the floor counts.
    ssd_cutoff_date = _parse_date_flexible(args.ssd_cutoff)
    if ssd_cutoff_date is None:
        logging.error('Could not parse --ssd-cutoff "%s". Use YYYY-MM-DD.', args.ssd_cutoff)
        sys.exit(1)
    logging.info('SSD guardrail cutoff: %s (confirmed = SSD < this date)', ssd_cutoff_date)

    # Step 5: Load All_Predictions sheet from the input Excel.
    pred_df = _load_predictions(args.input_excel, args.sheet_name)

    # Step 6: Detect M-window indices and group columns from the loaded DataFrame.
    m_windows = _detect_m_columns(pred_df)
    if not m_windows:
        logging.error('No M{n}_Actual / M{n}_Predicted columns found in sheet "%s".', args.sheet_name)
        sys.exit(1)
    logging.info('Detected M-windows: %s', m_windows)

    # Step 7: Detect group columns (all non-meta, non-M-window columns).
    group_cols = _detect_group_cols(pred_df, m_windows)
    logging.info('Detected group columns: %s', group_cols)

    # Step 8: Build or load SSD floors.
    # Priority: --floors-csv (explicit) > auto-detect ssd_floors.csv > --raw-data (compute on-the-fly).
    floors_csv = args.floors_csv

    # Step 8a: Last-chance auto-detect — look for ssd_floors.csv next to the test parquet.
    if not floors_csv and not args.raw_data and args.test_parquet:
        candidate = os.path.join(os.path.dirname(args.test_parquet), 'ssd_floors.csv')
        if os.path.exists(candidate):
            floors_csv = candidate
            logging.info('  Auto-detected pre-computed SSD floors: %s', floors_csv)

    if floors_csv and os.path.exists(floors_csv):
        # Step 8b: Fast path — load the pre-computed CSV written by data_split.py.
        logging.info('Loading pre-computed SSD floors from %s', floors_csv)
        floors_df = pd.read_csv(floors_csv)
        if 'window' in floors_df.columns:
            floors_df['window'] = floors_df['window'].astype(int)
        for gc in group_cols:
            if gc in floors_df.columns:
                floors_df[gc] = floors_df[gc].astype(str).str.strip()
        # Keep only windows that are in our prediction set.
        if 'window' in floors_df.columns:
            floors_df = floors_df[floors_df['window'].isin(m_windows)].copy()
        logging.info('  Loaded %d floor rows covering windows %s',
                     len(floors_df),
                     sorted(floors_df['window'].unique().tolist()) if 'window' in floors_df.columns else '?')
    elif args.raw_data:
        # Step 8c: Slow path — compute floors by scanning the raw DFC CSV.
        floors_df = build_ssd_floors(
            raw_csv=args.raw_data,
            ssd_cutoff_date=ssd_cutoff_date,
            cutoff_year=cutoff_year,
            cutoff_month=cutoff_month,
            group_cols_excel=group_cols,
            m_windows=m_windows,
            exclude_open=args.exclude_open,
            output_dir=args.output_dir,
        )
    else:
        logging.error(
            'No SSD floor source available. Provide --raw-data, --floors-csv, '
            'or run data_split.py first (it saves ssd_floors.csv next to test_dataset.parquet).'
        )
        sys.exit(1)

    # Step 9: Apply the guardrail — corrected = max(predicted, ssd_floor) for each row x window.
    logging.info('Applying SSD guardrail...')
    corrected_df = apply_ssd_guardrail(pred_df, floors_df, group_cols, m_windows)

    total_corrections = corrected_df['Any_SSD_Correction'].sum() if 'Any_SSD_Correction' in corrected_df else 0
    logging.info('  %d/%d rows had at least one month corrected upward.',
                 total_corrections, len(corrected_df))

    # Step 10: Reorder columns into a readable layout:
    # group_cols | M{w}_Actual / Predicted / Floor / Corrected / flags | meta cols.
    corrected_df = _reorder_columns(corrected_df, group_cols, m_windows)

    if args.publishing:
        # Publishing mode: no real actuals — skip accuracy summary entirely.
        logging.info('[Publishing mode] Skipping accuracy summary (no real actuals available).')
        summary_df = pd.DataFrame()
        main_individual_df = pd.DataFrame()
        main_wavg = {}
    else:
        # Step 11: Build the SSD_Accuracy_Summary sheet (Individual-model rows only,
        # REMAINDER groups appended after a blank-row separator).
        logging.info('Building accuracy summary (Individual-model rows only)...')
        summary_df, main_individual_df, main_wavg = build_summary(corrected_df, group_cols, m_windows)

        # Log weighted accuracy from the main Individual groups for quick inspection.
        if main_wavg:
            orig = main_wavg.get('Overall_Accuracy_Original_%')
            corr = main_wavg.get('Overall_Accuracy_Corrected_%')
            if orig is not None and corr is not None:
                logging.info('  Weighted accuracy (Individual groups): Original=%.1f%% -> Corrected=%.1f%%',
                             orig, corr)

    # Step 12: Write the output Excel:
    # All_Predictions_SSD | SSD_Accuracy_Summary | SSD_Floors_Detail | Backfill | New_demand.
    out_path = write_output(corrected_df, summary_df, floors_df,
                            args.output_dir, args.input_excel,
                            group_cols=group_cols, m_windows=m_windows)
    logging.info('Done. Guardrail Excel saved to: %s', out_path)

    if not args.publishing:
        # Step 13: Write the improvement JSON for quick cross-run accuracy comparison.
        json_path = write_json_summary(
            summary_df, m_windows,
            args.output_dir, args.input_excel,
            args.ssd_cutoff, cutoff_year, cutoff_month,
            group_cols=group_cols,
            main_individual_df=main_individual_df,
            main_wavg=main_wavg,
        )
        logging.info('Improvement JSON saved to:      %s', json_path)


if __name__ == '__main__':
    main()
