"""
AutoGluon training and evaluation script with optional per-group mode.

Trains and evaluates an AutoGluon TabularPredictor for monthly count (SO demand)
forecasting.  The script can run in two modes:

  Global mode (default):
    Reads a single training parquet and a single test parquet, trains one model,
    evaluates it and writes Excel + JSON outputs.

  Per-group mode (--group-data-dir):
    Reads a group_manifest.json produced by build_training_groups.py.
    Trains a global model FIRST (on all data), then trains one individual model
    per group, and writes a combined Excel workbook that compares global vs
    individual accuracy.

Inputs:
  --training-dataset   Parquet file with engineered features; target column =
                       'target_count'; rows keyed by (group_by_cols, cutoff_date,
                       window_start).
  --test-dataset       Same schema as training dataset but covering the forecast
                       horizon (Jul-Dec 2025 by default).
  --group-data-dir     Directory created by build_training_groups.py containing
                       per-group train/test parquets, group_manifest.json, and
                       optional backfill_new_demand_ratios.json.

Outputs (per run):
  results/<timestamp>.json         All model metrics serialised as JSON.
  results/summary_<timestamp>.csv  One-row-per-model accuracy table.
  results/<prefix>_<model>_<ts>.xlsx  Per-model Excel with sheets:
                                       All_Predictions, Overall_Metrics,
                                       Group_Metrics.
  results/combined_group_results_<ts>.xlsx  (per-group mode only)
    Sheets: All_Predictions, Global_vs_Individual_Accuracy, Backfill,
            New_demand, Overall_Metrics, Group_Metrics, Model_Averages,
            Global_All_Predictions, Global_Overall_Metrics, Global_Group_Metrics.
  models/autogluon_temp_<id>/      Saved AutoGluon predictor artefacts.
  shap_summary_<label>.png         SHAP summary plot (if --run-shap).
  shap_bar_<label>.png             SHAP feature-importance bar plot.

Usage:
    # Global mode:
    python train_and_predict.py \\
        --training-dataset results_BS/data_BS/training_dataset.parquet \\
        --test-dataset     results_BS/data_BS/test_dataset.parquet

    # Per-group mode:
    python train_and_predict.py \\
        --group-data-dir results_BS/data_BS \\
        --output-dir     results_BS_run1 \\
        --months-ahead   0 1 2 3 4 5 \\
        --run-shap
"""

import sys
import os
import warnings

# Suppress noisy deprecation warnings from AutoGluon deps (hyperopt, ray, pkg_resources)
os.environ.setdefault('RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO', '0')
os.environ.setdefault('RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS', '0')
warnings.filterwarnings('ignore', message=r'.*pkg_resources.*deprecated.*')
warnings.filterwarnings('ignore', message=r'.*force_int_remainder_cols.*', category=FutureWarning)

# Add current directory to path for imports (script is in root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import polars as pl
import numpy as np
import pandas as pd
from datetime import datetime
import logging
import coloredlogs
import argparse
import json
import traceback
import re
import shutil
import uuid
from pathlib import Path
from tqdm import tqdm

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import shap

# Model imports
try:
    from autogluon.tabular import TabularPredictor
    HAVE_AUTOGLUON = True
except ImportError:
    HAVE_AUTOGLUON = False
    logging.warning("AutoGluon not installed. Install with: pip install autogluon")

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s - %(message)s', isatty=True)

# Set random seeds for reproducibility
RANDOM_SEED = 42
TOP_N_SHAP_FEATURES = 25
np.random.seed(RANDOM_SEED)

# Universal backfill share when no group-level or level-average ratio exists (analysis: backfill < 7%)
UNIVERSAL_RATIO_BACKFILL = 0.07

def weighted_mape(y_true, y_pred):
    """MAPE but only for non-zero actuals - focuses on demand cases only.

    Zero-actual rows are excluded because dividing by zero is undefined and
    because zero-demand periods carry no business weight.  The result is
    equivalent to the standard MAPE formula applied only to the non-zero
    subset (no sample-size weighting beyond that).

    Args:
        y_true: Array-like of actual values.
        y_pred: Array-like of predicted values (same length as y_true).

    Returns:
        float: Mean absolute percentage error (%) over non-zero actuals,
               or np.nan when all actuals are zero.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    # Keep only rows where an actual demand event occurred
    mask = y_true > 0
    if mask.sum() == 0:
        return np.nan

    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def calculate_mape_accuracy(y_true, y_pred):
    """
    Calculate MAPE and Accuracy metrics for demand data.

    The computation deliberately ignores zero-actual rows.  Including them
    would make MAPE undefined (division by zero) and would inflate the metric
    with noise from periods where no demand occurred at all.

    Capping APE at 1.0 (100 %) prevents a single extreme outlier from
    dominating the mean.  The cap is consistent with common industry practice
    for intermittent-demand forecasting.

    Steps:
        1. Filter to only non-zero actuals (ignore zero cases).
        2. Calculate Absolute Error (AE) = |forecast - actual|.
        3. Calculate Absolute Percentage Error (APE) = AE / actual, capped at 100%.
        4. MAPE = mean of all APEs expressed as a percentage (0-100).
        5. Accuracy = 1 - MAPE/100  (expressed as a fraction 0-1, where 1.0 = perfect).

    Args:
        y_true: Array-like of actual values.
        y_pred: Array-like of predicted values (same length as y_true).

    Returns:
        dict: {
            'mape': float  -- MAPE in percent (0-100), or np.nan if no non-zero actuals,
            'accuracy': float  -- 1 - mape/100 (0-1), or np.nan.
        }
    """
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)

    # Only consider non-zero actuals (zero-demand periods are excluded from MAPE)
    mask = y_true > 0

    if mask.sum() == 0:
        # No non-zero actuals: metrics are undefined
        return {
            'mape': np.nan,
            'accuracy': np.nan
        }

    # Calculate Absolute Error for non-zero actuals only
    ae = np.abs(y_pred[mask] - y_true[mask])

    # Calculate Absolute Percentage Error (APE) for non-zero actuals.
    # Cap each APE at 1.0 (= 100 %) so extreme outliers don't dominate.
    ape = ae / y_true[mask]
    ape = np.minimum(ape, 1.0)  # Cap at 100% (1.0)

    # MAPE = mean of all APEs expressed as a percentage
    mape = np.mean(ape) * 100  # Convert from fraction to percentage

    # Accuracy is the complement of MAPE (expressed as a 0-1 fraction)
    accuracy = 1.0 - (mape / 100.0)

    return {
        'mape': mape,
        'accuracy': accuracy
    }

def _autogluon_path_for_group_by(group_by_cols):
    """Build AutoGluon temp path suffix from first letters of group_by columns (e.g. BU+Skill Cluster -> _BS)."""
    if not group_by_cols:
        return ''
    suffix = ''.join(col[0].upper() for col in group_by_cols if col and col.strip())
    return f'_{suffix}' if suffix else ''

def build_excel_prefix(group_by_cols, months_ahead):
    """Build Excel filename prefix (e.g. BS_m6) from args; group initials kept capital to match results folder."""
    if not group_by_cols:
        group_part = 'all'
    else:
        group_part = ''.join(col.strip()[0].upper() for col in group_by_cols if col and col.strip())
        group_part = group_part or 'all'

    if months_ahead:
        months_count = len(set(months_ahead))
    else:
        months_count = 0

    return f"{group_part}_m{months_count}"

def sanitize_filename_token(value):
    """Sanitize a string for safe filenames."""
    if not value:
        return 'model'
    token = re.sub(r'[^a-z0-9]+', '_', str(value).lower()).strip('_')
    return token or 'model'


def _load_group_manifest(group_data_dir: str) -> dict:
    """Load the per-group dataset manifest from disk."""
    manifest_path = os.path.join(group_data_dir, 'group_manifest.json')
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    return manifest


def _format_group_label(group_values: dict) -> str:
    """Format a readable label for logging."""
    if not group_values:
        return "unknown_group"
    return ", ".join(f"{k}={v}" for k, v in group_values.items())


def _build_group_token(group_values: dict, group_by_cols: list) -> str:
    """Build a filename-safe token from ordered group values."""
    if group_by_cols and group_values:
        values = [group_values.get(col) for col in group_by_cols]
    else:
        values = list(group_values.values()) if group_values else []
    token = "__".join(sanitize_filename_token(val) for val in values if val is not None)
    return token or 'group'


def _compute_group_m0_total(test_path: str) -> float:
    """Compute total target_count for M0 from a per-group test dataset."""
    try:
        df = pl.read_parquet(test_path)
    except Exception as e:
        logging.warning(f"  Failed to read test dataset for M0 ordering: {test_path} ({e})")
        return None

    m0_df = df
    if 'window_start' in df.columns:
        try:
            m0_df = df.filter(pl.col('window_start') == 0)
        except Exception:
            m0_df = df

    if 'target_count' in m0_df.columns:
        try:
            return float(m0_df['target_count'].sum())
        except Exception:
            return None

    return float(len(m0_df))


def _sanitize_model_name(name: str) -> str:
    """Remove 'Auto' from model names for display (e.g. AutoGluon_lightgbm -> Gluon_lightgbm)."""
    if not name or not isinstance(name, str):
        return name
    return name.replace("Auto", "")


def _append_combined_frames(combined_frames: dict, results_pd: pd.DataFrame, metrics_df: pd.DataFrame,
                            group_metrics_df: pd.DataFrame, group_label: str, model_name: str) -> None:
    """Append per-group frames to the combined collection."""
    display_name = _sanitize_model_name(model_name)
    if results_pd is not None:
        results_pd = results_pd.copy()
        results_pd['Group_Label'] = group_label
        results_pd['Model_Name'] = display_name
        combined_frames['All_Predictions'].append(results_pd)

    if metrics_df is not None:
        metrics_df = metrics_df.copy()
        metrics_df['Group_Label'] = group_label
        metrics_df['Model_Name'] = display_name
        combined_frames['Overall_Metrics'].append(metrics_df)

    if group_metrics_df is not None:
        group_metrics_df = group_metrics_df.copy()
        group_metrics_df['Group_Label'] = group_label
        group_metrics_df['Model_Name'] = display_name
        combined_frames['Group_Metrics'].append(group_metrics_df)


def _build_accuracy_comparison_df(combined_frames: dict, global_frames: dict) -> pd.DataFrame:
    """Build one row per group with columns Overall_Global, Overall_Individual, Total_Volume, CV, M0_Global, M0_Individual, etc."""
    metric_cols = {'Metric_Type', 'Window', 'Total_Actual', 'Total_NonZero_Predicted', 'MAPE_%', 'Accuracy', 'wMAPE_%', 'sMAPE_%'}
    ind_df = None
    if combined_frames and combined_frames.get('Group_Metrics'):
        ind_df = pd.concat(combined_frames['Group_Metrics'], ignore_index=True)
    glob_df = None
    if global_frames and global_frames.get('Group_Metrics') is not None and not global_frames['Group_Metrics'].empty:
        glob_df = global_frames['Group_Metrics']
    if ind_df is None or ind_df.empty:
        return pd.DataFrame()
    if glob_df is None or glob_df.empty:
        return pd.DataFrame()

    group_cols_global = [c for c in glob_df.columns if c not in metric_cols]
    # Vectorized Group_Label for global (same format as _global_row_to_label)
    glob_label_parts = [glob_df[c].astype(str).radd(f"{c}=") for c in group_cols_global]
    glob_df = glob_df.copy()
    glob_df['_glabel'] = glob_label_parts[0]
    for p in glob_label_parts[1:]:
        glob_df['_glabel'] = glob_df['_glabel'] + ", " + p

    global_by_group_window = (
        glob_df[['_glabel', 'Window', 'Accuracy']].dropna(subset=['Accuracy'])
        .set_index(['_glabel', 'Window'])['Accuracy'].to_dict()
    )
    # Rebuild as nested dict group -> window -> acc for existing downstream logic
    gb_nested = {}
    for (lab, w), acc in global_by_group_window.items():
        if lab not in gb_nested:
            gb_nested[lab] = {}
        gb_nested[lab][w] = acc
    global_by_group_window = gb_nested

    ind_label = ind_df['Group_Label'].astype(str)
    ind_df_valid = ind_df.loc[ind_label.notna() & (ind_label != 'nan')]
    individual_by_group_window = (
        ind_df_valid[['Group_Label', 'Window', 'Accuracy']].assign(Group_Label=ind_df_valid['Group_Label'].astype(str))
        .dropna(subset=['Accuracy'])
        .set_index(['Group_Label', 'Window'])['Accuracy'].to_dict()
    )
    ind_nested = {}
    for (lab, w), acc in individual_by_group_window.items():
        if lab not in ind_nested:
            ind_nested[lab] = {}
        ind_nested[lab][w] = acc
    individual_by_group_window = ind_nested

    all_groups = sorted(set(individual_by_group_window) | set(global_by_group_window))

    total_volume_by_group = {}
    total_predicted_by_group = {}
    if 'Group_Label' in ind_df.columns and 'Window' in ind_df.columns and 'Total_Actual' in ind_df.columns:
        all_w = ind_df[ind_df['Window'] == 'ALL_WINDOWS']
        if not all_w.empty:
            vol_agg = all_w.dropna(subset=['Group_Label']).groupby('Group_Label', dropna=False)['Total_Actual'].sum()
            total_volume_by_group = {str(lab): float(vol) for lab, vol in vol_agg.items()
                                    if lab is not None and not (isinstance(lab, float) and np.isnan(lab))}
        if 'Total_NonZero_Predicted' in ind_df.columns and not all_w.empty:
            pred_agg = all_w.dropna(subset=['Group_Label']).groupby('Group_Label', dropna=False)['Total_NonZero_Predicted'].sum()
            total_predicted_by_group = {str(lab): float(vol) for lab, vol in pred_agg.items()
                                        if lab is not None and not (isinstance(lab, float) and np.isnan(lab))}
    # Fill Total_Volume for global-only groups from global Group_Metrics
    if glob_df is not None and 'Window' in glob_df.columns and 'Total_Actual' in glob_df.columns:
        all_w_glob = glob_df[glob_df['Window'] == 'ALL_WINDOWS']
        if not all_w_glob.empty:
            agg_glob = all_w_glob.groupby('_glabel', dropna=False)['Total_Actual'].sum()
            for lab, vol in agg_glob.items():
                if lab not in total_volume_by_group:
                    total_volume_by_group[lab] = float(vol)
            if 'Total_NonZero_Predicted' in glob_df.columns:
                agg_glob_pred = all_w_glob.groupby('_glabel', dropna=False)['Total_NonZero_Predicted'].sum()
                for lab, vol in agg_glob_pred.items():
                    if lab not in total_predicted_by_group:
                        total_predicted_by_group[lab] = float(vol)

    cv_by_group = {}
    if combined_frames.get('All_Predictions'):
        all_pred = pd.concat(combined_frames['All_Predictions'], ignore_index=True)
        actual_cols = [c for c in all_pred.columns if str(c).endswith('_Actual')]
        _cv_exclude = set(actual_cols) | {'Group_Label', 'Model_Name'}
        group_cols = [c for c in all_pred.columns if c not in _cv_exclude and not str(c).endswith('_Predicted')]
        if actual_cols and group_cols:
            all_pred = all_pred.dropna(subset=group_cols)
            if not all_pred.empty:
                cv_label_parts = [all_pred[c].astype(str).radd(f"{c}=") for c in group_cols]
                all_pred = all_pred.copy()
                all_pred['_cv_label'] = cv_label_parts[0]
                for p in cv_label_parts[1:]:
                    all_pred['_cv_label'] = all_pred['_cv_label'] + ", " + p
                vals = all_pred[actual_cols].astype(float).fillna(0)
                mean_vals = vals.mean(axis=1)
                std_vals = vals.std(axis=1)
                cv_vals = np.where(mean_vals > 0, std_vals / mean_vals, np.inf)
                for lab, cv in zip(all_pred['_cv_label'], cv_vals):
                    if lab not in cv_by_group:
                        cv_by_group[lab] = float(cv)
    if global_frames and global_frames.get('All_Predictions') is not None and not global_frames['All_Predictions'].empty:
        glob_pred = global_frames['All_Predictions']
        actual_cols_glob = [c for c in glob_pred.columns if str(c).endswith('_Actual')]
        _cv_exclude_glob = set(actual_cols_glob) | {'Model_Name'}
        group_cols_glob = [c for c in glob_pred.columns if c not in _cv_exclude_glob and not str(c).endswith('_Predicted')]
        if actual_cols_glob and group_cols_glob:
            gpred = glob_pred[group_cols_glob + actual_cols_glob].dropna(subset=group_cols_glob).copy()
            if not gpred.empty:
                gpred_label_parts = [gpred[c].astype(str).radd(f"{c}=") for c in group_cols_glob]
                gpred['_cv_label'] = gpred_label_parts[0]
                for p in gpred_label_parts[1:]:
                    gpred['_cv_label'] = gpred['_cv_label'] + ", " + p
                vals_g = gpred[actual_cols_glob].astype(float).fillna(0)
                mean_g = vals_g.mean(axis=1)
                std_g = vals_g.std(axis=1)
                cv_g = np.where(mean_g > 0, std_g / mean_g, np.inf)
                for lab, cv in zip(gpred['_cv_label'], cv_g):
                    if lab not in cv_by_group:
                        cv_by_group[lab] = float(cv)

    def _numeric_window_key(w):
        if w == 'ALL_WINDOWS':
            return -1
        if isinstance(w, str) and w.startswith('M'):
            try:
                return int(w[1:])
            except ValueError:
                return 0
        return 0
    all_windows = set()
    for d in [global_by_group_window, individual_by_group_window]:
        for g in d:
            for w in d[g]:
                all_windows.add(w)
    windows = ['ALL_WINDOWS'] + sorted([w for w in all_windows if w != 'ALL_WINDOWS'], key=_numeric_window_key)
    month_windows = [w for w in windows if w != 'ALL_WINDOWS']
    group_month_totals = _group_month_totals_from_predictions(combined_frames, global_frames, month_windows)

    rows = []
    for group in all_groups:
        row = {'Group': group}
        g_glob = global_by_group_window.get(group, {})
        g_ind = individual_by_group_window.get(group, {})
        row['Overall_Global'] = g_glob.get('ALL_WINDOWS', np.nan)
        row['Overall_Individual'] = g_ind.get('ALL_WINDOWS', np.nan)
        row['Total_Volume'] = total_volume_by_group.get(group, np.nan)
        row['Total_Predicted_Volume'] = total_predicted_by_group.get(group, np.nan)
        row['CV'] = cv_by_group.get(group, np.nan)
        for w in windows:
            if w == 'ALL_WINDOWS':
                continue
            row[f'{w}_Global'] = g_glob.get(w, np.nan)
            row[f'{w}_Individual'] = g_ind.get(w, np.nan)
        month_data = group_month_totals.get(group, [])
        row['Group_Analysis'] = _classify_group_analysis_from_actual_pred(month_data, month_windows)
        rows.append(row)
    return pd.DataFrame(rows)


def _get_group_weights(group_metrics_df: pd.DataFrame):
    """Return dict of group label -> weight (Total_Actual for ALL_WINDOWS) for use in notes and weighted avg."""
    if group_metrics_df is None or group_metrics_df.empty:
        return {}
    if 'Group_Label' not in group_metrics_df.columns or 'Window' not in group_metrics_df.columns or 'Total_Actual' not in group_metrics_df.columns:
        return {}
    all_w = group_metrics_df[group_metrics_df['Window'] == 'ALL_WINDOWS']
    if all_w.empty:
        return {}
    weights_ser = all_w.groupby(all_w['Group_Label'].astype(str), dropna=False)['Total_Actual'].sum()
    return weights_ser.to_dict()


def _round_df_for_excel(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    """Round all numeric columns to the given decimal places for consistent Excel output."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]) and c != '_weight':
            out[c] = out[c].round(decimals)
    return out


def _group_month_totals_from_predictions(combined_frames: dict, global_frames: dict, month_windows: list):
    """Build dict group -> list of (window, total_actual, total_predicted) from individual All_Predictions, else global."""
    result = {}
    actual_suffix = '_Actual'
    pred_suffix = '_Predicted'

    if combined_frames and combined_frames.get('All_Predictions'):
        for pred_df in combined_frames['All_Predictions']:
            if pred_df is None or pred_df.empty:
                continue
            lab = pred_df['Group_Label'].iloc[0] if 'Group_Label' in pred_df.columns else None
            if lab is None or (isinstance(lab, float) and np.isnan(lab)):
                continue
            lab = str(lab)
            rows = []
            for w in month_windows:
                ac, pc = f'{w}{actual_suffix}', f'{w}{pred_suffix}'
                if ac not in pred_df.columns or pc not in pred_df.columns:
                    continue
                ta = pred_df[ac].astype(float).fillna(0).sum()
                tp = pred_df[pc].astype(float).fillna(0).sum()
                rows.append((w, float(ta), float(tp)))
            if rows:
                result[lab] = rows

    if not global_frames or global_frames.get('All_Predictions') is None or global_frames['All_Predictions'].empty:
        return result
    glob_df = global_frames['All_Predictions']
    group_cols = [c for c in glob_df.columns if not str(c).endswith(actual_suffix) and not str(c).endswith(pred_suffix)]
    if not group_cols:
        return result
    glob_df = glob_df.copy()
    glab_parts = [glob_df[c].astype(str).radd(f"{c}=") for c in group_cols]
    glob_df['_glab'] = glab_parts[0]
    for p in glab_parts[1:]:
        glob_df['_glab'] = glob_df['_glab'] + ", " + p
    for lab, grp in glob_df.groupby('_glab', dropna=False):
        if lab is None or (isinstance(lab, float) and np.isnan(lab)):
            continue
        lab = str(lab)
        if lab in result:
            continue
        rows = []
        for w in month_windows:
            ac, pc = f'{w}{actual_suffix}', f'{w}{pred_suffix}'
            if ac not in grp.columns or pc not in grp.columns:
                continue
            ta = grp[ac].astype(float).fillna(0).sum()
            tp = grp[pc].astype(float).fillna(0).sum()
            rows.append((w, float(ta), float(tp)))
        if rows:
            result[lab] = rows
    return result


def _classify_group_analysis_from_actual_pred(month_data: list, month_windows: list) -> str:
    """Classify from month-on-month actual vs predicted: systematic over/under, low M3-M6 visibility, etc."""
    if not month_data:
        return "No prediction data"
    late = [w for w in month_windows if isinstance(w, str) and w.startswith('M') and len(w) >= 2]
    try:
        late = [w for w in late if 3 <= int(w[1:]) <= 6]
    except (ValueError, IndexError):
        late = []
    pct_diffs = []
    late_actual_sum = 0.0
    late_count = 0
    for w, actual, pred in month_data:
        if actual > 0:
            pct = (pred - actual) / actual
            pct_diffs.append((w, pct, actual))
            if w in late:
                late_actual_sum += actual
                late_count += 1
    if not pct_diffs:
        return "No months with positive actuals"
    pcts = np.array([x[1] for x in pct_diffs])
    tags = []
    mean_pct = float(np.mean(pcts))
    n = len(pcts)
    consistent_over = np.sum(pcts > 0.05) >= max(1, 0.75 * n)
    consistent_under = np.sum(pcts < -0.05) >= max(1, 0.75 * n)
    if consistent_over and mean_pct > 0.10:
        pct_str = abs(int(round(mean_pct * 100)))
        tags.append(f"Systematic overprediction (avg ~{pct_str}% above actual)")
    elif consistent_under and mean_pct < -0.10:
        pct_str = abs(int(round(mean_pct * 100)))
        tags.append(f"Systematic underprediction (avg ~{pct_str}% below actual)")
    elif np.std(pcts) > 0.25 and n >= 3:
        tags.append("High month-on-month variance in bias")
    if late:
        late_tuples = [(w, actual, pred) for w, actual, pred in month_data if w in late]
        zero_preds_in_late = sum(1 for _, a, p in late_tuples if a > 0 and p < 1e-6)
        late_abs_pct_errors = []
        for w, actual, pred in late_tuples:
            if actual > 0:
                late_abs_pct_errors.append(abs(pred - actual) / actual)
        poor_accuracy_late = late_abs_pct_errors and np.mean(late_abs_pct_errors) > 0.50
        if zero_preds_in_late >= 1 or poor_accuracy_late:
            tags.append("Low M3-M6 visibility (poor accuracy / many zero predictions there)")
        elif late_tuples:
            late_pcts = [p for (w, p, a) in pct_diffs if w in late]
            if late_pcts and np.mean(late_pcts) < -0.15:
                tags.append("Underprediction in M3-M6")
            elif late_pcts and np.mean(late_pcts) > 0.15:
                tags.append("Overprediction in M3-M6")
    return "; ".join(tags) if tags else "No specific pattern"


def _weighted_overall_accuracy_row(comparison_df: pd.DataFrame, group_metrics_df: pd.DataFrame):
    """Compute one weighted-avg row and formula note. Weights = group size (Total_Actual for ALL_WINDOWS); larger groups count more."""
    if comparison_df.empty or group_metrics_df is None or group_metrics_df.empty:
        return None, None, None
    if 'Group_Label' not in group_metrics_df.columns or 'Window' not in group_metrics_df.columns or 'Total_Actual' not in group_metrics_df.columns:
        return None, None, None
    all_w = group_metrics_df[group_metrics_df['Window'] == 'ALL_WINDOWS']
    if all_w.empty:
        return None, None, None
    weights_ser = all_w.groupby(all_w['Group_Label'].astype(str), dropna=False)['Total_Actual'].sum()
    group_weights = weights_ser.to_dict()
    comparison_df = comparison_df.copy()
    comparison_df['_weight'] = comparison_df['Group'].astype(str).map(group_weights).fillna(0)
    numeric_cols = [c for c in comparison_df.columns if c not in ('Group', '_weight') and pd.api.types.is_numeric_dtype(comparison_df[c])]
    if not numeric_cols:
        return None, None, None
    weighted_vals = {}
    for col in numeric_cols:
        if col == 'Total_Volume':
            weighted_vals[col] = comparison_df['_weight'].sum()
            continue
        valid = (comparison_df['_weight'] > 0) & comparison_df[col].notna()
        if valid.any():
            w = comparison_df.loc[valid, '_weight']
            weighted_vals[col] = (comparison_df.loc[valid, col] * w).sum() / w.sum()
        else:
            weighted_vals[col] = np.nan
    weightages_str = "; ".join(f"{g}={int(w)}" for g, w in sorted(group_weights.items(), key=lambda x: -x[1])[:15])
    if len(group_weights) > 15:
        weightages_str += f" ... (+{len(group_weights) - 15} more)"
    note = f"Weighted avg by group size (Total_Actual, ALL_WINDOWS). Weights: {weightages_str}. Formula: sum(Value * Weight) / sum(Weight) over groups."
    row = {'Group': 'Weighted overall avg', **weighted_vals}
    return row, note, group_weights


def _partition_accuracy_groups(comparison_df: pd.DataFrame, group_order: list, max_groups: int,
                                publishing: bool = False):
    """Split comparison rows into top_n (first max_groups non-remainder), remainder, and remaining (rest of non-remainder)."""
    if not group_order or max_groups is None or comparison_df.empty:
        return None, None, None
    remainder_key = 'REMAINDER'
    top_n_labels, remainder_labels, remaining_labels = [], [], []
    seen_non_remainder = 0
    for lab in group_order:
        if lab not in comparison_df['Group'].values:
            continue
        if str(lab).strip().upper().startswith(remainder_key):
            remainder_labels.append(lab)
        else:
            if seen_non_remainder < max_groups:
                top_n_labels.append(lab)
                seen_non_remainder += 1
            else:
                remaining_labels.append(lab)
    _vol_col = ('Total_Predicted_Volume' if publishing and 'Total_Predicted_Volume' in comparison_df.columns
                else 'Total_Volume')
    top_n_df = comparison_df[comparison_df['Group'].isin(top_n_labels)].copy()
    if not top_n_df.empty and _vol_col in top_n_df.columns:
        top_n_df = top_n_df.sort_values(_vol_col, ascending=False, na_position='last')
    remainder_df = comparison_df[comparison_df['Group'].isin(remainder_labels)].copy() if remainder_labels else pd.DataFrame()
    if not remainder_df.empty and _vol_col in remainder_df.columns:
        remainder_df = remainder_df.sort_values(_vol_col, ascending=False, na_position='last')
    remaining_df = comparison_df[comparison_df['Group'].isin(remaining_labels)].copy()
    if not remaining_df.empty and _vol_col in remaining_df.columns:
        remaining_df = remaining_df.sort_values(_vol_col, ascending=False, na_position='last')
    return top_n_df, remainder_df, remaining_df


def _load_backfill_new_demand_ratios(group_data_dir: str, output_dir: str) -> tuple:
    """Load per-group backfill-to-new-demand split ratios and persist a copy to the output dir.

    Each group has a historical ratio of backfill SOs (positions that already
    existed in a previous period) vs. new demand SOs.  This ratio is used to
    decompose total predicted counts into Backfill and New_demand sheets in the
    combined Excel workbook, giving business stakeholders separate visibility
    into each demand category.

    Fallback hierarchy for ratio_backfill:
      1. Per-group ratio from backfill_new_demand_ratios.json  (most specific)
      2. Level-average ratio (_level_avg_ratio_backfill in the JSON)           (same grouping level mean)
      3. UNIVERSAL_RATIO_BACKFILL constant (= 7 %)  (global fallback when no file)

    The file is copied to output_dir so the results folder is self-contained
    (the original lives next to the training parquets in group_data_dir).

    Args:
        group_data_dir: Directory containing backfill_new_demand_ratios.json.
        output_dir:     Results directory where the copy is saved.

    Returns:
        tuple: (ratios_by_label, level_avg_ratio_backfill)
               ratios_by_label: dict of group_label -> {'ratio_backfill': float, ...}
               level_avg_ratio_backfill: float, mean backfill ratio at the grouping level.
    """
    ratios_path = os.path.join(group_data_dir, 'backfill_new_demand_ratios.json') if group_data_dir else None
    ratios_by_label = {}
    # Default to the universal ratio when no group-level data is available
    level_avg_ratio_backfill = UNIVERSAL_RATIO_BACKFILL
    if ratios_path and os.path.exists(ratios_path):
        try:
            with open(ratios_path, 'r') as f:
                data = json.load(f)
            # Keys starting with '_' are metadata (e.g. _level_avg_ratio_backfill); skip them
            ratios_by_label = {k: v for k, v in data.items() if not k.startswith('_') and isinstance(v, dict)}
            if ratios_by_label:
                # Try to read the pre-computed level average; fall back to computing it
                level_avg_ratio_backfill = data.get('_level_avg_ratio_backfill')
                if level_avg_ratio_backfill is None:
                    level_avg_ratio_backfill = float(np.mean([v.get('ratio_backfill', UNIVERSAL_RATIO_BACKFILL) for v in ratios_by_label.values()]))
                else:
                    level_avg_ratio_backfill = float(level_avg_ratio_backfill)
            # Persist a copy alongside the other results for reproducibility
            out_path = os.path.join(output_dir, 'backfill_new_demand_ratios.json')
            os.makedirs(output_dir, exist_ok=True)
            with open(out_path, 'w') as f:
                json.dump(data, f, indent=2)
            logging.info("  Backfill:new_demand ratios loaded and saved to results folder: %s (level_avg_backfill=%.2f%%)", out_path, level_avg_ratio_backfill * 100)
        except Exception as e:
            logging.warning("  Could not load backfill_new_demand_ratios.json: %s", e)
    return ratios_by_label, level_avg_ratio_backfill


def _build_backfill_new_demand_sheets(all_predictions_df: pd.DataFrame, ratios_by_label: dict,
                                     months_ahead: list, level_avg_ratio_backfill: float) -> tuple:
    """Build Backfill and New_demand sheets: actuals/predictions split by ratio (same-level avg or universal). M*_Actual, M*_Predicted, M*_Accuracy_%, weighted avg row."""
    if all_predictions_df is None or all_predictions_df.empty or not months_ahead:
        return None, None
    non_m_cols = [c for c in all_predictions_df.columns if not re.match(r"M\d+_(Actual|Predicted)$", str(c))]
    month_cols = []
    for m in months_ahead:
        month_cols.append((m, f"M{m}_Actual", f"M{m}_Predicted"))
    # Filter to columns that exist
    month_cols = [(m, a, p) for m, a, p in month_cols if a in all_predictions_df.columns and p in all_predictions_df.columns]
    if not month_cols:
        return None, None

    def _one_sheet(df: pd.DataFrame, ratio_key: str, ratio_mult: float) -> pd.DataFrame:
        rows = []
        for _, row in df.iterrows():
            group_label = row.get('Group_Label') if 'Group_Label' in row else None
            if pd.isna(group_label):
                group_label = ''
            # Look up this group's historical backfill ratio.
            # Fallback order: per-group ratio -> level average -> UNIVERSAL_RATIO_BACKFILL.
            rec = ratios_by_label.get(str(group_label), {})
            ratio_backfill = float(rec.get('ratio_backfill', level_avg_ratio_backfill))
            # Determine the split fraction for the requested sheet type:
            #   'backfill'    -> use ratio_backfill directly (e.g. 0.07 = 7%)
            #   'new_demand'  -> use 1 - ratio_backfill (e.g. 0.93 = 93%)
            if ratio_key == 'new_demand':
                r = 1.0 - ratio_backfill
            else:
                r = ratio_backfill
            # ratio_mult allows future scaling (currently always 1.0); clamp to [0, 1]
            r = max(0.0, min(1.0, r * ratio_mult))
            new_row = {c: row[c] for c in non_m_cols}
            for m, actual_col, pred_col in month_cols:
                act = float(row.get(actual_col, 0) or 0)
                pred = float(row.get(pred_col, 0) or 0)
                part_act = act * r
                part_pred = pred * r
                acc = np.nan
                if part_act > 0:
                    ape = min(1.0, abs(part_pred - part_act) / part_act)
                    acc = (1.0 - ape) * 100
                new_row[actual_col] = round(part_act, 2)
                new_row[pred_col] = round(part_pred, 2)
                new_row[f"M{m}_Accuracy_%"] = round(acc, 2) if not np.isnan(acc) else np.nan
            rows.append(new_row)
        out = pd.DataFrame(rows)
        # Weighted average row (weight by M0_Actual)
        m0_actual_col = f"M{months_ahead[0]}_Actual"
        if m0_actual_col in out.columns and out[m0_actual_col].notna().any():
            w = out[m0_actual_col].fillna(0)
            if w.sum() > 0:
                avg_row = {c: np.nan for c in out.columns}
                for c in non_m_cols:
                    avg_row[c] = '' if c == 'Group_Label' else np.nan
                avg_row['Group_Label'] = 'Weighted avg'
                for m, actual_col, pred_col in month_cols:
                    acc_col = f"M{m}_Accuracy_%"
                    if acc_col in out.columns:
                        valid = (w > 0) & out[acc_col].notna()
                        if valid.any() and w[valid].sum() > 0:
                            avg_row[acc_col] = round(float((out.loc[valid, acc_col] * w[valid]).sum() / w[valid].sum()), 2)
                    avg_row[actual_col] = round(float(out[actual_col].sum()), 2)
                    avg_row[pred_col] = round(float(out[pred_col].sum()), 2)
                out = pd.concat([out, pd.DataFrame([avg_row])], ignore_index=True)
        return out

    backfill_df = _one_sheet(all_predictions_df, 'backfill', 1.0)
    new_demand_df = _one_sheet(all_predictions_df, 'new_demand', 1.0)
    return backfill_df, new_demand_df


def _build_unified_all_predictions(combined_frames: dict, global_frames: dict) -> pd.DataFrame | None:
    """Build All_Predictions with individual model rows plus global model rows for groups without individual models."""
    if not combined_frames or not combined_frames.get('All_Predictions'):
        return None
    pred_list = combined_frames['All_Predictions']
    pred_list_sorted = sorted(
        pred_list,
        key=lambda df: (1 if (not df.empty and 'Group_Label' in df.columns and
            str(df['Group_Label'].iloc[0]).strip().upper().startswith('REMAINDER')) else 0)
    )
    ind_df = pd.concat(pred_list_sorted, ignore_index=True)
    ind_df = ind_df.copy()
    ind_df['Prediction_Source'] = 'Individual'

    if not global_frames or global_frames.get('All_Predictions') is None or global_frames['All_Predictions'].empty:
        return ind_df

    glob_df = global_frames['All_Predictions'].copy()
    group_cols = [c for c in glob_df.columns if not str(c).endswith('_Actual') and not str(c).endswith('_Predicted')]
    if not group_cols:
        return ind_df
    glabel_parts = [glob_df[c].astype(str).radd(f"{c}=") for c in group_cols]
    glob_df = glob_df.copy()
    glob_df['Group_Label'] = glabel_parts[0]
    for p in glabel_parts[1:]:
        glob_df['Group_Label'] = glob_df['Group_Label'] + ", " + p
    individual_labels = set(ind_df['Group_Label'].astype(str).unique())
    global_only = glob_df[~glob_df['Group_Label'].astype(str).isin(individual_labels)]
    if global_only.empty:
        return ind_df
    best_global = global_frames.get('Best_Model_Name') or 'global_model'
    global_only = global_only.copy()
    global_only['Model_Name'] = f"global_model: {_sanitize_model_name(best_global)}"
    global_only['Prediction_Source'] = 'Global'
    if 'Prediction_Source' not in ind_df.columns:
        ind_df['Prediction_Source'] = 'Individual'
    common_cols = list(ind_df.columns)
    for c in common_cols:
        if c not in global_only.columns:
            global_only[c] = np.nan
    global_only = global_only[common_cols]
    ind_df = ind_df[common_cols]
    return pd.concat([ind_df, global_only], ignore_index=True)


def _write_combined_excel(combined_frames: dict, output_dir: str, global_frames: dict = None,
                          group_order: list = None, max_groups: int = None,
                          group_data_dir: str = None, months_ahead: list = None,
                          publishing: bool = False) -> None:
    """Write a combined Excel workbook for all groups and optionally global model sheets. Adds Backfill and New_demand sheets after Global_vs_Individual_Accuracy when group_data_dir/months_ahead are provided."""
    if not combined_frames and not global_frames:
        return

    has_data = any(combined_frames.get(sheet) for sheet in ['All_Predictions', 'Overall_Metrics', 'Group_Metrics']) if combined_frames else False
    has_global = bool(global_frames)
    if not has_data and not has_global:
        logging.warning("No combined or global frames available; skipping combined Excel.")
        return

    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    combined_path = os.path.join(output_dir, f'combined_group_results_{timestamp}.xlsx')
    ratios_by_label, level_avg_ratio_backfill = _load_backfill_new_demand_ratios(group_data_dir or '', output_dir) if (group_data_dir or output_dir) else ({}, UNIVERSAL_RATIO_BACKFILL)

    def _compute_model_averages(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
        if df is None or df.empty:
            return None
        df = df.copy()
        if 'Model_Name' in df.columns:
            df = df.drop(columns=['Model_Name'])
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            return None
        group_cols = [col for col in df.columns if col not in numeric_cols]
        avg_df = df.groupby(group_cols, dropna=False)[numeric_cols].mean().reset_index()
        avg_df['Source_Sheet'] = source_label
        return avg_df

    def _blank_row(cols):
        return pd.DataFrame([{c: np.nan for c in cols}])

    all_predictions_df = None
    overall_df = None
    group_metrics_df = None

    with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
        if combined_frames:
            if combined_frames.get('All_Predictions'):
                all_predictions_df = _build_unified_all_predictions(combined_frames, global_frames or {})
                if all_predictions_df is not None and not all_predictions_df.empty:
                    # Sort rows: publishing → by M0_Predicted (no actuals yet); normal → by M0_Actual
                    _sort_suffix = '_Predicted' if publishing else '_Actual'
                    _sort_candidates = [c for c in all_predictions_df.columns if c.startswith('M0') and c.endswith(_sort_suffix)]
                    if _sort_candidates:
                        all_predictions_df = all_predictions_df.sort_values(_sort_candidates[0], ascending=False, na_position='last')
                    _round_df_for_excel(all_predictions_df).to_excel(writer, sheet_name='All_Predictions', index=False)
            if combined_frames.get('Overall_Metrics'):
                overall_df = pd.concat(combined_frames['Overall_Metrics'], ignore_index=True)
            if combined_frames.get('Group_Metrics'):
                group_metrics_df = pd.concat(combined_frames['Group_Metrics'], ignore_index=True)

            if global_frames and not publishing:
                comparison_df = _build_accuracy_comparison_df(combined_frames, global_frames)
                if not comparison_df.empty:
                    comparison_df = comparison_df.copy()
                    _vol_sort_col = ('Total_Predicted_Volume' if publishing and 'Total_Predicted_Volume' in comparison_df.columns
                                     else 'Total_Volume')
                    if _vol_sort_col in comparison_df.columns:
                        comparison_df = comparison_df.sort_values(_vol_sort_col, ascending=False, na_position='last')
                    out_cols = list(comparison_df.columns) + ['Note']
                    group_weights = _get_group_weights(group_metrics_df) if group_metrics_df is not None else {}

                    def _note_with_weight(group_val):
                        w = group_weights.get(str(group_val), 0)
                        return f"Global vs Individual; Weight: {int(w)}"

                    top_n_df, remainder_df, remaining_df = _partition_accuracy_groups(
                        comparison_df, group_order or [], max_groups, publishing=publishing
                    )
                    use_structured = (
                        group_order is not None and len(group_order) > 0 and max_groups is not None and
                        top_n_df is not None and (not top_n_df.empty or not remainder_df.empty or not remaining_df.empty)
                    )
                    if use_structured and (not top_n_df.empty or not remainder_df.empty or not remaining_df.empty):
                        parts = []
                        if not top_n_df.empty:
                            top_n_df = top_n_df.copy()
                            top_n_df['Note'] = top_n_df['Group'].map(_note_with_weight)
                            parts.append(top_n_df)
                            weighted_top, formula_note, _ = _weighted_overall_accuracy_row(
                                top_n_df, group_metrics_df
                            )
                            if weighted_top is not None and formula_note is not None:
                                blank = _blank_row(out_cols)
                                blank['Group'] = ''
                                row_dict = {c: weighted_top.get(c, np.nan) for c in comparison_df.columns}
                                row_dict['Note'] = formula_note
                                parts.append(blank)
                                parts.append(pd.DataFrame([row_dict])[out_cols])
                        # Global-only groups (in global but no individual model): after weighted row, 2 blanks, then list + weighted avg
                        individual_group_set = set(group_order or [])
                        global_only_groups_df = comparison_df[~comparison_df['Group'].astype(str).isin(individual_group_set)]
                        if not global_only_groups_df.empty:
                            global_only_groups_df = global_only_groups_df.copy()
                            _gog_vol_col = ('Total_Predicted_Volume' if publishing and 'Total_Predicted_Volume' in global_only_groups_df.columns
                                            else 'Total_Volume')
                            if _gog_vol_col in global_only_groups_df.columns:
                                global_only_groups_df = global_only_groups_df.sort_values(_gog_vol_col, ascending=False, na_position='last')
                            global_only_groups_df['Note'] = 'Global model only'
                            parts.append(_blank_row(out_cols))
                            parts.append(_blank_row(out_cols))
                            parts.append(global_only_groups_df[out_cols])
                            # Build synthetic group_metrics (Group_Label, Total_Actual) from global Group_Metrics for weighting
                            _metric_cols = {'Metric_Type', 'Window', 'Total_Actual', 'Total_NonZero_Predicted', 'MAPE_%', 'Accuracy', 'wMAPE_%', 'sMAPE_%'}
                            _glob_gm = global_frames.get('Group_Metrics') if global_frames else None
                            global_weights_df = None
                            if _glob_gm is not None and not _glob_gm.empty and 'Window' in _glob_gm.columns:
                                _gcols = [c for c in _glob_gm.columns if c not in _metric_cols]
                                _all_w = _glob_gm[_glob_gm['Window'] == 'ALL_WINDOWS']
                                if not _all_w.empty and _gcols:
                                    _gw = _all_w.copy()
                                    _gwp = [_gw[c].astype(str).radd(f"{c}=") for c in _gcols]
                                    _gw['Group_Label'] = _gwp[0]
                                    for _p in _gwp[1:]:
                                        _gw['Group_Label'] = _gw['Group_Label'] + ", " + _p
                                    global_weights_df = _gw[['Group_Label', 'Window', 'Total_Actual']].copy()
                            weighted_global_only, note_global_only, _ = _weighted_overall_accuracy_row(
                                global_only_groups_df, global_weights_df
                            )
                            if weighted_global_only is not None and note_global_only is not None:
                                blank = _blank_row(out_cols)
                                blank['Group'] = ''
                                row_dict_go = {c: weighted_global_only.get(c, np.nan) for c in comparison_df.columns}
                                row_dict_go['Note'] = f"Weighted avg (global-only groups). {note_global_only}"
                                parts.append(blank)
                                parts.append(pd.DataFrame([row_dict_go])[out_cols])
                        parts.append(_blank_row(out_cols))
                        parts.append(_blank_row(out_cols))
                        if not remainder_df.empty:
                            remainder_df = remainder_df.copy()
                            remainder_df['Note'] = remainder_df['Group'].map(_note_with_weight)
                            parts.append(remainder_df)
                        parts.append(_blank_row(out_cols))
                        if not remaining_df.empty:
                            remaining_df = remaining_df.copy()
                            remaining_df['Note'] = remaining_df['Group'].map(_note_with_weight)
                            parts.append(remaining_df)
                        weighted_remaining, note_remaining, _ = _weighted_overall_accuracy_row(
                            remaining_df if not remaining_df.empty else pd.DataFrame(), group_metrics_df
                        )
                        if weighted_remaining is not None and note_remaining is not None and not remaining_df.empty:
                            row_rem = {c: weighted_remaining.get(c, np.nan) for c in comparison_df.columns}
                            row_rem['Note'] = "Weighted avg of remaining (left-out) groups."
                            parts.append(pd.DataFrame([row_rem])[out_cols])
                        out_df = pd.concat(parts, ignore_index=True)
                    else:
                        out_df = comparison_df.copy()
                        out_df['Note'] = out_df['Group'].map(_note_with_weight)
                        weighted_row, formula_note, _ = _weighted_overall_accuracy_row(comparison_df, group_metrics_df)
                        if weighted_row is not None and formula_note is not None:
                            blank = _blank_row(out_cols)
                            blank['Group'] = ''
                            row_dict = {c: weighted_row.get(c, np.nan) for c in comparison_df.columns}
                            row_dict['Note'] = formula_note
                            out_df = pd.concat([
                                out_df,
                                blank,
                                pd.DataFrame([row_dict])[out_cols],
                            ], ignore_index=True)
                    _round_df_for_excel(out_df).to_excel(writer, sheet_name='Global_vs_Individual_Accuracy', index=False)

            # Backfill and New_demand sheets: actuals/predictions split by ratio (group -> level avg -> universal 7%)
            if all_predictions_df is not None and not all_predictions_df.empty and months_ahead:
                backfill_df, new_demand_df = _build_backfill_new_demand_sheets(
                    all_predictions_df, ratios_by_label, months_ahead, level_avg_ratio_backfill
                )
                if backfill_df is not None and not backfill_df.empty:
                    _round_df_for_excel(backfill_df).to_excel(writer, sheet_name='Backfill', index=False)
                if new_demand_df is not None and not new_demand_df.empty:
                    _round_df_for_excel(new_demand_df).to_excel(writer, sheet_name='New_demand', index=False)

            if not publishing:
                if combined_frames.get('Overall_Metrics'):
                    _round_df_for_excel(overall_df).to_excel(writer, sheet_name='Overall_Metrics', index=False)
                if combined_frames.get('Group_Metrics'):
                    _round_df_for_excel(group_metrics_df).to_excel(writer, sheet_name='Group_Metrics', index=False)

                avg_frames = []
                avg_overall = _compute_model_averages(overall_df, 'Overall_Metrics')
                if avg_overall is not None:
                    avg_frames.append(avg_overall)
                avg_group = _compute_model_averages(group_metrics_df, 'Group_Metrics')
                if avg_group is not None:
                    avg_frames.append(avg_group)
                if avg_frames:
                    _round_df_for_excel(pd.concat(avg_frames, ignore_index=True)).to_excel(
                        writer, sheet_name='Model_Averages', index=False
                    )

        if global_frames:
            if global_frames.get('All_Predictions') is not None and not global_frames['All_Predictions'].empty:
                _round_df_for_excel(global_frames['All_Predictions']).to_excel(writer, sheet_name='Global_All_Predictions', index=False)
            if not publishing:
                if global_frames.get('Overall_Metrics') is not None and not global_frames['Overall_Metrics'].empty:
                    _round_df_for_excel(global_frames['Overall_Metrics']).to_excel(writer, sheet_name='Global_Overall_Metrics', index=False)
                if global_frames.get('Group_Metrics') is not None and not global_frames['Group_Metrics'].empty:
                    _round_df_for_excel(global_frames['Group_Metrics']).to_excel(writer, sheet_name='Global_Group_Metrics', index=False)

    logging.info(f"  Combined Excel saved: {combined_path}")

def _find_latest_autogluon_path(models_dir):
    """Find the most recent AutoGluon predictor directory."""
    if not models_dir or not os.path.exists(models_dir):
        return None

    candidates = []
    for path in Path(models_dir).glob('autogluon_temp_*'):
        if path.is_dir() and (path / 'predictor.pkl').exists():
            candidates.append(path)

    if not candidates:
        return None

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return str(latest)

def _resolve_autogluon_path(autogluon_path, models_dir, reuse_autogluon, run_id):
    """Resolve AutoGluon path from args or existing predictors."""
    if autogluon_path:
        return autogluon_path

    if reuse_autogluon:
        return _find_latest_autogluon_path(models_dir)

    return os.path.join(models_dir, f'autogluon_temp_{run_id}')

def _split_train_validation(df_train, validation_fraction=0.1, date_col='cutoff_date'):
    """Split train into train/validation using last fraction by date."""
    if df_train is None or len(df_train) == 0:
        return df_train, df_train

    df_sorted = df_train
    if date_col in df_train.columns:
        try:
            df_sorted = df_train.sort(date_col)
        except Exception as e:
            logging.warning(f"  Could not sort by {date_col} for validation split: {e}")

    total_rows = len(df_sorted)
    val_size = max(1, int(total_rows * validation_fraction))
    if val_size >= total_rows:
        val_size = max(1, total_rows - 1)

    split_idx = total_rows - val_size
    df_train_split = df_sorted.slice(0, split_idx)
    df_val_split = df_sorted.slice(split_idx, val_size)

    return df_train_split, df_val_split

def _prepare_autogluon_frame(df_pd, target_col, include_target):
    """Build AutoGluon frame with categorical forecast horizon."""
    df_work = df_pd.copy()

    # Treat forecast horizon as categorical to mirror custom CatBoost behavior.
    if 'window_start' in df_work.columns:
        horizon_numeric = pd.to_numeric(df_work['window_start'], errors='coerce')
        horizon_int = horizon_numeric.round().astype('Int64')
        df_work['window_start'] = horizon_int
        df_work['window_start_cat'] = horizon_int.astype('string')

    exclude_cols = ['cutoff_date']
    if target_col in df_work.columns:
        exclude_cols.append(target_col)
    if 'window_start_cat' in df_work.columns:
        exclude_cols.append('window_start')

    feature_cols = [col for col in df_work.columns if col not in exclude_cols]
    frame = df_work[feature_cols].copy()

    if include_target and target_col in df_work.columns:
        frame[target_col] = df_work[target_col].values

    return frame, feature_cols


def _run_model_shap(predictor, df_feature_pd, shap_plot_dir, model_label='global_model',
                    sample_background=50, sample_explain=100, max_display=None):
    """Run SHAP (SHapley Additive exPlanations) analysis for an AutoGluon predictor.

    SHAP is a game-theoretic method that assigns each feature a contribution
    score for every prediction.  Positive SHAP values push the prediction
    above the baseline; negative values push it below.  The summary plot
    shows both the direction and magnitude of each feature's influence,
    making the model interpretable to business stakeholders.

    Because AutoGluon uses an ensemble of internal models, we wrap the
    predictor in a callable and use SHAP's model-agnostic KernelExplainer.
    KernelExplainer is slower than tree-specific explainers but works for
    any black-box model.

    Two plots are produced and saved:
      shap_summary_<label>.png  -- Beeswarm plot: each dot = one sample,
                                   colour = feature value, x-axis = SHAP value.
      shap_bar_<label>.png      -- Bar plot: mean |SHAP| per feature (global importance).

    Both plots use a signed-sqrt x-axis scale so that small/medium SHAP values
    remain visible even when a few outliers have large absolute SHAP values.

    Args:
        predictor:       Trained AutoGluon TabularPredictor.
        df_feature_pd:   pandas DataFrame of feature rows (no target column).
        shap_plot_dir:   Directory where PNG plots are saved.
        model_label:     Label used in output file names.
        sample_background: Number of background rows for KernelExplainer baseline.
        sample_explain:  Number of rows to explain (subsample of full dataset).
        max_display:     Max features to show in plots; defaults to TOP_N_SHAP_FEATURES.
    """
    if max_display is None:
        max_display = TOP_N_SHAP_FEATURES
    if df_feature_pd is None or df_feature_pd.empty:
        logging.warning("  No feature data for SHAP; skipping SHAP analysis for %s.", model_label)
        return
    try:
        if not isinstance(df_feature_pd, pd.DataFrame):
            df_feature_pd = df_feature_pd.to_pandas() if hasattr(df_feature_pd, 'to_pandas') else pd.DataFrame(df_feature_pd)
        os.makedirs(shap_plot_dir, exist_ok=True)
        n = len(df_feature_pd)

        # Sample a small background dataset used by KernelExplainer to estimate
        # the expected model output (the SHAP baseline / reference value).
        background_size = min(sample_background, n)
        explain_size = min(sample_explain, n)
        background = df_feature_pd.sample(n=background_size, random_state=RANDOM_SEED, replace=(n < background_size))

        # Sample the rows we actually want to explain (may differ from background)
        explain_sample = df_feature_pd.sample(n=explain_size, random_state=RANDOM_SEED + 1, replace=(n < explain_size))
        feature_names = list(explain_sample.columns)

        # Wrap AutoGluon's predict so SHAP can treat the model as a callable.
        # When SHAP passes a numpy array (not a DataFrame), we reconstruct
        # the column names so AutoGluon can handle it correctly.
        def _predict_for_shap(X):
            if isinstance(X, pd.DataFrame):
                return predictor.predict(X)
            return predictor.predict(pd.DataFrame(X, columns=feature_names))

        logging.info("  Computing SHAP values for %s (KernelExplainer; may take a few minutes)...", model_label)

        # Build KernelExplainer using the background sample as the reference distribution.
        # nsamples controls the number of coalition samples per explanation (speed vs accuracy).
        explainer = shap.KernelExplainer(_predict_for_shap, background)
        shap_values = explainer.shap_values(explain_sample, nsamples=min(50, 2 * background_size))

        # KernelExplainer may return a list (one array per output); for regression
        # with a single output we take the first element.
        if hasattr(shap_values, 'shape') and len(shap_values.shape) == 2:
            pass  # Already a 2-D array (samples x features) -- correct shape
        elif isinstance(shap_values, list) and len(shap_values) > 0:
            shap_values = shap_values[0]

        # Signed sqrt scale: compresses large SHAP values so small/medium contributions
        # remain visible on the same axis.  Gentler than log (which is undefined at 0).
        def _shap_forward(x):
            return np.sign(x) * np.sqrt(np.abs(x))

        def _shap_inverse(x):
            return np.sign(x) * (x ** 2)

        # Plot 1: Beeswarm summary plot showing direction + magnitude per feature
        plt.figure(figsize=(12, 10))
        shap.summary_plot(shap_values, explain_sample, feature_names=feature_names, max_display=max_display, show=False)
        ax = plt.gca()
        ax.set_xscale('function', functions=(_shap_forward, _shap_inverse))
        plt.tight_layout()
        out_summary = os.path.join(shap_plot_dir, f'shap_summary_{model_label}.png')
        plt.savefig(out_summary, dpi=300, bbox_inches='tight')
        plt.close()

        # Plot 2: Bar chart of mean absolute SHAP values (global feature importance)
        plt.figure(figsize=(12, 8))
        shap.summary_plot(shap_values, explain_sample, feature_names=feature_names, plot_type='bar', max_display=max_display, show=False)
        ax = plt.gca()
        ax.set_xscale('function', functions=(_shap_forward, _shap_inverse))
        plt.tight_layout()
        out_bar = os.path.join(shap_plot_dir, f'shap_bar_{model_label}.png')
        plt.savefig(out_bar, dpi=300, bbox_inches='tight')
        plt.close()
        logging.info("  SHAP plots saved: %s, %s", out_summary, out_bar)
    except Exception as e:
        logging.warning("  SHAP analysis failed for %s: %s", model_label, e)
        logging.debug(traceback.format_exc())


def _evaluate_autogluon_variants(
    predictor,
    df_test_filtered,
    df_test_eval,
    group_by_cols,
    months_ahead,
    round_predictions,
    target_col
):
    """Evaluate all AutoGluon models using two-phase aggregated metrics."""
    val_pd = df_test_filtered.to_pandas() if hasattr(df_test_filtered, "to_pandas") else df_test_filtered.copy()
    # val_frame_with_target still needed for leaderboard discovery
    val_frame_with_target, _ = _prepare_autogluon_frame(val_pd, target_col, include_target=True)

    model_names = None
    if hasattr(predictor, "model_names"):
        try:
            model_names = predictor.model_names()
        except Exception:
            model_names = None
    if not model_names and hasattr(predictor, "get_model_names"):
        try:
            model_names = predictor.get_model_names()
        except Exception:
            model_names = None

    if not model_names:
        try:
            leaderboard = predictor.leaderboard(val_frame_with_target, silent=True)
            if leaderboard is not None and not leaderboard.empty and 'model' in leaderboard.columns:
                if 'can_infer' in leaderboard.columns:
                    leaderboard = leaderboard[leaderboard['can_infer'] == True]
                model_names = leaderboard['model'].tolist()
        except Exception as e:
            logging.warning(f"  Could not read AutoGluon leaderboard for model list: {e}")

    if not model_names:
        logging.warning("  No AutoGluon model names available for evaluation")
        return []

    results = []
    for model_name in model_names:
        try:
            # Two-phase prediction per variant: M0-M2 first, then bridge-updated M3-M5
            y_pred = _two_phase_predict(
                predictor, val_pd, group_by_cols,
                round_predictions=round_predictions,
                model_name=model_name,
                target_col=target_col,
            )
            metrics = calculate_aggregated_metrics(
                df_test_eval,
                y_pred,
                group_by_cols,
                months_ahead,
                model_name=model_name
            )
            if metrics:
                results.append(metrics)
        except Exception as e:
            logging.warning(f"  Skipping AutoGluon model {model_name}: {e}")

    return results

def train_autogluon(df_train, df_val, target_col='target_count', time_limit=None, group_by_cols=None, predictor_path=None):
    """Train an AutoGluon TabularPredictor optimised for zero-inflated count data.

    Uses fixed hyperparameters (no AutoGluon HPO) to avoid pickle-file race
    conditions on shared / cluster file systems.  The predictor directory is
    cleared before training so stale artefacts do not interfere.

    Model suite:
      - LightGBM (GBM):  standard count-data booster.
      - CatBoost (CAT):  Tweedie loss (variance_power=1.5) well-suited to
                         sparse/intermittent demand.
      - XGBoost  (XGB):  Poisson objective for non-negative count targets.
      - Neural Net (NN_TORCH) and FastAI: included as diversity models for
                         the weighted-ensemble stacker.

    All base models are bagged (5 folds) and stacked (1 level) under the
    AutoGluon 'high_quality' preset.

    Args:
        df_train: Polars DataFrame of training rows (features + target_col).
        df_val:   Polars DataFrame of validation rows (same schema; used as
                  AutoGluon tuning_data so it can detect overfitting).
        target_col: Name of the column to predict (default 'target_count').
        time_limit: Optional hard wall-clock cap in seconds; None = unlimited.
        group_by_cols: List of group-by column names (used only to build the
                       predictor directory suffix; does not affect training).
        predictor_path: Explicit path for saving the predictor.  If None, a
                        default path under models/ is derived automatically.

    Returns:
        TabularPredictor: Trained AutoGluon predictor, or None on failure.
    """
    if not HAVE_AUTOGLUON:
        logging.warning("  AutoGluon not available, skipping...")
        return None

    logging.info("\n  Training AutoGluon model (optimized for count data, this may take 10-30 minutes)...")

    # Step 1: Convert Polars DataFrames to pandas for AutoGluon compatibility
    train_pd = df_train.to_pandas()
    val_pd = df_val.to_pandas()

    # Step 2: Build feature frames (adds window_start_cat, drops excluded cols)
    train_subset, _ = _prepare_autogluon_frame(train_pd, target_col, include_target=True)
    val_subset, _ = _prepare_autogluon_frame(val_pd, target_col, include_target=True)

    # Step 3: Determine predictor save path and clean up any previous run
    if predictor_path is None:
        path_suffix = _autogluon_path_for_group_by(group_by_cols or [])
        predictor_path = f'models/autogluon_temp_{path_suffix}/'
    if os.path.exists(predictor_path):
        shutil.rmtree(predictor_path)
        logging.info(f"  Cleaned up existing AutoGluon directory: {predictor_path}")

    # Step 4: Instantiate the predictor (regression, RMSE eval metric)
    predictor = TabularPredictor(
        label=target_col,
        problem_type='regression',
        eval_metric='root_mean_squared_error',
        path=predictor_path
    )

    try:
        # Step 5: Define fixed hyperparameters for count data.
        # HPO is deliberately disabled (no hyperparameter search) to prevent
        # concurrent pickle writes that can corrupt artefacts on shared filesystems.
        hyperparameters = {
            'GBM': [{'num_boost_round': 1000, 'learning_rate': 0.05, 'num_leaves': 64, 'min_data_in_leaf': 20, 'max_depth': 6}],
            'CAT': [{
                'iterations': 1000,
                'learning_rate': 0.0678,
                'depth': 4,
                'l2_leaf_reg': 3.7459,
                'border_count': 90,
                'bootstrap_type': 'Bayesian',
                'one_hot_max_size': 10,
                'loss_function': 'Tweedie:variance_power=1.5',
                'eval_metric': 'RMSE',
                'random_seed': RANDOM_SEED,
            }],
            'XGB': [{'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 6, 'min_child_weight': 3, 'objective': 'count:poisson'}],
            'NN_TORCH': {},
            'FASTAI': {},
        }

        # Step 6: Build TabularPredictor.fit() keyword arguments.
        # Key settings explained:
        #   presets='high_quality'   -- enables bagging + stacking for best accuracy
        #   num_bag_folds=5          -- 5-fold cross-validation bagging
        #   num_bag_sets=1           -- one set of bags (more sets = more diversity but slower)
        #   num_stack_levels=1       -- one level of model stacking (L2 stacker on top of L1 bags)
        #   use_bag_holdout=True     -- reserves a holdout from the training bag for stacker
        #   dynamic_stacking=False   -- disabled so stacker always runs (stability)
        #   tuning_data              -- val_subset used to detect early stopping, not for stacker
        fit_kwargs = {
            'train_data': train_subset,
            'tuning_data': val_subset,
            'presets': 'high_quality',
            'hyperparameters': hyperparameters,
            'num_bag_folds': 5,
            'num_bag_sets': 1,
            'num_stack_levels': 1,
            'use_bag_holdout': True,
            'dynamic_stacking': False,
            'verbosity': 2,
            'ag_args_fit': {
                'ag.max_memory_usage_ratio': 1.0,
                'ag.max_time_limit_ratio': 1.0,
                'ag.max_time_limit': None,
            }
        }

        # Add time_limit only if the caller specified one (None = run to completion)
        if time_limit is not None:
            fit_kwargs['time_limit'] = time_limit

        # Step 7: Fit the predictor (this is the main training call)
        predictor.fit(**fit_kwargs)

        # Step 8: Log the AutoGluon leaderboard so we can see all models tried
        try:
            leaderboard = predictor.leaderboard(val_subset, silent=True)
            if leaderboard is not None and not leaderboard.empty:
                lb_display = leaderboard.fillna('N/A')
                logging.info("\n  AutoGluon Leaderboard (all models tried, NA = not available):")
                logging.info(f"  {lb_display.to_string()}")
            else:
                logging.info("\n  AutoGluon Leaderboard: (empty or N/A)")
        except Exception as e:
            logging.warning(f"  Could not retrieve AutoGluon leaderboard table: {e}")

        try:
            model_info = predictor.info()
            best_model_name = (
                model_info.get('model_best')
                or model_info.get('best_model')
                or model_info.get('leaderboard', [{}])[0].get('model', None)
            )
            if best_model_name:
                logging.info(f"\n  Best AutoGluon model (from predictor.info): {best_model_name}")
        except Exception as e:
            logging.warning(f"  Could not retrieve detailed AutoGluon model info: {e}")

        return predictor
    except Exception as e:
        logging.error(f"  AutoGluon training failed: {e}")
        logging.debug(traceback.format_exc())
        return None

def _compute_bridge_features_from_predictions(sma_3m, sma_6m, demand_growth_3m,
                                               recent_vs_hist, trend_slope_6m,
                                               momentum_30_vs_90, m0, m1):
    """Recompute all bridge features using M0-M1 predicted counts.

    During test Phase 2 we only have the existing pre-cutoff feature values
    and the two predicted monthly counts. Pre-cutoff monthly sums are
    reconstructed from the available SMA columns so that all bridge
    features stay consistent with training-time definitions.
    Bridge uses M0, M1 (2 months); pre-cutoff reference stays at 3 months.
    """
    _ALPHA = 0.3
    _MIN_GROWTH = 5
    bc = [m0, m1]

    # Reconstruct pre-cutoff 3-month sum from sma_3m (avg = sum/3)
    sum_pc3 = sma_3m * 3.0
    # Reconstruct pre-cutoff 6-month sum from sma_6m
    sum_pc6 = sma_6m * 6.0
    # Approximate the 3 individual pre-cutoff months as equal shares
    pc3 = [sum_pc3 / 3.0] * 3

    # Bridge SMA / WMA 3m (M0, M1 only — 2-month bridge)
    b_sma_3 = max(0.0, float(np.mean(bc)))
    w3 = np.power(1 - _ALPHA, np.arange(2))[::-1]
    w3 = w3 / w3.sum()
    b_wma_3 = max(0.0, float(np.sum(w3 * np.array(bc))))

    # Bridge SMA / WMA 6m (pre-cutoff 3m + bridge 2m = 5m window)
    ext6 = pc3 + bc
    b_sma_6 = max(0.0, float(np.mean(ext6)))
    w6 = np.power(1 - _ALPHA, np.arange(5))[::-1]
    w6 = w6 / w6.sum()
    b_wma_6 = max(0.0, float(np.sum(w6 * np.array(ext6))))

    # Bridge demand growth 3m: bridge period vs pre-cutoff 3m
    sum_bridge = sum(bc)
    if sum_pc3 >= _MIN_GROWTH:
        raw_g = (sum_bridge - sum_pc3) / sum_pc3 * 100.0
        b_growth_3m = float(min(max(raw_g, -300.0), 300.0))
    else:
        b_growth_3m = 0.0

    # Bridge recent-vs-historical ratio
    mean_bridge = np.mean(bc)
    mean_pc6 = sum_pc6 / 6.0 if sum_pc6 > 0 else 0.1
    ratio_bh = mean_bridge / max(mean_pc6, 0.1)
    b_ratio = float(min(max(ratio_bh, 0.2), 5.0))

    # Bridge trend slope (normalized linear regression over 5m window)
    ext6_arr = np.array(ext6, dtype=float)
    x_t = np.arange(5, dtype=float)
    mean_ext = np.mean(ext6_arr)
    if mean_ext >= 1.0 and np.std(ext6_arr) > 1e-6:
        n_t = 5
        num_t = n_t * np.sum(x_t * ext6_arr) - np.sum(x_t) * np.sum(ext6_arr)
        den_t = n_t * np.sum(x_t ** 2) - np.sum(x_t) ** 2
        slope_t = num_t / max(den_t, 1e-10)
        b_slope = float(min(max(slope_t / max(mean_ext, 1.0), -2.0), 2.0))
    else:
        b_slope = 0.0

    # Bridge momentum (M1 vs bridge average — M1 is now last bridge month)
    avg_bc = np.mean(bc)
    b_momentum = float(bc[1] / avg_bc) if avg_bc > 0 else 1.0

    return {
        'bridge_sma_3m': b_sma_3,
        'bridge_wma_3m': b_wma_3,
        'bridge_sma_6m': b_sma_6,
        'bridge_wma_6m': b_wma_6,
        'bridge_demand_growth_3m': b_growth_3m,
        'bridge_recent_vs_hist': b_ratio,
        'bridge_trend_slope_6m': b_slope,
        'bridge_momentum': b_momentum,
        'bridge_m0_count': float(bc[0]),
        'bridge_m1_count': float(bc[1]),
    }


# All column names populated by the bridge feature system
_BRIDGE_COLS = [
    'bridge_sma_3m', 'bridge_wma_3m', 'bridge_sma_6m', 'bridge_wma_6m',
    'bridge_demand_growth_3m', 'bridge_recent_vs_hist',
    'bridge_trend_slope_6m', 'bridge_momentum',
    'bridge_m0_count', 'bridge_m1_count',
]


def _two_phase_predict(predictor, df_test_pd, group_by_cols, round_predictions=False,
                       model_name=None, target_col='target_count'):
    """Two-phase prediction: M0-M1 first, then recompute bridge features for M2-M5.

    Phase 1 predicts short-horizon rows (window_start <= 1) whose bridge
    features already equal the pre-cutoff originals.
    Phase 2 uses the M0-M1 predictions to recompute every bridge column
    for far-horizon rows (window_start >= 2), giving the model fresh
    demand signals it would otherwise lack. M2 is now a Phase 2 prediction,
    benefiting from bridge features computed from M0 and M1.

    Gracefully degrades to single-pass when bridge columns are absent.
    """
    has_bridge = all(c in df_test_pd.columns for c in _BRIDGE_COLS)
    has_ws = 'window_start' in df_test_pd.columns

    # Fast path: no bridge columns or no window_start -> single-pass prediction
    if not has_bridge or not has_ws:
        frame, _ = _prepare_autogluon_frame(df_test_pd, target_col, include_target=False)
        preds = predictor.predict(frame, model=model_name) if model_name else predictor.predict(frame)
        preds = np.maximum(np.array(preds, dtype=float), 0.0)
        if round_predictions:
            preds = np.round(preds)
        return preds

    ws = pd.to_numeric(df_test_pd['window_start'], errors='coerce')
    phase1_mask = ws <= 1
    phase2_mask = ws >= 2

    # If there are no far-horizon rows, a single-pass is fine
    if not phase2_mask.any():
        frame, _ = _prepare_autogluon_frame(df_test_pd, target_col, include_target=False)
        preds = predictor.predict(frame, model=model_name) if model_name else predictor.predict(frame)
        preds = np.maximum(np.array(preds, dtype=float), 0.0)
        if round_predictions:
            preds = np.round(preds)
        return preds

    n_p1 = int(phase1_mask.sum())
    n_p2 = int(phase2_mask.sum())
    logging.info(f"    Two-phase predict: {n_p1} Phase-1 rows (M0-M1), {n_p2} Phase-2 rows (M2-M5)")

    y_pred_all = np.zeros(len(df_test_pd), dtype=float)

    # ------------------------------------------------------------------
    # Phase 1: predict M0-M1 (bridge features already correct from build)
    # ------------------------------------------------------------------
    if phase1_mask.any():
        p1_df = df_test_pd.loc[phase1_mask].copy()
        frame1, _ = _prepare_autogluon_frame(p1_df, target_col, include_target=False)
        y_p1 = predictor.predict(frame1, model=model_name) if model_name else predictor.predict(frame1)
        y_p1 = np.maximum(np.array(y_p1, dtype=float), 0.0)
        if round_predictions:
            y_p1 = np.round(y_p1)
        y_pred_all[phase1_mask.values] = y_p1

    # ------------------------------------------------------------------
    # Phase 2: recompute ALL bridge features from M0-M1 predictions,
    #          then predict M2-M5
    # ------------------------------------------------------------------
    if phase2_mask.any() and phase1_mask.any():
        # Collect Phase 1 predictions keyed by group
        p1_tmp = df_test_pd.loc[phase1_mask].copy()
        p1_tmp['_p1_pred'] = y_pred_all[phase1_mask.values]

        group_cols = [c for c in group_by_cols if c in df_test_pd.columns]

        # Build per-group bridge feature dict from M0-M1 predictions
        bridge_map = {}  # group_key -> dict of bridge feature values
        if group_cols:
            for gk, gdf in p1_tmp.groupby(group_cols):
                ws_pred = dict(zip(gdf['window_start'].astype(int), gdf['_p1_pred']))
                m0 = ws_pred.get(0, 0.0)
                m1 = ws_pred.get(1, 0.0)
                # Use the first row's original features to reconstruct pre-cutoff sums
                first = gdf.iloc[0]
                bf = _compute_bridge_features_from_predictions(
                    sma_3m=float(first.get('sma_3m', 0.0) if 'sma_3m' in gdf.columns else 0.0),
                    sma_6m=float(first.get('sma_6m', 0.0) if 'sma_6m' in gdf.columns else 0.0),
                    demand_growth_3m=float(first.get('demand_growth_3m', 0.0) if 'demand_growth_3m' in gdf.columns else 0.0),
                    recent_vs_hist=float(first.get('recent_vs_historical_ratio', 1.0) if 'recent_vs_historical_ratio' in gdf.columns else 1.0),
                    trend_slope_6m=float(first.get('trend_slope_6m', 0.0) if 'trend_slope_6m' in gdf.columns else 0.0),
                    momentum_30_vs_90=float(first.get('momentum_30_vs_90', 1.0) if 'momentum_30_vs_90' in gdf.columns else 1.0),
                    m0=m0, m1=m1,
                )
                key = gk if isinstance(gk, tuple) else (gk,)
                bridge_map[key] = bf

        # Overwrite bridge columns in Phase 2 rows (vectorized via merge)
        p2_df = df_test_pd.loc[phase2_mask].copy()
        if group_cols and bridge_map:
            bridge_rows = [(k,) + tuple(bridge_map[k].get(c, 0.0) for c in _BRIDGE_COLS)
                          for k in bridge_map]
            bridge_df = pd.DataFrame(bridge_rows, columns=['_gkey'] + list(_BRIDGE_COLS))
            p2_df['_gkey'] = p2_df[group_cols].apply(tuple, axis=1)
            n_updated = p2_df['_gkey'].isin(bridge_df['_gkey']).sum()
            p2_df = p2_df.merge(bridge_df, on='_gkey', how='left', suffixes=('', '_y'))
            for c in _BRIDGE_COLS:
                if f'{c}_y' in p2_df.columns:
                    p2_df[c] = p2_df[f'{c}_y']
                    p2_df.drop(columns=[f'{c}_y'], inplace=True)
            p2_df.drop(columns=['_gkey'], inplace=True)
            logging.info(f"    Updated bridge features for {int(n_updated)}/{n_p2} Phase-2 rows")

        frame2, _ = _prepare_autogluon_frame(p2_df, target_col, include_target=False)
        y_p2 = predictor.predict(frame2, model=model_name) if model_name else predictor.predict(frame2)
        y_p2 = np.maximum(np.array(y_p2, dtype=float), 0.0)
        if round_predictions:
            y_p2 = np.round(y_p2)
        y_pred_all[phase2_mask.values] = y_p2

    elif phase2_mask.any():
        # No Phase 1 rows available to derive bridge features -- predict as-is
        p2_df = df_test_pd.loc[phase2_mask].copy()
        frame2, _ = _prepare_autogluon_frame(p2_df, target_col, include_target=False)
        y_p2 = predictor.predict(frame2, model=model_name) if model_name else predictor.predict(frame2)
        y_p2 = np.maximum(np.array(y_p2, dtype=float), 0.0)
        if round_predictions:
            y_p2 = np.round(y_p2)
        y_pred_all[phase2_mask.values] = y_p2

    return y_pred_all


def generate_predictions(model, model_name, df_val=None, y_pred=None, round_predictions=False,
                         autogluon_model_name=None, target_col='target_count', group_by_cols=None):
    """Generate AutoGluon predictions with two-phase bridge update for M3-M5."""
    logging.info(f"\n  Generating predictions for {model_name}...")
    try:
        if y_pred is None:
            if model is not None and df_val is not None:
                val_pd = df_val.to_pandas() if hasattr(df_val, "to_pandas") else df_val.copy()
                # Use two-phase prediction when group_by_cols are provided
                if group_by_cols is not None:
                    y_pred = _two_phase_predict(
                        model, val_pd, group_by_cols,
                        round_predictions=round_predictions,
                        model_name=autogluon_model_name,
                        target_col=target_col,
                    )
                    return y_pred
                # Fallback: single-pass prediction (no group info for bridge update)
                val_frame, _ = _prepare_autogluon_frame(val_pd, target_col, include_target=False)
                if autogluon_model_name:
                    y_pred = model.predict(val_frame, model=autogluon_model_name)
                else:
                    y_pred = model.predict(val_frame)
            else:
                logging.error(f"  No model or predictions provided for {model_name}")
                return None
        y_pred = np.maximum(y_pred, 0.0)
        if round_predictions:
            y_pred = np.round(y_pred)
        return y_pred
    except Exception as e:
        logging.error(f"  Prediction failed for {model_name}: {e}")
        return None


def calculate_aggregated_metrics(df_val, y_pred, group_by_cols, months_ahead, model_name):
    """
    Calculate metrics using data aggregated by group and window_start (Excel-aligned).
    """
    if df_val is None or y_pred is None:
        logging.error(f"  Missing data for aggregated metrics: {model_name}")
        return None

    try:
        test_pd = df_val.to_pandas() if hasattr(df_val, "to_pandas") else df_val.copy()

        if len(y_pred) != len(test_pd):
            logging.error(
                f"  Prediction length ({len(y_pred)}) doesn't match test data length ({len(test_pd)}) for {model_name}"
            )
            return None

        test_pd_with_pred = test_pd.copy()
        test_pd_with_pred['predicted'] = y_pred

        group_cols = [col for col in group_by_cols if col in test_pd_with_pred.columns]
        results_pd = build_window_start_results_df(test_pd_with_pred, group_cols, months_ahead)
        if results_pd.empty:
            logging.error(f"  No aggregated results available for {model_name}")
            return None

        all_actual = []
        all_predicted = []

        for ws in months_ahead:
            actual_col = f'M{int(ws)}_Actual'
            pred_col = f'M{int(ws)}_Predicted'

            if actual_col in results_pd.columns and pred_col in results_pd.columns:
                y_true = results_pd[actual_col].to_numpy()
                y_pred_ws = results_pd[pred_col].to_numpy()

                all_actual.extend(y_true.tolist())
                all_predicted.extend(y_pred_ws.tolist())

        if len(all_actual) == 0:
            logging.error(f"  No aggregated data found for metrics: {model_name}")
            return None

        all_actual = np.array(all_actual)
        all_predicted = np.array(all_predicted)

        mape_accuracy = calculate_mape_accuracy(all_actual, all_predicted)
        wmape_val = weighted_mape(all_actual, all_predicted)

        metrics = {
            'model_name': model_name,
            'mape': mape_accuracy['mape'],
            'accuracy': mape_accuracy['accuracy'],
            'wmape': wmape_val,
            'mae': float(np.mean(np.abs(all_actual - all_predicted))),
            'rmse': float(np.sqrt(np.mean((all_actual - all_predicted) ** 2))),
            'total_error': ((np.sum(all_predicted) - np.sum(all_actual)) / np.sum(all_actual) * 100)
            if np.sum(all_actual) > 0 else np.nan
        }

        logging.info(f"    Aggregated MAPE: {metrics['mape']:.2f}%")
        logging.info(f"    Aggregated Accuracy: {metrics['accuracy'] * 100:.2f}%")
        logging.info(f"    Aggregated wMAPE: {metrics['wmape']:.2f}%")
        logging.info(f"    Aggregated Total Volume Error: {metrics['total_error']:.2f}%")

        return metrics

    except Exception as e:
        logging.error(f"  Aggregated evaluation failed for {model_name}: {e}")
        return None


def build_window_start_results_df(test_pd_with_pred, group_cols, months_ahead):
    """
    Aggregate actuals and predictions per group and window_start into a wide pandas DataFrame.
    """
    if 'window_start' not in test_pd_with_pred.columns:
        raise ValueError("window_start column missing from test data")
    if 'target_count' not in test_pd_with_pred.columns:
        raise ValueError("target_count column missing from test data")
    if 'predicted' not in test_pd_with_pred.columns:
        raise ValueError("predicted column missing from test data")

    if not months_ahead:
        return pd.DataFrame(columns=group_cols)

    months_ahead_numeric = [float(ws) for ws in months_ahead]

    df = test_pd_with_pred.copy()
    df['window_start'] = pd.to_numeric(df['window_start'], errors='coerce')
    df = df.dropna(subset=['window_start'])
    df = df[df['window_start'].isin(months_ahead_numeric)]

    if df.empty:
        return pd.DataFrame(columns=group_cols)

    agg_df = df.groupby(group_cols + ['window_start'], dropna=False, as_index=False).agg(
        target_count=('target_count', 'sum'),
        predicted=('predicted', 'sum')
    )

    actual_pivot = agg_df.pivot_table(
        index=group_cols,
        columns='window_start',
        values='target_count',
        fill_value=0.0,
        aggfunc='sum'
    )
    pred_pivot = agg_df.pivot_table(
        index=group_cols,
        columns='window_start',
        values='predicted',
        fill_value=0.0,
        aggfunc='sum'
    )

    all_index = actual_pivot.index.union(pred_pivot.index)
    actual_pivot = actual_pivot.reindex(all_index, fill_value=0.0)
    pred_pivot = pred_pivot.reindex(all_index, fill_value=0.0)

    results_pd = pd.DataFrame(index=all_index)
    for ws in months_ahead_numeric:
        actual_col = f"M{int(ws)}_Actual"
        pred_col = f"M{int(ws)}_Predicted"
        results_pd[actual_col] = actual_pivot[ws] if ws in actual_pivot.columns else 0.0
        results_pd[pred_col] = pred_pivot[ws] if ws in pred_pivot.columns else 0.0

    if group_cols:
        results_pd = results_pd.reset_index()
        ordered_cols = group_cols + [col for col in results_pd.columns if col not in group_cols]
        results_pd = results_pd[ordered_cols]
    else:
        results_pd = results_pd.reset_index(drop=True)

    return results_pd


def write_excel_results(results_df, metrics, excel_path, model_name, months_ahead, group_cols=None, test_pd_with_pred=None, publishing=False):
    """Write per-model prediction and accuracy results to an Excel workbook.

    The workbook contains up to three sheets:
      All_Predictions  -- One row per group-by combination, wide-format columns
                          M{w}_Actual and M{w}_Predicted for each forecast window.
                          Rows are sorted by M0_Actual descending (highest-demand
                          groups first).
      Overall_Metrics  -- Single-row summary with MAPE, Accuracy, wMAPE, sMAPE
                          for all windows combined and per-window breakdowns.
      Group_Metrics    -- One row per (group x window) with the same accuracy
                          columns; also includes an ALL_WINDOWS rollup row per
                          group.  Only written when group_cols is provided and
                          test_pd_with_pred contains the raw per-row predictions.

    When excel_path is None the function skips the file write and instead
    returns the three DataFrames so the caller can accumulate them across
    multiple groups (used by run_per_group_training via return_excel_frames).

    Args:
        results_df:          Polars DataFrame from build_window_start_results_df.
        metrics:             Dict with keys 'overall' and 'window_metrics'.
        excel_path:          Output .xlsx path, or None to skip writing.
        model_name:          String label for logging.
        months_ahead:        List of integer window offsets (e.g. [0,1,2,3,4,5]).
        group_cols:          List of group-by column names (for Group_Metrics sheet).
        test_pd_with_pred:   pandas DataFrame with a 'predicted' column aligned
                             row-by-row to the test set (required for Group_Metrics).

    Returns:
        tuple: (results_pd, metrics_df, group_metrics_df)
               All three are None on write failure.
    """
    try:
        results_pd = results_df.to_pandas()

        # Sort All_Predictions descending by M0_Predicted in publishing mode (no actuals exist yet)
        # or by M0_Actual in normal mode (highest-demand groups first).
        if months_ahead:
            first_window = months_ahead[0]
            sort_suffix = f'M{first_window}_{"Predicted" if publishing else "Actual"}'
            sort_cols = [c for c in results_pd.columns if c.endswith(sort_suffix)]
            if sort_cols:
                sort_col = sort_cols[0]
                results_pd = results_pd.sort_values(by=sort_col, ascending=False, na_position='last')
                if excel_path:
                    logging.info(f"    Sorted All_Predictions by {sort_col} (descending)")

        # Build the Overall_Metrics rows: one summary row + one per forecast window
        metrics_data = []
        metrics_data.append({
            'Metric_Type': 'OVERALL_METRICS',
            'Window': 'ALL_WINDOWS',
            'Total_Actual': metrics['overall']['total_actual'],
            'Total_NonZero_Predicted': metrics['overall']['total_predicted'],
            'MAPE_%': metrics['overall']['mape'],
            'Accuracy': round(metrics['overall']['accuracy'] * 100, 2),
            'wMAPE_%': metrics['overall']['wmape'],
            'sMAPE_%': metrics['overall']['smape']
        })
        for window_key, window_data in metrics['window_metrics'].items():
            metrics_data.append({
                'Metric_Type': 'WINDOW_METRICS',
                'Window': window_key,
                'Total_Actual': window_data['total_actual'],
                'Total_NonZero_Predicted': window_data['total_predicted'],
                'MAPE_%': window_data['mape'],
                'Accuracy': round(window_data['accuracy'] * 100, 2),
                'wMAPE_%': window_data['wmape'],
                'sMAPE_%': window_data['smape']
            })
        metrics_df = pd.DataFrame(metrics_data)

        # Build Group_Metrics: aggregate per (group x window) to give per-group accuracy breakdown.
        # Each group's ALL_WINDOWS row is also appended for a roll-up view.
        group_metrics_rows = []
        group_metrics_df = None
        if group_cols and test_pd_with_pred is not None:
            _gdf = test_pd_with_pred.copy()
            # Coerce window_start to numeric and drop any rows where it is non-parseable
            _gdf['window_start'] = pd.to_numeric(_gdf['window_start'], errors='coerce')
            _gdf = _gdf.dropna(subset=['window_start'])
            # Keep only windows that were requested via months_ahead
            _gdf = _gdf[_gdf['window_start'].isin([float(ws) for ws in months_ahead])]
            if not _gdf.empty:
                agg = _gdf.groupby(group_cols + ['window_start'], dropna=False).agg(
                    actual=('target_count', 'sum'),
                    predicted=('predicted', 'sum')
                ).reset_index()
                for group_values, sub_df in agg.groupby(group_cols, dropna=False):
                    if not isinstance(group_values, tuple):
                        group_values = (group_values,)
                    group_dict = {col: val for col, val in zip(group_cols, group_values)}
                    for ws in months_ahead:
                        ws_float = float(ws)
                        ws_df = sub_df[sub_df['window_start'] == ws_float]
                        if ws_df.empty:
                            continue
                        y_true_ws = ws_df['actual'].to_numpy()
                        y_pred_ws = ws_df['predicted'].to_numpy()
                        wmape_ws = weighted_mape(y_true_ws, y_pred_ws)
                        eps = 1e-8
                        smape_ws = float(np.mean(200.0 * np.abs(y_pred_ws - y_true_ws) / (np.abs(y_pred_ws) + np.abs(y_true_ws) + eps)))
                        mape_acc_ws = calculate_mape_accuracy(y_true_ws, y_pred_ws)
                        group_metrics_rows.append({
                            **group_dict,
                            'Metric_Type': 'GROUP_WINDOW_METRICS',
                            'Window': f'M{int(ws)}',
                            'Total_Actual': float(y_true_ws.sum()),
                            'Total_NonZero_Predicted': float(y_pred_ws[y_true_ws > 0].sum()),
                            'MAPE_%': mape_acc_ws['mape'],
                            'Accuracy': round(mape_acc_ws['accuracy'] * 100, 2),
                            'wMAPE_%': wmape_ws,
                            'sMAPE_%': smape_ws,
                        })
                    y_true_all = sub_df['actual'].to_numpy()
                    y_pred_all = sub_df['predicted'].to_numpy()
                    wmape_all = weighted_mape(y_true_all, y_pred_all)
                    smape_all = float(np.mean(200.0 * np.abs(y_pred_all - y_true_all) / (np.abs(y_pred_all) + np.abs(y_true_all) + eps)))
                    mape_acc_all = calculate_mape_accuracy(y_true_all, y_pred_all)
                    group_metrics_rows.append({
                        **group_dict,
                        'Metric_Type': 'GROUP_OVERALL_METRICS',
                        'Window': 'ALL_WINDOWS',
                        'Total_Actual': float(y_true_all.sum()),
                        'Total_NonZero_Predicted': float(y_pred_all[y_true_all > 0].sum()),
                        'MAPE_%': mape_acc_all['mape'],
                        'Accuracy': round(mape_acc_all['accuracy'] * 100, 2),
                        'wMAPE_%': wmape_all,
                        'sMAPE_%': smape_all,
                    })
        if group_metrics_rows:
            group_metrics_df = pd.DataFrame(group_metrics_rows)

        if not excel_path:
            return results_pd, metrics_df, group_metrics_df

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            _round_df_for_excel(results_pd).to_excel(writer, sheet_name='All_Predictions', index=False)
            if not publishing:
                _round_df_for_excel(metrics_df).to_excel(writer, sheet_name='Overall_Metrics', index=False)
                if group_metrics_df is not None:
                    _round_df_for_excel(group_metrics_df).to_excel(writer, sheet_name='Group_Metrics', index=False)

        logging.info(f"    Excel saved: {excel_path}")
        return results_pd, metrics_df, group_metrics_df
    except Exception as e:
        logging.warning(f"    Failed to create Excel for {model_name}: {e}")
        logging.debug(traceback.format_exc())
        return None, None, None


def main(training_dataset_file=None, test_dataset_file=None, output_dir='results',
         autogluon_time_limit=None, group_by=None,
         round_predictions=True, months_ahead=[0, 1], reuse_autogluon=False, autogluon_model_name=None,
         autogluon_path=None, group_label=None, group_token=None, combined_frames=None,
         return_excel_frames=False, run_shap=False, shap_plot_dir=None, shap_model_label='global_model',
         publishing=False):
    """Core AutoGluon training + evaluation entry point (single dataset pair).

    This function is called once per run in global mode and once per group in
    per-group mode (orchestrated by run_per_group_training).

    Execution phases:
      Step 1 -- Load training and test parquet files.
      Step 2 -- Run test-data diagnostics (window/cutoff/null checks).
      Step 3 -- Resolve and validate group_by columns; align feature space between
                train and test by keeping only common columns.
      Step 4 -- Split training data 90/10 by cutoff_date for AutoGluon validation.
      Step 5 -- Validate months_ahead against windows present in test data.
      Step 6 -- Train or reuse an AutoGluon TabularPredictor.
      Step 7 -- Evaluate all AutoGluon model variants; select the best by accuracy.
      Step 8 -- Generate final predictions using the best (or requested) model.
      Step 9 -- Build Excel outputs (All_Predictions, Overall_Metrics, Group_Metrics).
      Step 10 -- Summarise and persist results to JSON + CSV.
      Step 11 -- (Optional) Run SHAP analysis on training features.

    Args:
        training_dataset_file: Path to training parquet.
        test_dataset_file:     Path to test parquet.
        output_dir:            Directory for all outputs.
        autogluon_time_limit:  Wall-clock cap for AutoGluon.fit() in seconds.
        group_by:              List of column names to group by for metrics.
        round_predictions:     If True, round predictions to nearest integer.
        months_ahead:          List of window offsets (M0, M1, ...) to evaluate.
        reuse_autogluon:       If True, load an existing predictor instead of training.
        autogluon_model_name:  Specific internal model name to use for predictions.
        autogluon_path:        Explicit path to AutoGluon predictor directory.
        group_label:           Human-readable group label for combined Excel rows.
        group_token:           File-name-safe group token for Excel file names.
        combined_frames:       Dict accumulating frames across groups (per-group mode).
        return_excel_frames:   If True, return DataFrames instead of writing Excel files.
        run_shap:              If True, compute and save SHAP plots.
        shap_plot_dir:         Directory for SHAP PNG outputs.
        shap_model_label:      Label used in SHAP plot file names.

    Returns:
        tuple: (results, predictions, excel_frames_or_None)
    """

    os.makedirs(output_dir, exist_ok=True)
    models_dir = os.path.join(output_dir, 'models')
    os.makedirs(models_dir, exist_ok=True)
    # Unique 8-char run ID used in output file names to avoid collisions
    run_id = uuid.uuid4().hex[:8]

    logging.info("="*80)
    logging.info("MODEL COMPARISON FOR  COUNT FORECASTING (AUTOGLUON)")
    logging.info("="*80)

    # Step 1a: Load training parquet (all engineered features + target_count)
    logging.info(f"\nLoading training dataset from {training_dataset_file}...")
    df_train_full = pl.read_parquet(training_dataset_file)
    logging.info(f"Loaded {len(df_train_full):,} training samples")

    # Step 1b: Load test parquet and run diagnostics when available
    if test_dataset_file and os.path.exists(test_dataset_file):
        logging.info(f"Loading test dataset from {test_dataset_file}...")
        df_test = pl.read_parquet(test_dataset_file)
        logging.info(f"Loaded {len(df_test):,} test samples")
        
        # ====================================================================
        # DIAGNOSTIC: Analyze test data structure
        # ====================================================================
        logging.info("\n" + "="*80)
        logging.info("TEST DATA DIAGNOSTIC")
        logging.info("="*80)
        
        # Check if target_month column exists
        if 'target_month' in df_test.columns:
            logging.info("✓ Found 'target_month' column in test data")
            
            # Count unique target months
            unique_months = df_test.select(['target_month']).unique().sort('target_month')
            logging.info(f"  Unique target months: {unique_months['target_month'].to_list()}")
            
            # For each target month, show how many rows
            month_counts = df_test.group_by('target_month').agg([
                pl.len().alias('row_count'),
                pl.col('target_count').sum().alias('total_qty')
            ]).sort('target_month')
            
            logging.info("\n  Target month breakdown:")
            for row in month_counts.iter_rows(named=True):
                logging.info(f"    Month {row['target_month']:2d}: {row['row_count']:4d} rows, {row['total_qty']:6.0f} total qty")
        else:
            logging.warning("✗ No 'target_month' column found in test data")
        
        # Check window_start distribution
        if 'window_start' in df_test.columns:
            window_counts = df_test.group_by('window_start').agg([
                pl.len().alias('row_count'),
                pl.col('target_count').sum().alias('total_qty')
            ]).sort('window_start')
            
            logging.info("\n  Window_start (months ahead) breakdown:")
            for row in window_counts.iter_rows(named=True):
                logging.info(f"    M{int(row['window_start'])}: {row['row_count']:4d} rows, {row['total_qty']:6.0f} total qty")
        
        # Check cutoff_date distribution
        if 'cutoff_date' in df_test.columns:
            cutoff_counts = df_test.group_by('cutoff_date').agg([
                pl.len().alias('row_count'),
                pl.col('target_count').sum().alias('total_qty')
            ]).sort('cutoff_date')
            
            logging.info("\n  Cutoff date breakdown:")
            for row in cutoff_counts.iter_rows(named=True):
                logging.info(f"    Cutoff {row['cutoff_date']}: {row['row_count']:4d} rows, {row['total_qty']:6.0f} total qty")
        
        # Cross-tabulation: cutoff_date × window_start → target_month
        if 'cutoff_date' in df_test.columns and 'window_start' in df_test.columns and 'target_month' in df_test.columns:
            logging.info("\n  Cross-tab: (cutoff_date, window_start) → target_month")
            
            cross_tab = df_test.group_by(['cutoff_date', 'window_start', 'target_month']).agg([
                pl.len().alias('row_count'),
                pl.col('target_count').sum().alias('total_qty')
            ]).sort(['cutoff_date', 'window_start'])
            
            # Show first 20 entries
            logging.info("    First 20 entries:")
            for i, row in enumerate(cross_tab.head(20).iter_rows(named=True)):
                logging.info(f"      Cutoff={row['cutoff_date']}, M{int(row['window_start'])} "
                           f"→ Month {row['target_month']:2d}: "
                           f"{row['row_count']:3d} rows, {row['total_qty']:6.0f} qty")
        
        # Check for null values in key columns
        null_counts = {}
        for col in ['cutoff_date', 'window_start', 'target_month', 'target_count']:
            if col in df_test.columns:
                null_count = df_test[col].null_count()
                if null_count > 0:
                    null_counts[col] = null_count
        
        if null_counts:
            logging.warning("\n  ⚠️  Null values found in key columns:")
            for col, count in null_counts.items():
                pct = 100 * count / len(df_test)
                logging.warning(f"    {col}: {count:,} nulls ({pct:.2f}%)")
        else:
            logging.info("\n  ✓ No null values in key columns")
        
        logging.info("="*80 + "\n")
        
        df_train = df_train_full  # Use full training data
        use_separate_test = True

    else:
        raise ValueError("No test dataset provided")
    
    ##### Resolve group_by columns and align train/test feature space
    available_cols_train = set(df_train.columns)
    available_cols_test = set(df_test.columns) if test_dataset_file else available_cols_train

    # Find common columns between train and test (excluding target)
    exclude_cols = ['cutoff_date', 'target_count', 'window_start']
    common_feature_cols = (available_cols_train & available_cols_test) - set(exclude_cols)

    # Set default group_by_cols if not provided (same as build_training_groups.py)
    if group_by is None:
        group_by_cols = ['Country', 'SO GRADE', 'Skill Cluster']
    else:
        # group_by is a list of column names
        group_by_cols = group_by if isinstance(group_by, list) else [group_by]
    
    logging.info(f"  Requested group_by columns: {group_by_cols}")

    # Filter group_by_cols to only include columns that exist in the data
    original_group_by_cols = group_by_cols.copy()
    group_by_cols = [col for col in group_by_cols if col in common_feature_cols]
    
    if len(group_by_cols) == 0:
        # Fallback: use first available column
        candidate_cols = list(common_feature_cols)
        if len(candidate_cols) > 0:
            group_by_cols = [candidate_cols[0]]
            logging.warning(f"  None of the requested group columns found in data, using fallback: {group_by_cols}")
    elif len(group_by_cols) < len(original_group_by_cols):
        missing_cols = [col for col in original_group_by_cols if col not in group_by_cols]
        logging.warning(f"  Some requested group columns not found in data (missing: {missing_cols})")
        logging.info(f"  Using available columns: {group_by_cols}")

    logging.info(f"  Final group_by columns: {group_by_cols}")
    logging.info(f"  Common feature columns: {len(common_feature_cols)} total")

    # Filter datasets to only include common features
    keep_cols = list(common_feature_cols) + ['cutoff_date', 'target_count', 'window_start']
    df_train_filtered = df_train.select([col for col in keep_cols if col in df_train.columns])
    df_test_filtered = df_test.select([col for col in keep_cols if col in df_test.columns])

    # Use the original test dataframe (with all grouping columns) for evaluation/Excel,
    # while df_test_filtered is used only for model feature preparation.
    df_test_eval = df_test

    logging.info(f"  Train dataset filtered to {len(df_train_filtered.columns)} columns")
    logging.info(f"  Test dataset filtered to {len(df_test_filtered.columns)} columns")

    feature_cols = [c for c in df_train_filtered.columns if c not in ('cutoff_date', 'target_count')]
    logging.info(f"\nTraining: {len(df_train_filtered):,} samples")
    logging.info(f"Test: {len(df_test_filtered):,} samples")

    # Use the last 10% of training data by cutoff_date for validation.
    df_train_fit, df_val_fit = _split_train_validation(
        df_train_filtered,
        validation_fraction=0.1,
        date_col='cutoff_date'
    )
    logging.info(f"  AutoGluon train split: {len(df_train_fit):,} rows")
    logging.info(f"  AutoGluon validation split: {len(df_val_fit):,} rows")

    results = {}
    predictions = {}

    ##### Use months_ahead directly from args (don't override)
    # Validate that requested months_ahead exist in test data
    if 'window_start' in df_test.columns:
        test_months = set(df_test['window_start'].unique().to_list())
        requested_months = set(months_ahead)
        missing_months = requested_months - test_months
        if missing_months:
            logging.warning(f"  Requested months_ahead {sorted(list(missing_months))} not found in test data")
        available_months = sorted(list(requested_months & test_months))
        if available_months:
            months_ahead = available_months
            logging.info(f"  Using months_ahead from args (filtered to available in test data): {months_ahead}")
        else:
            logging.warning(f"  None of requested months_ahead found in test data, using all test months")
            months_ahead = sorted(list(test_months))
    else:
        logging.warning(f"  No window_start column in test data, using months_ahead from args: {months_ahead}")
    
    logging.info(f"  Test period covers {len(months_ahead)} months: {months_ahead}")

    ##### 1) AutoGluon: train or reuse existing
    ag_model = None
    autogluon_path = _resolve_autogluon_path(autogluon_path, models_dir, reuse_autogluon, run_id)
    if autogluon_path:
        autogluon_path = autogluon_path.rstrip(os.sep) + os.sep
    elif reuse_autogluon:
        logging.error(
            "  Reuse AutoGluon requested but no predictor path found. "
            "Provide --autogluon-path pointing to a valid predictor directory."
        )
    logging.info("\n" + "="*80)
    if reuse_autogluon:
        logging.info("1. AUTOGLUON (REUSE EXISTING PREDICTOR)")
        logging.info("="*80)
        if autogluon_path:
            try:
                ag_model = TabularPredictor.load(autogluon_path)
                logging.info(f"  Loaded existing AutoGluon predictor from {autogluon_path}")
            except Exception as e:
                logging.error(f"  Failed to load existing AutoGluon predictor from {autogluon_path}: {e}")
                ag_model = None
    else:
        logging.info("1. AUTOGLUON (AUTOML TRAINING)")
        logging.info("="*80)
        ag_model = train_autogluon(
            df_train_fit, df_val_fit,
            time_limit=autogluon_time_limit,
            group_by_cols=group_by_cols,
            predictor_path=autogluon_path
        )

    ag_variant_results = []
    best_variant_name = None
    if ag_model:
        logging.info("\n  Evaluating all AutoGluon model variants with aggregated metrics...")
        ag_variant_results = _evaluate_autogluon_variants(
            ag_model,
            df_test_filtered,
            df_test_eval,
            group_by_cols,
            months_ahead,
            round_predictions,
            target_col='target_count'
        )
        if ag_variant_results:
            variants_df = pd.DataFrame(ag_variant_results)
            variants_df = variants_df[['model_name', 'accuracy', 'mape', 'wmape', 'mae', 'rmse', 'total_error']]
            variants_df = variants_df.sort_values('accuracy', ascending=False, na_position='last')
            best_variant_name = variants_df.iloc[0]['model_name']
            logging.info("\n  AutoGluon variants (aggregated metrics):")
            logging.info("\n" + variants_df.to_string(index=False))

            variants_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            variants_file = os.path.join(output_dir, f'autogluon_variants_{variants_timestamp}.csv')
            variants_df.to_csv(variants_file, index=False)
            logging.info(f"  AutoGluon variants table saved: {variants_file}")
        else:
            logging.warning("  No AutoGluon variants produced aggregated metrics")

        primary_model_name = autogluon_model_name or best_variant_name
        if autogluon_model_name and best_variant_name and autogluon_model_name != best_variant_name:
            logging.info(
                f"  Best AutoGluon variant by aggregated accuracy: {best_variant_name}. "
                f"Using requested model: {autogluon_model_name}"
            )

        ag_pred = generate_predictions(
            ag_model,
            'AutoGluon',
            df_val=df_test_filtered,
            round_predictions=round_predictions,
            autogluon_model_name=primary_model_name,
            group_by_cols=group_by_cols,
        )
        if ag_pred is not None:
            ag_display_name = f"AutoGluon::{primary_model_name}" if primary_model_name else 'AutoGluon'
            ag_metrics = calculate_aggregated_metrics(
                df_test_eval,
                ag_pred,
                group_by_cols,
                months_ahead,
                model_name=ag_display_name
            )
            if ag_metrics:
                results[ag_metrics['model_name']] = ag_metrics
                predictions[ag_metrics['model_name']] = ag_pred

    ##### 2) Create Excel outputs for each model
    logging.info("\n" + "="*80)
    logging.info("2. CREATING EXCEL OUTPUTS")
    logging.info("="*80)
    
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    excel_prefix = build_excel_prefix(group_by_cols, months_ahead)
    
    # Convert original test data (with all grouping columns) to pandas for easier manipulation
    test_pd_df = df_test_eval.to_pandas()

    collected_excel_frames = None
    # Build results DataFrame for each model
    for model_name, y_pred in predictions.items():
        if model_name not in results:
            continue
            
        logging.info(f"\n  Creating Excel output for {model_name}...")
        
        # CRITICAL: Predictions are aligned row-by-row with test data
        # y_pred[i] corresponds to test_pd_df.iloc[i]
        # Each row in test_pd_df has: group_cols, window_start, target_count
        
        if len(y_pred) != len(test_pd_df):
            logging.error(f"    CRITICAL: Prediction length ({len(y_pred)}) doesn't match test data length ({len(test_pd_df)}) for {model_name}")
            continue
        
        # Add predictions directly to test data by row index (they're already aligned!)
        test_pd_with_pred = test_pd_df.copy()
        test_pd_with_pred['predicted'] = y_pred
        
        # Group by group_by_cols and aggregate by window_start
        group_cols = [col for col in group_by_cols if col in test_pd_with_pred.columns]

        if 'window_start' not in test_pd_with_pred.columns:
            logging.error("    window_start column not found, skipping Excel export")
            continue
        if 'target_count' not in test_pd_with_pred.columns:
            logging.error("    target_count column not found, skipping Excel export")
            continue

        try:
            results_pd = build_window_start_results_df(test_pd_with_pred, group_cols, months_ahead)
        except ValueError as e:
            logging.error(f"    {e}, skipping Excel export")
            continue

        if results_pd.empty:
            logging.info(f"    No results to export for {model_name}")
            continue

        results_df = pl.from_pandas(results_pd)

        if publishing:
            # Publishing mode: no real actuals exist yet — skip all accuracy metrics
            metrics = {
                'window_metrics': {},
                'country_metrics': {},
                'so_grade_metrics': {},
                'overall': {'total_actual': 0, 'total_predicted': 0, 'mape': 0, 'accuracy': 0, 'wmape': 0, 'smape': 0}
            }
        else:
            # Calculate metrics per window
            window_metrics = {}
            all_actual = []
            all_predicted = []

            for ws in months_ahead:
                actual_col = f'M{int(ws)}_Actual'
                pred_col = f'M{int(ws)}_Predicted'

                if actual_col in results_df.columns and pred_col in results_df.columns:
                    y_true = results_df[actual_col].to_numpy()
                    y_pred_ws = results_df[pred_col].to_numpy()

                    all_actual.extend(y_true.tolist())
                    all_predicted.extend(y_pred_ws.tolist())

                    wmape_ws = weighted_mape(y_true, y_pred_ws)
                    eps = 1e-8
                    smape_ws = float(np.mean(200.0 * np.abs(y_pred_ws - y_true) / (np.abs(y_pred_ws) + np.abs(y_true) + eps)))
                    mape_acc_ws = calculate_mape_accuracy(y_true, y_pred_ws)

                    window_metrics[f'Qty_M{int(ws)}'] = {
                        'total_actual': float(y_true.sum()),
                        'total_predicted': float(y_pred_ws[y_true > 0].sum()),
                        'mape': mape_acc_ws['mape'],
                        'accuracy': mape_acc_ws['accuracy'],
                        'wmape': wmape_ws,
                        'smape': smape_ws
                    }

            # Overall metrics
            all_actual = np.array(all_actual)
            all_predicted = np.array(all_predicted)
            overall_wmape = weighted_mape(all_actual, all_predicted)
            eps = 1e-8
            overall_smape = float(np.mean(200.0 * np.abs(all_predicted - all_actual) / (np.abs(all_predicted) + np.abs(all_actual) + eps)))
            overall_mape_acc = calculate_mape_accuracy(all_actual, all_predicted)

            metrics = {
                'window_metrics': window_metrics,
                'country_metrics': {},
                'so_grade_metrics': {},
                'overall': {
                    'total_actual': float(all_actual.sum()),
                    'total_predicted': float(all_predicted[all_actual > 0].sum()),
                    'mape': overall_mape_acc['mape'],
                    'accuracy': overall_mape_acc['accuracy'],
                    'wmape': overall_wmape,
                    'smape': overall_smape
                }
            }

        model_token = sanitize_filename_token(model_name)
        if return_excel_frames:
            excel_path = None
        elif group_token:
            excel_path = os.path.join(output_dir, f'{excel_prefix}_{group_token}_{model_token}_{timestamp}.xlsx')
        else:
            excel_path = os.path.join(output_dir, f'{excel_prefix}_{model_token}_{timestamp}.xlsx')

        results_pd, metrics_df, group_metrics_df = write_excel_results(
            results_df=results_df,
            metrics=metrics,
            excel_path=excel_path,
            model_name=model_name,
            months_ahead=months_ahead,
            group_cols=group_cols,
            test_pd_with_pred=test_pd_with_pred,
            publishing=publishing,
        )
        if return_excel_frames and results_pd is not None:
            collected_excel_frames = {
                'All_Predictions': results_pd,
                'Overall_Metrics': metrics_df,
                'Group_Metrics': group_metrics_df,
            }
        if combined_frames is not None and results_pd is not None and not return_excel_frames:
            resolved_group_label = group_label or group_token or 'group'
            _append_combined_frames(
                combined_frames=combined_frames,
                results_pd=results_pd,
                metrics_df=metrics_df,
                group_metrics_df=group_metrics_df,
                group_label=resolved_group_label,
                model_name=model_name
            )
    
    ##### Summarize results and persist outputs
    if publishing:
        logging.info("\n[Publishing mode] Skipping accuracy comparison summary (no real actuals available).")
        return results, predictions, (collected_excel_frames if return_excel_frames else None)

    logging.info("\n" + "="*80)
    logging.info("3. COMPARISON SUMMARY")
    logging.info("="*80)

    if not results:
        logging.error("No aggregated metrics available (AutoGluon training may have failed).")
        print("\nmodel_name  accuracy  mape  wmape  mae  rmse  total_error")
        print("(no results - AutoGluon training failed or produced no predictions)")
        return results, predictions, (collected_excel_frames if return_excel_frames else None)

    # Sort by accuracy
    sorted_results = sorted(results.items(), key=lambda x: x[1]['accuracy'], reverse=True)
    summary_df = pd.DataFrame([r[1] for r in sorted_results])
    summary_df = summary_df[['model_name', 'accuracy', 'mape', 'wmape', 'mae', 'rmse', 'total_error']]
    display_df = summary_df.copy()
    display_df['accuracy'] = (display_df['accuracy'] * 100).round(2).astype(str) + '%'
    print("\n" + display_df.to_string(index=False))
    
    # Save results
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    results_file = os.path.join(output_dir, f'{timestamp}.json')
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    summary_file = os.path.join(output_dir, f'summary_{timestamp}.csv')
    summary_df.to_csv(summary_file, index=False)
    
    logging.info(f"\nResults saved to:")
    logging.info(f"  {results_file}")
    logging.info(f"  {summary_file}")
    
    # Best model
    best_model_name = sorted_results[0][0]
    best_accuracy = sorted_results[0][1]['accuracy']
    
    # Enhanced logging for AutoGluon winner
    if best_model_name.startswith('AutoGluon'):
        try:
            # Get AutoGluon model details if available
            if 'ag_model' in locals() and ag_model is not None:
                model_info = ag_model.info()
                best_ag_model = (
                    model_info.get('model_best')
                    or model_info.get('best_model')
                    or None
                )
                if best_ag_model:
                    logging.info(f"\nBEST AUTOGLUON MODEL: {best_ag_model}")
                    if 'WeightedEnsemble' in best_ag_model:
                        logging.info("   Model Type: ensemble (weighted ensemble of base models)")
                    else:
                        model_type = best_ag_model.split('_')[0] if '_' in best_ag_model else 'single'
                        logging.info(f"   Model Type: {model_type}")
        except Exception as e:
            logging.debug(f"Could not get AutoGluon model details: {e}")
    
    logging.info(f"\nBest Model: {best_model_name} (Accuracy: {best_accuracy * 100:.2f}%)")

    # Write run metadata to results
    best_autogluon_model = None
    best_model_path = None
    if best_model_name.startswith('AutoGluon') and ag_model is not None:
        try:
            model_info = ag_model.info()
            best_autogluon_model = (
                model_info.get('model_best')
                or model_info.get('best_model')
                or None
            )
            if best_autogluon_model:
                best_model_path = os.path.join(autogluon_path, 'models', best_autogluon_model)
        except Exception:
            pass

    metadata_lines = [
        f"run_id={run_id}",
        f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"output_dir={output_dir}",
        f"autogluon_temp_path={autogluon_path.rstrip(os.sep)}",
        f"best_model_name={best_model_name}",
        f"best_accuracy={best_accuracy * 100:.2f}%",
    ]
    if best_autogluon_model:
        metadata_lines.append(f"best_autogluon_model={best_autogluon_model}")
    if best_model_path and os.path.exists(best_model_path):
        metadata_lines.append(f"best_model_path={best_model_path}")
    metadata_lines.extend([
        f"results_file={results_file}",
        f"summary_file={summary_file}",
    ])

    metadata_file = os.path.join(output_dir, f'run_metadata_{run_id}.txt')
    with open(metadata_file, 'w') as f:
        f.write('\n'.join(metadata_lines))
    logging.info(f"  Run metadata: {metadata_file}")

    if run_shap and ag_model is not None and df_train_fit is not None:
        train_pd = df_train_fit.to_pandas() if hasattr(df_train_fit, 'to_pandas') else df_train_fit
        feature_frame, _ = _prepare_autogluon_frame(train_pd, 'target_count', include_target=False)
        _run_model_shap(ag_model, feature_frame, shap_plot_dir or output_dir, model_label=shap_model_label)

    if return_excel_frames and collected_excel_frames is not None:
        collected_excel_frames['Best_Model_Name'] = best_model_name
    return results, predictions, (collected_excel_frames if return_excel_frames else None)


def run_per_group_training(group_data_dir: str, output_dir: str, group_by: list,
                           autogluon_time_limit, round_predictions: bool,
                           months_ahead: list, reuse_autogluon: bool,
                           autogluon_model_name: str, autogluon_path: str,
                           global_train_all: bool = False, run_shap: bool = False,
                           publishing: bool = False) -> None:
    """Train AutoGluon models per group using pre-split datasets.

    The global model runs FIRST (on all data), then individual group models.
    When global_train_all is True, the global model uses the unfiltered
    'training_dataset_global.parquet' / 'test_dataset_global.parquet' files
    instead of the demand-filtered versions.
    """
    if reuse_autogluon or autogluon_path:
        logging.warning("Per-group training ignores --reuse-autogluon/--autogluon-path to avoid cross-group reuse.")
        reuse_autogluon = False
        autogluon_path = None

    manifest = _load_group_manifest(group_data_dir)
    if not manifest:
        logging.error(f"Group manifest not found in {group_data_dir}. Run build_training_groups.py with --individual-groups.")
        return

    manifest_group_by = manifest.get('group_by_cols') or []
    group_by_cols = manifest_group_by if manifest_group_by else (group_by or [])
    groups = manifest.get('groups', [])

    if not groups:
        logging.warning("No groups found in manifest; nothing to train.")
        return

    os.makedirs(output_dir, exist_ok=True)

    logging.info("=" * 80)
    logging.info("PER-GROUP AUTOGLUON TRAINING")
    logging.info("=" * 80)
    logging.info(f"Group data dir: {group_data_dir}")
    logging.info(f"Output dir: {output_dir}")
    logging.info(f"Groups: {len(groups)}")
    logging.info(f"Global train all: {global_train_all}")

    # ======================================================================
    # STEP 0: Run global model FIRST (before individual groups)
    # ======================================================================
    global_frames = None

    # Choose global parquet paths: use unfiltered versions when --global-train-all is set
    if global_train_all:
        global_train = os.path.join(group_data_dir, 'training_dataset_global.parquet')
        global_test = os.path.join(group_data_dir, 'test_dataset_global.parquet')
        # Fallback to regular parquets if global versions don't exist
        if not os.path.exists(global_train) or not os.path.exists(global_test):
            logging.warning("  Global (unfiltered) parquets not found; falling back to regular parquets.")
            global_train = os.path.join(group_data_dir, 'training_dataset.parquet')
            global_test = os.path.join(group_data_dir, 'test_dataset.parquet')
    else:
        global_train = os.path.join(group_data_dir, 'training_dataset.parquet')
        global_test = os.path.join(group_data_dir, 'test_dataset.parquet')

    if os.path.exists(global_train) and os.path.exists(global_test):
        logging.info("\n" + "=" * 80)
        data_label = "ALL data, no demand filter" if global_train_all else "demand-filtered data"
        logging.info(f"GLOBAL MODEL (trained FIRST on {data_label})")
        logging.info("=" * 80)
        logging.info(f"  Global train: {global_train}")
        logging.info(f"  Global test:  {global_test}")
        global_output_dir = os.path.join(output_dir, '_global_model')
        _, _, global_frames = main(
            training_dataset_file=global_train,
            test_dataset_file=global_test,
            output_dir=global_output_dir,
            group_by=group_by_cols,
            autogluon_time_limit=autogluon_time_limit,
            round_predictions=round_predictions,
            months_ahead=months_ahead,
            reuse_autogluon=False,
            autogluon_model_name=autogluon_model_name,
            autogluon_path=None,
            group_label=None,
            group_token=None,
            combined_frames=None,
            return_excel_frames=True,
            run_shap=run_shap,
            shap_plot_dir=output_dir,
            shap_model_label='global_model',
            publishing=publishing,
        )
        if global_frames and any(global_frames.get(k) is not None and not (hasattr(global_frames[k], 'empty') and global_frames[k].empty) for k in ('All_Predictions', 'Overall_Metrics')):
            excel_prefix = build_excel_prefix(group_by_cols, months_ahead)
            timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            global_excel_path = os.path.join(global_output_dir, f'{excel_prefix}_global_best_model_{timestamp}.xlsx')
            try:
                with pd.ExcelWriter(global_excel_path, engine='openpyxl') as writer:
                    if global_frames.get('All_Predictions') is not None and not global_frames['All_Predictions'].empty:
                        _round_df_for_excel(global_frames['All_Predictions']).to_excel(writer, sheet_name='All_Predictions', index=False)
                    if not publishing:
                        if global_frames.get('Overall_Metrics') is not None and not global_frames['Overall_Metrics'].empty:
                            _round_df_for_excel(global_frames['Overall_Metrics']).to_excel(writer, sheet_name='Overall_Metrics', index=False)
                        if global_frames.get('Group_Metrics') is not None and not global_frames['Group_Metrics'].empty:
                            _round_df_for_excel(global_frames['Group_Metrics']).to_excel(writer, sheet_name='Group_Metrics', index=False)
                logging.info(f"  Global model detailed Excel: {global_excel_path}")
            except Exception as e:
                logging.warning(f"  Failed to write global model detailed Excel: {e}")
    else:
        logging.info("  Global train/test not found; skipping global model.")

    # ======================================================================
    # STEP 1: Train individual group models
    # ======================================================================
    combined_frames = {
        'All_Predictions': [],
        'Overall_Metrics': [],
        'Group_Metrics': []
    }

    ordered_groups = []
    for group_entry in groups:
        test_file = group_entry.get('test_file')
        if not test_file:
            ordered_groups.append((group_entry, None))
            continue
        test_path = os.path.join(group_data_dir, test_file)
        if not os.path.exists(test_path):
            ordered_groups.append((group_entry, None))
            continue
        m0_total = _compute_group_m0_total(test_path)
        ordered_groups.append((group_entry, m0_total))

    # Sort groups descending by their M0 test-set total so the highest-demand groups
    # are trained first.  None-total groups (missing test file) sort to the end.
    ordered_groups.sort(key=lambda item: (item[1] is None, -(item[1] or 0.0)))
    logging.info("\n" + "=" * 80)
    logging.info("INDIVIDUAL GROUP MODELS")
    logging.info("=" * 80)
    logging.info("  Ordering groups by M0 total target_count (descending)")

    # group_order preserves the sorted order so combined Excel can display groups
    # in the same demand-descending sequence.
    group_order = []
    for group_entry, m0_total in tqdm(ordered_groups, desc="Training per-group models", unit="group"):
        group_id = group_entry.get('group_id') or sanitize_filename_token(_format_group_label(group_entry.get('group_values', {})))
        group_values = group_entry.get('group_values', {})
        # is_remainder: True for the catch-all group that absorbs small demand groups
        is_remainder = group_entry.get('is_remainder', False)

        if is_remainder:
            # REMAINDER groups are small clusters combined into one model; label shows count
            group_label = f"REMAINDER ({group_values.get('_num_groups', '?')} groups combined)"
        else:
            group_label = _format_group_label(group_values)
        group_order.append(group_label)
        # Build a file-name-safe token from the group values (used in Excel names)
        group_token = _build_group_token(group_values, group_by_cols)

        train_file = group_entry.get('train_file')
        test_file = group_entry.get('test_file')
        if not train_file or not test_file:
            logging.warning(f"Missing train/test file names for {group_label}; skipping.")
            continue

        train_path = os.path.join(group_data_dir, train_file)
        test_path = os.path.join(group_data_dir, test_file)
        if not os.path.exists(train_path) or not os.path.exists(test_path):
            logging.warning(f"Missing train/test files on disk for {group_label}; skipping.")
            continue

        group_output_dir = os.path.join(output_dir, group_id)
        os.makedirs(group_output_dir, exist_ok=True)

        m0_label = f"{m0_total:.2f}" if isinstance(m0_total, (int, float)) else "unknown"
        logging.info(f"\nTraining group: {group_label} ({group_id}) | M0 total={m0_label}")
        main(
            training_dataset_file=train_path,
            test_dataset_file=test_path,
            output_dir=group_output_dir,
            group_by=group_by_cols,
            autogluon_time_limit=autogluon_time_limit,
            round_predictions=round_predictions,
            months_ahead=months_ahead,
            reuse_autogluon=reuse_autogluon,
            autogluon_model_name=autogluon_model_name,
            autogluon_path=autogluon_path,
            group_label=group_label,
            group_token=group_token,
            combined_frames=combined_frames,
            run_shap=run_shap,
            shap_plot_dir=group_output_dir,
            shap_model_label=group_id,
            publishing=publishing,
        )

    _write_combined_excel(
        combined_frames, output_dir, global_frames=global_frames,
        group_order=group_order, max_groups=manifest.get('max_groups'),
        group_data_dir=group_data_dir, months_ahead=months_ahead,
        publishing=publishing,
    )


if __name__ == "__main__":
    # ── Argument parsing ──────────────────────────────────────────────────────
    # The parser covers two execution modes:
    #   (a) Global mode  -- single training/test parquet pair, no --group-data-dir.
    #   (b) Per-group mode -- --group-data-dir points to a directory with a
    #       group_manifest.json and per-group parquets; a global model is trained
    #       first, then one individual model per group entry in the manifest.
    parser = argparse.ArgumentParser(description='AutoGluon count forecasting: train/evaluate and write Excel/JSON.')

    # ── Data paths ────────────────────────────────────────────────────────────
    parser.add_argument('--training-dataset', default='results_BS/data_BS/training_dataset.parquet',
                        help='Path to training dataset parquet (default: results_BS/data_BS for BU x Skill Cluster)')
    parser.add_argument('--test-dataset', default='results_BS/data_BS/test_dataset.parquet',
                        help='Path to test dataset parquet (default: results_BS/data_BS for BU x Skill Cluster)')
    parser.add_argument('--output-dir', default='results',
                        help='Output directory for results')
    # ── Grouping ──────────────────────────────────────────────────────────────
    parser.add_argument('--group-by', nargs='*', default=['BU', 'Skill Cluster'],
                        help='Columns to group by (must match build_training_groups.py)')
    # ── AutoGluon tuning ──────────────────────────────────────────────────────
    parser.add_argument('--autogluon-time-limit', type=int, default=None,
                        help='AutoGluon time limit in seconds (default: None = run until completion)')
    parser.add_argument('--round-predictions', action='store_true', default=True,
                        help='Round non-negative predictions to the nearest integer')
    parser.add_argument('--months-ahead', type=int, nargs='+', default=[0, 1, 2, 3, 4, 5],
                        help='Months ahead to predict')
    # ── Reuse / predictor selection ───────────────────────────────────────────
    parser.add_argument('--reuse-autogluon', action='store_true', default=False,
                        help='Reuse existing AutoGluon predictor instead of retraining')
    parser.add_argument('--autogluon-path', type=str, default=None,
                        help='Path to AutoGluon predictor dir (for reuse or override default)')
    parser.add_argument('--autogluon-model-name', type=str, default=None,
                        help='Specific internal AutoGluon model name for predictions')
    # ── Per-group mode ────────────────────────────────────────────────────────
    parser.add_argument('--group-data-dir', type=str, default=None,
                        help='Directory containing per-group train/test parquet files and group_manifest.json')
    parser.add_argument('--global-train-all', action='store_true', default=False,
                        help='Use unfiltered global parquets (training_dataset_global.parquet / test_dataset_global.parquet) '
                             'for the global model so it trains on ALL data, not just demand-filtered groups.')
    # ── SHAP ──────────────────────────────────────────────────────────────────
    parser.add_argument('--run-shap', action='store_true', default=False,
                        help='Run SHAP analysis (top %d features) for global and per-group models when using --group-data-dir.' % TOP_N_SHAP_FEATURES)
    # ── Publishing ────────────────────────────────────────────────────────────
    parser.add_argument('--publishing', action='store_true', default=False,
                        help='Publishing mode: sort All_Predictions by M0_Predicted (not M0_Actual) '
                             'since Jan–Jun 2026 actuals do not exist yet.')

    args = parser.parse_args()

    # Dispatch: per-group mode when --group-data-dir is provided, else global mode
    if args.group_data_dir:
        run_per_group_training(
            group_data_dir=args.group_data_dir,
            output_dir=args.output_dir,
            group_by=args.group_by,
            autogluon_time_limit=args.autogluon_time_limit,
            round_predictions=args.round_predictions,
            months_ahead=args.months_ahead,
            reuse_autogluon=args.reuse_autogluon,
            autogluon_model_name=args.autogluon_model_name,
            autogluon_path=args.autogluon_path,
            global_train_all=args.global_train_all,
            run_shap=args.run_shap,
            publishing=args.publishing,
        )
    else:
        main(
            training_dataset_file=args.training_dataset,
            test_dataset_file=args.test_dataset,
            output_dir=args.output_dir,
            group_by=args.group_by,
            autogluon_time_limit=args.autogluon_time_limit,
            round_predictions=args.round_predictions,
            months_ahead=args.months_ahead,
            reuse_autogluon=args.reuse_autogluon,
            autogluon_model_name=args.autogluon_model_name,
            autogluon_path=args.autogluon_path,
            run_shap=args.run_shap,
            shap_plot_dir=args.output_dir,
            publishing=args.publishing,
        )