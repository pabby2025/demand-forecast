#!/usr/bin/env bash
# Orchestration script to run data preparation (data_split.py), training group building (build_training_groups.py),
# and AutoGluon training/evaluation (train_and_predict.py) with simple flags for UPLF usage and grouping level.
# Grouping modes (mutually exclusive): RLC, S, BS, BRLC.
# Notation: S = Skill Cluster only; RLC = Role Location Cluster (SO GRADE, Country, Skill Cluster);
# BS = BU + Skill Cluster; BRLC = BU + RLC (BU, SO GRADE, Country, Skill Cluster).

set -euo pipefail

export PYTHONIOENCODING=utf-8

NO_UPLF="false"
# Grouping mode flags (mutually exclusive; default: S)
# RLC  = Role Location Cluster (SO GRADE, Country, Skill Cluster)
# S    = Skill Cluster only
# BS   = BU + Skill Cluster
# BRLC = BU + RLC (BU, SO GRADE, Country, Skill Cluster)
# ALL  = run all four modes sequentially
RLC="false"
S="false"
BS="true"
BRLC="false"
ALL="false"
INDIVIDUAL_GROUPS="true"
# Practice area abbreviation (e.g. DE, EPS). Drives input file path and results dir name.
PRACTICE_AREA="DE"
# Year range used in input CSV filenames (must match what clustering.sh produced).
YEAR_RANGE="2023-2025"
# Leave empty to auto-derive from PRACTICE_AREA and YEAR_RANGE (recommended).
# Set explicitly only if you want to override the auto-derived path.
INPUT_FILE=""
MARKETS="Americas"
MONTHS_AHEAD="0 1 2 3 4 5"
DEMAND_PCT="80"
TRAIN_MIN_YEAR="2024"
MAX_GROUPS="45"
CV_THRESHOLD="1.0"
GLOBAL_TRAIN_ALL="true" #if false,the global model trains on the demand-filtered dataset
RUN_SHAP="true"
# SSD Guardrail (runs after Step 3 for all grouping modes)
SSD_GUARDRAIL="true"           # set false with --no-ssd-guardrail to skip
SSD_CUTOFF="2025-06-30"        # SOs with SO Submission Date < this are "confirmed"
PUBLISHING="false"             # set true with --publishing to train on 2024-2025 and predict Jan-Jun 2026

usage() {
  cat <<EOF
Usage: $(basename "$0") [--practice-area PA] [--year-range YYYY-YYYY] [--no-uplf] [--rlc|--s|--bs|--brlc] [--input-file PATH] [--months-ahead N1 N2 ...] [--demand-pct PCT] [--individual-groups] [--max-groups N] [--cv-threshold T] [--global-train-all] [--run-shap]

Flags:
  --practice-area PA
                    Practice Area abbreviation (e.g. DE, EPS). Used to auto-derive the input
                    file path (data/{PA}/DFC_YTD_{yr}_{PA}_V2_skill_clusters) and to include
                    the PA in the results directory name. Default: DE
  --year-range YYYY-YYYY
                    Year range matching the clustering output filenames. Default: 2023-2025

  --no-uplf         Disable UPLF usage and quarter growth features in data/build scripts.
                    train_and_predict.py still runs normally, but outputs go to *_no_uplf directory.

  Grouping mode (exactly one; default: --bs):
    --rlc           RLC = Role Location Cluster (SO GRADE, Country, Skill Cluster)
    --s             S = Skill Cluster only
    --bs            BS = BU + Skill Cluster
    --brlc          BRLC = BU + RLC (BU, SO GRADE, Country, Skill Cluster)

  --input-file PATH Override the auto-derived input CSV base path (market suffix appended).

  --months-ahead    Number of months ahead to predict (space-separated values).
                    Passed to build_training_groups.py and train_and_predict.py.
                    Default: 0 1 2 3 4 5
  --demand-pct PCT  Keep groups covering this %% of train demand (data_split.py). Default: 80
  --individual-groups
                    Build per-group train/test datasets in results_<GROUP>/data_<GROUP> and train per-group models.
  --max-groups N    Maximum number of individual group models (default: 40). Top N by demand
                    (CV < cv-threshold) get one model each; remaining groups use the global model only.
  --cv-threshold T  CV threshold for individual group eligibility (default: 1.0). Groups with
                    coefficient of variation >= T are ineligible for individual models.
  --global-train-all
                    Train the global model on ALL data (not filtered by --demand-pct).
                    If false, global model uses demand-filtered data only (e.g. demand_pct=80).
  --run-shap        Run SHAP analysis (top 25 features) for global and per-group models (can be slow).
  --markets M [M ...]
                    Space-separated list of markets to run. Default: "Americas EMEA".
                    Example: --markets Americas
  --no-ssd-guardrail
                    Skip the SSD guardrail step (runs for all modes by default).
  --ssd-cutoff DATE SOs with SO Submission Date < DATE are treated as confirmed.
                    Format: YYYY-MM-DD. Default: 2025-06-30.
  --publishing      Publishing mode: trains on 2024–2025 data (RSD up to Dec 31 2025)
                    and predicts the next 6 months (M0=Jan 2026 … M5=Jun 2026).
                    Automatically sets --train-min-year 2024, --ssd-cutoff 2025-12-31,
                    and propagates --publishing to data_split, build_training_groups,
                    and ssd_guardrail so all date boundaries are consistent.

Behavior:
  - One global model: on 100%% of data if --global-train-all, else on demand-filtered data only.
  - Individual models: one per group up to --max-groups (by demand, CV < cv-threshold).
  - Pipeline order: data_split.py -> build_training_groups.py -> train_and_predict.py.
  - Results dir: results_{GROUP}_{PA}[_no_uplf]_m{N}_per_group_{Market}

Examples:
  # BS grouping for DE practice area (default)
  ./run_pipeline.sh --bs

  # BS grouping for EPS practice area
  ./run_pipeline.sh --bs --practice-area EPS

  # RLC grouping for DE, no UPLF
  ./run_pipeline.sh --rlc --practice-area DE --no-uplf

  # Override input file explicitly
  ./run_pipeline.sh --bs --input-file data/DE/my_custom_clusters

  # Custom months ahead
  ./run_pipeline.sh --bs --practice-area DE --months-ahead 0 1 2
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --practice-area)
        if [[ $# -lt 2 ]]; then
          echo "Error: --practice-area requires a value" >&2
          usage; exit 1
        fi
        PRACTICE_AREA="$2"
        shift 2
        ;;
      --year-range)
        if [[ $# -lt 2 ]]; then
          echo "Error: --year-range requires a value" >&2
          usage; exit 1
        fi
        YEAR_RANGE="$2"
        shift 2
        ;;
      --no-uplf)
        NO_UPLF="true"
        shift
        ;;
      --rlc)
        RLC="true"
        shift
        ;;
      --s)
        S="true"
        shift
        ;;
      --bs)
        BS="true"
        shift
        ;;
      --brlc)
        BRLC="true"
        shift
        ;;
      --all)
        ALL="true"
        shift
        ;;
      --input-file)
        if [[ $# -lt 2 ]]; then
          echo "Error: --input-file requires a file path" >&2
          usage
          exit 1
        fi
        INPUT_FILE="$2"
        shift 2
        ;;
      --individual-groups)
        INDIVIDUAL_GROUPS="true"
        shift
        ;;
      --months-ahead)
        if [[ $# -lt 2 ]]; then
          echo "Error: --months-ahead requires at least one value" >&2
          usage
          exit 1
        fi
        shift
        MONTHS_AHEAD_VALUES=()
        while [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; do
          if ! [[ "$1" =~ ^[0-9]+$ ]]; then
            echo "Error: --months-ahead values must be integers, got: $1" >&2
            usage
            exit 1
          fi
          MONTHS_AHEAD_VALUES+=("$1")
          shift
        done
        if [[ ${#MONTHS_AHEAD_VALUES[@]} -eq 0 ]]; then
          echo "Error: --months-ahead requires at least one value" >&2
          usage
          exit 1
        fi
        MONTHS_AHEAD="${MONTHS_AHEAD_VALUES[*]}"
        ;;
      --demand-pct)
        if [[ $# -lt 2 ]]; then
          echo "Error: --demand-pct requires a value" >&2
          usage
          exit 1
        fi
        if ! [[ "$2" =~ ^[0-9]+\.?[0-9]*$ ]]; then
          echo "Error: --demand-pct must be a number, got: $2" >&2
          usage
          exit 1
        fi
        DEMAND_PCT="$2"
        shift 2
        ;;
      --max-groups)
        if [[ $# -lt 2 ]]; then
          echo "Error: --max-groups requires a value" >&2
          usage
          exit 1
        fi
        if ! [[ "$2" =~ ^[0-9]+$ ]]; then
          echo "Error: --max-groups must be a positive integer, got: $2" >&2
          usage
          exit 1
        fi
        MAX_GROUPS="$2"
        shift 2
        ;;
      --cv-threshold)
        if [[ $# -lt 2 ]]; then
          echo "Error: --cv-threshold requires a value" >&2
          usage
          exit 1
        fi
        if ! [[ "$2" =~ ^[0-9]+\.?[0-9]*$ ]]; then
          echo "Error: --cv-threshold must be a number, got: $2" >&2
          usage
          exit 1
        fi
        CV_THRESHOLD="$2"
        shift 2
        ;;
      --global-train-all)
        GLOBAL_TRAIN_ALL="true"
        shift
        ;;
      --run-shap)
        RUN_SHAP="true"
        shift
        ;;
      --markets)
        if [[ $# -lt 2 ]]; then
          echo "Error: --markets requires at least one value" >&2
          usage; exit 1
        fi
        shift
        MARKETS=""
        while [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; do
          MARKETS="$MARKETS $1"
          shift
        done
        MARKETS="${MARKETS# }"  # trim leading space
        ;;
      --no-ssd-guardrail)
        SSD_GUARDRAIL="false"
        shift
        ;;
      --ssd-cutoff)
        if [[ $# -lt 2 ]]; then
          echo "Error: --ssd-cutoff requires a date value (YYYY-MM-DD)" >&2
          usage; exit 1
        fi
        SSD_CUTOFF="$2"
        shift 2
        ;;
      --publishing)
        PUBLISHING="true"
        shift
        ;;
      --train-min-year)
        if [[ $# -lt 2 ]]; then
          echo "Error: --train-min-year requires a year value" >&2
          usage; exit 1
        fi
        if ! [[ "$2" =~ ^[0-9]{4}$ ]]; then
          echo "Error: --train-min-year must be a 4-digit year, got: $2" >&2
          usage; exit 1
        fi
        TRAIN_MIN_YEAR="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  # ── Publishing mode overrides ────────────────────────────────────────────────
  # When --publishing is set, force dates that produce a 2026 forecast:
  #   • TRAIN_MIN_YEAR = 2024  (training data starts Jan 2024)
  #   • SSD_CUTOFF     = 2025-12-31  (SSD floor boundary = end of training window)
  # These are only overridden here if the user did not explicitly pass them as
  # flags (i.e., they still hold their default values).  If the user explicitly
  # passed e.g. --train-min-year 2023 alongside --publishing, that explicit value
  # takes precedence because parse_args has already written it to TRAIN_MIN_YEAR.
  PUBLISH_ARGS=()
  if [[ "$PUBLISHING" == "true" ]]; then
    echo "  [Publishing mode] Setting TRAIN_MIN_YEAR=2024, SSD_CUTOFF=2025-12-31"
    TRAIN_MIN_YEAR="2024"
    SSD_CUTOFF="2025-12-31"
    PUBLISH_ARGS=(--publishing)
  fi

  # --all: re-invoke this script for each mode, forwarding all other args
  if [[ "$ALL" == "true" ]]; then
    FORWARDED=()
    for arg in "$@"; do
      [[ "$arg" != "--all" ]] && FORWARDED+=("$arg")
    done
    for mode_flag in --s --bs --rlc --brlc; do
      echo
      echo "########## ALL mode: running $mode_flag ##########"
      bash "$(realpath "$0")" "$mode_flag" "${FORWARDED[@]}"
    done
    exit 0
  fi

  # Default to RLC if no grouping mode specified
  if [[ "$RLC" != "true" && "$S" != "true" && "$BS" != "true" && "$BRLC" != "true" ]]; then
    RLC="true"
  fi

  # Validate exactly one grouping mode
  MODE_COUNT=0
  [[ "$RLC" == "true" ]] && MODE_COUNT=$((MODE_COUNT + 1))
  [[ "$S" == "true" ]] && MODE_COUNT=$((MODE_COUNT + 1))
  [[ "$BS" == "true" ]] && MODE_COUNT=$((MODE_COUNT + 1))
  [[ "$BRLC" == "true" ]] && MODE_COUNT=$((MODE_COUNT + 1))
  if [[ $MODE_COUNT -gt 1 ]]; then
    echo "Error: Only one grouping mode allowed (--rlc, --s, --bs, --brlc)" >&2
    usage
    exit 1
  fi

  # Auto-derive INPUT_FILE from PRACTICE_AREA / YEAR_RANGE when not explicitly set.
  # The per-market suffix (_Americas.csv, _EMEA.csv) is appended in the loop below.
  if [[ -z "${INPUT_FILE:-}" ]]; then
    INPUT_FILE="data/${PRACTICE_AREA}/DFC_YTD_${YEAR_RANGE}_${PRACTICE_AREA}_V2_skill_clusters"
  fi

  echo "======================================================================"
  echo "Running pipeline with configuration:"
  echo "  PRACTICE_AREA    = $PRACTICE_AREA"
  echo "  YEAR_RANGE       = $YEAR_RANGE"
  echo "  NO_UPLF          = $NO_UPLF"
  echo "  RLC              = $RLC"
  echo "  S                = $S"
  echo "  BS               = $BS"
  echo "  BRLC             = $BRLC"
  echo "  INDIVIDUAL_GROUPS = $INDIVIDUAL_GROUPS"
  echo "  INPUT_FILE       = $INPUT_FILE"
  echo "  MARKETS          = $MARKETS"
  echo "  MONTHS_AHEAD     = $MONTHS_AHEAD"
  echo "  DEMAND_PCT       = $DEMAND_PCT"
  echo "  MAX_GROUPS       = $MAX_GROUPS"
  echo "  CV_THRESHOLD     = $CV_THRESHOLD"
  echo "  GLOBAL_TRAIN_ALL = $GLOBAL_TRAIN_ALL"
  echo "  SSD_GUARDRAIL    = $SSD_GUARDRAIL  (cutoff=$SSD_CUTOFF)"
  echo "  TRAIN_MIN_YEAR   = $TRAIN_MIN_YEAR"
  echo "  PUBLISHING       = $PUBLISHING  (train 2024-Dec2025; predict Jan-Jun 2026)"
  echo "======================================================================"

  if [[ "$RLC" == "true" ]]; then
    # RLC = Role Location Cluster (SO GRADE, Country, Skill Cluster)
    DATA_GROUP_BY=(--group-by "Country" "SO GRADE" "Skill Cluster")
    BUILD_GROUP_BY=(--group-by "Country" "SO GRADE" "Skill Cluster")
    COMPARE_GROUP_BY=(--group-by "Country" "SO GRADE" "Skill Cluster")
    GROUP_SUFFIX="_rlc"
    GROUP_INITIALS="RLC"
  elif [[ "$S" == "true" ]]; then
    # S = Skill Cluster only
    DATA_GROUP_BY=(--group-by "Skill Cluster")
    BUILD_GROUP_BY=(--group-by "Skill Cluster")
    COMPARE_GROUP_BY=(--group-by "Skill Cluster")
    GROUP_SUFFIX="_s"
    GROUP_INITIALS="S"
  elif [[ "$BS" == "true" ]]; then
    # BS = BU + Skill Cluster
    DATA_GROUP_BY=(--group-by "BU" "Skill Cluster")
    BUILD_GROUP_BY=(--group-by "BU" "Skill Cluster")
    COMPARE_GROUP_BY=(--group-by "BU" "Skill Cluster")
    GROUP_SUFFIX="_bs"
    GROUP_INITIALS="BS"
  else
    # BRLC = BU + RLC (BU, SO GRADE, Country, Skill Cluster)
    DATA_GROUP_BY=(--group-by "BU" "SO GRADE" "Country" "Skill Cluster")
    BUILD_GROUP_BY=(--group-by "BU" "SO GRADE" "Country" "Skill Cluster")
    COMPARE_GROUP_BY=(--group-by "BU" "SO GRADE" "Country" "Skill Cluster")
    GROUP_SUFFIX="_brlc"
    GROUP_INITIALS="BRLC"
  fi

  # UPLF / quarter-growth handling
  if [[ "$NO_UPLF" == "true" ]]; then
    # For data_split: use explicit --no-uplf flag
    DATA_UPLF_ARGS=(--no-uplf)
    # For build_training_groups: explicit flags
    BUILD_UPLF_ARGS=(--no-uplf --no-quarter-growth)
    UPLF_SUFFIX="_no_uplf"
  else
    # Empty array - will be conditionally expanded
    DATA_UPLF_ARGS=()
    BUILD_UPLF_ARGS=()
    UPLF_SUFFIX=""
  fi

  # Create months ahead suffix (e.g., m6 for [0,1,2,3,4,5])
  MONTHS_COUNT=$(echo "$MONTHS_AHEAD" | wc -w | tr -d ' ')
  MONTHS_SUFFIX="_m${MONTHS_COUNT}"
  
  # Build --global-train-all argument conditionally
  GLOBAL_TRAIN_ALL_ARGS=()
  [[ "$GLOBAL_TRAIN_ALL" == "true" ]] && GLOBAL_TRAIN_ALL_ARGS=(--global-train-all)

  for market in $MARKETS; do
    # Per-market input: base name + _Market.csv (e.g. ..._Americas.csv, ..._EMEA.csv)
    INPUT_FILE_MARKET="${INPUT_FILE}_${market}.csv"
    # Result directory name: include market; resolve to first non-existing name (e.g. results_S_m6_per_group_Americas_1 if base exists)
    # Include PA in results dir so different practice areas don't collide.
    # e.g.  results_BS_DE_m6_per_group_Americas  /  results_BS_EPS_m6_per_group_EMEA
    if [[ "$INDIVIDUAL_GROUPS" == "true" ]]; then
      RESULTS_BASE="results_${GROUP_INITIALS}_${PRACTICE_AREA}${UPLF_SUFFIX}${MONTHS_SUFFIX}_per_group_${market}"
    else
      RESULTS_BASE="results_${GROUP_INITIALS}_${PRACTICE_AREA}${UPLF_SUFFIX}${MONTHS_SUFFIX}_${market}"
    fi
    RESULTS_DIR=$(python -c "from data_split import resolve_results_dir; import sys; print(resolve_results_dir(sys.argv[1]))" "$RESULTS_BASE")
    DATA_DIR="$RESULTS_DIR/data_$GROUP_INITIALS"

    echo
    echo "========== Market: $market (input: $INPUT_FILE_MARKET) =========="
    echo
    echo "Step 1/3: Running data_split.py ..."
    if [[ ${#DATA_UPLF_ARGS[@]} -gt 0 ]]; then
      python data_split.py \
        --input-file "$INPUT_FILE_MARKET" \
        --results-dir "$RESULTS_DIR" \
        --demand-pct "$DEMAND_PCT" \
        --ssd-cutoff "$SSD_CUTOFF" \
        --train-min-year "$TRAIN_MIN_YEAR" \
        "${DATA_GROUP_BY[@]}" \
        "${DATA_UPLF_ARGS[@]}" \
        "${GLOBAL_TRAIN_ALL_ARGS[@]}" \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    else
      python data_split.py \
        --input-file "$INPUT_FILE_MARKET" \
        --results-dir "$RESULTS_DIR" \
        --demand-pct "$DEMAND_PCT" \
        --ssd-cutoff "$SSD_CUTOFF" \
        --train-min-year "$TRAIN_MIN_YEAR" \
        "${DATA_GROUP_BY[@]}" \
        "${GLOBAL_TRAIN_ALL_ARGS[@]}" \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    fi

    echo
    echo "Step 2/3: Running build_training_groups.py ..."
    INDIVIDUAL_ARGS=()
    [[ "$INDIVIDUAL_GROUPS" == "true" ]] && INDIVIDUAL_ARGS=(--individual-groups)
    if [[ ${#BUILD_UPLF_ARGS[@]} -gt 0 ]]; then
      python build_training_groups.py \
        "${BUILD_GROUP_BY[@]}" \
        --months-ahead $MONTHS_AHEAD \
        --data-dir "$DATA_DIR" \
        --max-groups "$MAX_GROUPS" \
        --cv-threshold "$CV_THRESHOLD" \
        "${BUILD_UPLF_ARGS[@]}" \
        "${INDIVIDUAL_ARGS[@]}" \
        "${GLOBAL_TRAIN_ALL_ARGS[@]}" \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    else
      python build_training_groups.py \
        "${BUILD_GROUP_BY[@]}" \
        --months-ahead $MONTHS_AHEAD \
        --data-dir "$DATA_DIR" \
        --max-groups "$MAX_GROUPS" \
        --cv-threshold "$CV_THRESHOLD" \
        "${INDIVIDUAL_ARGS[@]}" \
        "${GLOBAL_TRAIN_ALL_ARGS[@]}" \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    fi

    echo
    RUN_SHAP_ARGS=()
    [[ "$RUN_SHAP" == "true" ]] && RUN_SHAP_ARGS=(--run-shap)
    if [[ "$INDIVIDUAL_GROUPS" == "true" ]]; then
      echo "Step 3/3: Running train_and_predict.py (per-group models) ..."
      python train_and_predict.py \
        --group-data-dir "$DATA_DIR" \
        --output-dir "$RESULTS_DIR" \
        --months-ahead $MONTHS_AHEAD \
        "${COMPARE_GROUP_BY[@]}" \
        "${GLOBAL_TRAIN_ALL_ARGS[@]}" \
        ${RUN_SHAP_ARGS[@]+"${RUN_SHAP_ARGS[@]}"} \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    else
      echo "Step 3/3: Running train_and_predict.py ..."
      python train_and_predict.py \
        --training-dataset "$DATA_DIR/training_dataset.parquet" \
        --test-dataset "$DATA_DIR/test_dataset.parquet" \
        --output-dir "$RESULTS_DIR" \
        --months-ahead $MONTHS_AHEAD \
        "${COMPARE_GROUP_BY[@]}" \
        ${RUN_SHAP_ARGS[@]+"${RUN_SHAP_ARGS[@]}"} \
        ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
    fi

    # ── Step 4: SSD Guardrail ────────────────────────────────────────────────
    if [[ "$SSD_GUARDRAIL" == "true" ]]; then
      echo
      echo "Step 4/4: Running ssd_guardrail.py (SSD guardrail) ..."

      # Find the most-recently-written combined Excel in RESULTS_DIR.
      # Exclude any *_SSD_guardrail_*.xlsx files so a previous guardrail run
      # doesn't get picked up instead of the fresh predictions Excel.
      LATEST_EXCEL=$(ls -t "$RESULTS_DIR"/combined_group_results_*.xlsx 2>/dev/null \
                     | grep -v '_SSD_guardrail_' | head -1)
      if [[ -z "$LATEST_EXCEL" ]]; then
        echo "  WARNING: No combined_group_results_*.xlsx found in $RESULTS_DIR; skipping SSD guardrail." >&2
      else
        echo "  Using predictions Excel: $LATEST_EXCEL"
        python ssd_guardrail.py \
          --input-excel  "$LATEST_EXCEL" \
          --floors-csv   "$DATA_DIR/ssd_floors.csv" \
          --ssd-cutoff   "$SSD_CUTOFF" \
          --test-parquet "$DATA_DIR/test_dataset.parquet" \
          --output-dir   "$RESULTS_DIR" \
          ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
        echo "  SSD guardrail complete. Output saved in $RESULTS_DIR"
      fi
    fi

    echo
    echo "Pipeline finished for market: $market. Results: $RESULTS_DIR"
  done

  echo
  echo "Pipeline finished successfully for all markets."
}

main "$@"

